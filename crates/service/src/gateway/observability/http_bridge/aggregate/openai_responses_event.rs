use serde_json::{Map, Value};

use super::output_text::{
    append_output_text, collect_output_text_from_event_fields, collect_response_output_text,
    extract_error_message_from_json, parse_usage_from_json, UpstreamResponseUsage,
};
use super::{parse_sse_frame_json, SseTerminal};

const STREAM_INCOMPLETE_FALLBACK_MESSAGE: &str = "连接中断（可能是网络波动或客户端主动取消）";
const STREAM_IDLE_TIMEOUT_FALLBACK_MESSAGE: &str = "上游流式空闲超时";
const UPSTREAM_NON_SUCCESS_FALLBACK_MESSAGE: &str = "上游请求失败，未返回具体错误信息";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum OpenAIResponsesEventKind {
    Completed,
    Done,
    Failed,
    Incomplete,
    OutputTextDelta,
    OutputTextDone,
    OutputItemAdded,
    OutputItemDone,
    ContentPartAdded,
    ContentPartDone,
    Other,
}

impl OpenAIResponsesEventKind {
    fn from_type(kind: &str) -> Self {
        match kind.trim() {
            "response.completed" => Self::Completed,
            "response.done" => Self::Done,
            "response.failed" => Self::Failed,
            "response.incomplete" => Self::Incomplete,
            "response.output_text.delta" => Self::OutputTextDelta,
            "response.output_text.done" => Self::OutputTextDone,
            "response.output_item.added" => Self::OutputItemAdded,
            "response.output_item.done" => Self::OutputItemDone,
            "response.content_part.added" | "response.content_part.delta" => Self::ContentPartAdded,
            "response.content_part.done" => Self::ContentPartDone,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone)]
pub(in super::super) struct OpenAIResponsesEvent {
    pub(in super::super) event_type: Option<String>,
    pub(in super::super) usage: UpstreamResponseUsage,
    pub(in super::super) terminal: Option<SseTerminal>,
    pub(in super::super) upstream_error_hint: Option<String>,
    pub(in super::super) response_snapshot: Option<Value>,
}

impl OpenAIResponsesEvent {
    pub(in super::super) fn parse(lines: &[String]) -> Option<Self> {
        let value = parse_sse_frame_json(lines)?;
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|kind| !kind.is_empty())
            .map(str::to_string);
        let kind = event_type
            .as_deref()
            .map(OpenAIResponsesEventKind::from_type)
            .unwrap_or(OpenAIResponsesEventKind::Other);

        let upstream_error_hint = extract_error_message_from_json(&value);
        let terminal =
            terminal_for_event(kind, event_type.as_deref(), upstream_error_hint.as_deref());

        let mut usage = parse_usage_from_json(&value);
        if let Some(extra_output_text) = collect_extra_output_text(&value, kind) {
            let target = usage.output_text.get_or_insert_with(String::new);
            append_output_text(target, extra_output_text.as_str());
        }

        Some(Self {
            event_type,
            usage,
            terminal,
            upstream_error_hint,
            response_snapshot: extract_response_snapshot(&value),
        })
    }
}

fn extract_response_snapshot(value: &Value) -> Option<Value> {
    if let Some(response) = value.get("response").cloned().filter(Value::is_object) {
        return Some(response);
    }

    let mut response = Map::new();
    if let Some(response_id) = value.get("response_id").and_then(Value::as_str) {
        response.insert("id".to_string(), Value::String(response_id.to_string()));
    }
    if let Some(created) = value.get("created").cloned() {
        response.insert("created".to_string(), created);
    }
    if let Some(model) = value.get("model").and_then(Value::as_str) {
        response.insert("model".to_string(), Value::String(model.to_string()));
    }

    if response.is_empty() {
        None
    } else {
        Some(Value::Object(response))
    }
}

fn terminal_for_event(
    kind: OpenAIResponsesEventKind,
    event_type: Option<&str>,
    upstream_error_hint: Option<&str>,
) -> Option<SseTerminal> {
    if let Some(raw) = upstream_error_hint {
        return Some(SseTerminal::Err(normalize_terminal_error_hint(raw)));
    }

    match kind {
        OpenAIResponsesEventKind::Completed | OpenAIResponsesEventKind::Done => {
            Some(SseTerminal::Ok)
        }
        OpenAIResponsesEventKind::Failed => Some(SseTerminal::Err(
            UPSTREAM_NON_SUCCESS_FALLBACK_MESSAGE.to_string(),
        )),
        OpenAIResponsesEventKind::Incomplete => Some(SseTerminal::Err(
            event_type
                .filter(|value| !value.trim().is_empty())
                .map(|_| STREAM_INCOMPLETE_FALLBACK_MESSAGE.to_string())
                .unwrap_or_else(|| STREAM_INCOMPLETE_FALLBACK_MESSAGE.to_string()),
        )),
        OpenAIResponsesEventKind::OutputTextDelta
        | OpenAIResponsesEventKind::OutputTextDone
        | OpenAIResponsesEventKind::OutputItemAdded
        | OpenAIResponsesEventKind::OutputItemDone
        | OpenAIResponsesEventKind::ContentPartAdded
        | OpenAIResponsesEventKind::ContentPartDone
        | OpenAIResponsesEventKind::Other => None,
    }
}

fn normalize_terminal_error_hint(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return UPSTREAM_NON_SUCCESS_FALLBACK_MESSAGE.to_string();
    }

    let normalized = trimmed.to_ascii_lowercase();
    if normalized.contains("stream_timeout")
        || normalized.contains("stream idle timeout")
        || normalized.contains("idle timeout")
    {
        return STREAM_IDLE_TIMEOUT_FALLBACK_MESSAGE.to_string();
    }

    trimmed.to_string()
}

fn collect_extra_output_text(value: &Value, kind: OpenAIResponsesEventKind) -> Option<String> {
    let mut text_out = String::new();

    match kind {
        OpenAIResponsesEventKind::OutputTextDelta
        | OpenAIResponsesEventKind::OutputTextDone
        | OpenAIResponsesEventKind::ContentPartAdded
        | OpenAIResponsesEventKind::ContentPartDone => {
            if let Some(delta) = value.get("delta") {
                collect_response_output_text(delta, &mut text_out);
            }
            collect_output_text_from_event_fields(value, &mut text_out);
        }
        OpenAIResponsesEventKind::OutputItemAdded | OpenAIResponsesEventKind::OutputItemDone => {
            collect_output_text_from_event_fields(value, &mut text_out);
        }
        OpenAIResponsesEventKind::Completed
        | OpenAIResponsesEventKind::Done
        | OpenAIResponsesEventKind::Failed
        | OpenAIResponsesEventKind::Incomplete
        | OpenAIResponsesEventKind::Other => {}
    }

    let text = text_out.trim();
    if text.is_empty() {
        None
    } else {
        Some(text_out)
    }
}

#[cfg(test)]
mod tests {
    use super::{OpenAIResponsesEvent, SseTerminal};

    #[test]
    fn parse_openai_responses_event_maps_bare_incomplete_to_user_friendly_terminal() {
        let lines = vec![
            "event: response.incomplete\n".to_string(),
            "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\"}}\n"
                .to_string(),
            "\n".to_string(),
        ];

        let event = OpenAIResponsesEvent::parse(&lines).expect("parsed event");
        assert_eq!(event.event_type.as_deref(), Some("response.incomplete"));
        assert!(matches!(
            event.terminal,
            Some(SseTerminal::Err(ref message))
                if message == "连接中断（可能是网络波动或客户端主动取消）"
        ));
    }

    #[test]
    fn parse_openai_responses_event_maps_stream_timeout_hint_to_idle_timeout() {
        let lines = vec![
            "event: response.incomplete\n".to_string(),
            "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"status_details\":{\"error\":{\"message\":\"stream timeout at upstream\",\"code\":\"stream_timeout\"}}}}\n".to_string(),
            "\n".to_string(),
        ];

        let event = OpenAIResponsesEvent::parse(&lines).expect("parsed event");
        assert_eq!(
            event.upstream_error_hint.as_deref(),
            Some("code=stream_timeout stream timeout at upstream")
        );
        assert!(matches!(
            event.terminal,
            Some(SseTerminal::Err(ref message)) if message == "上游流式空闲超时"
        ));
    }
}
