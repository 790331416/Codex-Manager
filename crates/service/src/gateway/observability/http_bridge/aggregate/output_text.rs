use serde_json::{Map, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

const OUTPUT_TEXT_LIMIT_BYTES_ENV: &str = "CODEXMANAGER_HTTP_BRIDGE_OUTPUT_TEXT_LIMIT_BYTES";
const DEFAULT_OUTPUT_TEXT_LIMIT_BYTES: usize = 128 * 1024;
pub(in super::super) const OUTPUT_TEXT_TRUNCATED_MARKER: &str = "[output_text truncated]";
static OUTPUT_TEXT_LIMIT_BYTES: AtomicUsize = AtomicUsize::new(DEFAULT_OUTPUT_TEXT_LIMIT_BYTES);
static OUTPUT_TEXT_LIMIT_LOADED: OnceLock<()> = OnceLock::new();
const UPSTREAM_ERROR_HINT_LIMIT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Default)]
pub(crate) struct UpstreamResponseUsage {
    pub input_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub reasoning_output_tokens: Option<i64>,
    pub output_text: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct UpstreamResponseBridgeResult {
    pub usage: UpstreamResponseUsage,
    pub stream_terminal_seen: bool,
    pub stream_terminal_error: Option<String>,
    pub delivery_error: Option<String>,
    pub upstream_error_hint: Option<String>,
    pub delivered_status_code: Option<u16>,
    pub upstream_request_id: Option<String>,
    pub upstream_cf_ray: Option<String>,
    pub upstream_auth_error: Option<String>,
    pub upstream_identity_error_code: Option<String>,
    pub upstream_content_type: Option<String>,
    pub last_sse_event_type: Option<String>,
}

impl UpstreamResponseBridgeResult {
    pub(crate) fn is_ok(&self, is_stream: bool) -> bool {
        if self.delivery_error.is_some() {
            return false;
        }
        if is_stream {
            if !self.stream_terminal_seen {
                return false;
            }
            if self.stream_terminal_error.is_some() {
                return false;
            }
        }
        true
    }

    pub(crate) fn error_message(&self, is_stream: bool) -> Option<String> {
        if let Some(err) = self.stream_terminal_error.as_ref() {
            return Some(err.clone());
        }
        if is_stream && !self.stream_terminal_seen {
            return Some("上游流中途中断（未正常结束）".to_string());
        }
        if let Some(err) = self.delivery_error.as_ref() {
            return Some(format!("response write failed: {err}"));
        }
        None
    }
}

pub(in super::super) fn merge_usage(
    target: &mut UpstreamResponseUsage,
    source: UpstreamResponseUsage,
) {
    if source.input_tokens.is_some() {
        target.input_tokens = source.input_tokens;
    }
    if source.cached_input_tokens.is_some() {
        target.cached_input_tokens = source.cached_input_tokens;
    }
    if source.output_tokens.is_some() {
        target.output_tokens = source.output_tokens;
    }
    if source.total_tokens.is_some() {
        target.total_tokens = source.total_tokens;
    }
    if source.reasoning_output_tokens.is_some() {
        target.reasoning_output_tokens = source.reasoning_output_tokens;
    }
    if let Some(source_text) = source.output_text {
        let target_text = target.output_text.get_or_insert_with(String::new);
        append_output_text_raw(target_text, source_text.as_str());
    }
}

pub(in super::super) fn usage_has_signal(usage: &UpstreamResponseUsage) -> bool {
    usage.input_tokens.is_some()
        || usage.cached_input_tokens.is_some()
        || usage.output_tokens.is_some()
        || usage.total_tokens.is_some()
        || usage.reasoning_output_tokens.is_some()
        || usage
            .output_text
            .as_ref()
            .is_some_and(|text| !text.trim().is_empty())
}

fn parse_usage_from_object(usage: Option<&Map<String, Value>>) -> UpstreamResponseUsage {
    let input_tokens = usage
        .and_then(|map| map.get("input_tokens").and_then(Value::as_i64))
        .or_else(|| usage.and_then(|map| map.get("prompt_tokens").and_then(Value::as_i64)));
    let output_tokens = usage
        .and_then(|map| map.get("output_tokens").and_then(Value::as_i64))
        .or_else(|| usage.and_then(|map| map.get("completion_tokens").and_then(Value::as_i64)));
    let total_tokens = usage.and_then(|map| map.get("total_tokens").and_then(Value::as_i64));
    let cached_input_tokens = usage
        .and_then(|map| map.get("input_tokens_details"))
        .and_then(Value::as_object)
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_i64)
        .or_else(|| {
            usage.and_then(|map| map.get("cache_read_input_tokens").and_then(Value::as_i64))
        })
        .or_else(|| {
            usage
                .and_then(|map| map.get("prompt_tokens_details"))
                .and_then(Value::as_object)
                .and_then(|details| details.get("cached_tokens"))
                .and_then(Value::as_i64)
        });
    let reasoning_output_tokens = usage
        .and_then(|map| map.get("output_tokens_details"))
        .and_then(Value::as_object)
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_i64)
        .or_else(|| {
            usage.and_then(|map| map.get("reasoning_output_tokens").and_then(Value::as_i64))
        })
        .or_else(|| {
            usage
                .and_then(|map| map.get("completion_tokens_details"))
                .and_then(Value::as_object)
                .and_then(|details| details.get("reasoning_tokens"))
                .and_then(Value::as_i64)
        });
    UpstreamResponseUsage {
        input_tokens,
        cached_input_tokens,
        output_tokens,
        total_tokens,
        reasoning_output_tokens,
        output_text: None,
    }
}

pub(in super::super) fn append_output_text(buffer: &mut String, text: &str) {
    if text.is_empty() {
        return;
    }
    let limit = output_text_limit_bytes();
    if limit > 0 && buffer.len() >= limit {
        mark_output_text_truncated(buffer, limit);
        return;
    }
    if !buffer.is_empty() {
        if limit > 0 && buffer.len() + 1 > limit {
            mark_output_text_truncated(buffer, limit);
            return;
        }
        buffer.push('\n');
    }
    if limit == 0 {
        buffer.push_str(text);
        return;
    }
    let remaining = limit.saturating_sub(buffer.len());
    if remaining == 0 {
        mark_output_text_truncated(buffer, limit);
        return;
    }
    let slice = truncate_str_to_bytes(text, remaining);
    buffer.push_str(slice);
    if slice.len() < text.len() {
        mark_output_text_truncated(buffer, limit);
    }
}

pub(in super::super) fn append_output_text_raw(buffer: &mut String, text: &str) {
    if text.is_empty() {
        return;
    }
    let limit = output_text_limit_bytes();
    if limit > 0 && buffer.len() >= limit {
        mark_output_text_truncated(buffer, limit);
        return;
    }
    if limit == 0 {
        buffer.push_str(text);
        return;
    }
    let remaining = limit.saturating_sub(buffer.len());
    if remaining == 0 {
        mark_output_text_truncated(buffer, limit);
        return;
    }
    let slice = truncate_str_to_bytes(text, remaining);
    buffer.push_str(slice);
    if slice.len() < text.len() {
        mark_output_text_truncated(buffer, limit);
    }
}

pub(in super::super) fn collect_response_output_text(value: &Value, output: &mut String) {
    match value {
        Value::String(text) => append_output_text(output, text),
        Value::Array(items) => {
            for item in items {
                collect_response_output_text(item, output);
            }
        }
        Value::Object(map) => {
            if let Some(text) = map.get("output_text").and_then(Value::as_str) {
                append_output_text(output, text);
            }
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                append_output_text(output, text);
            }
            if let Some(content) = map.get("content") {
                collect_response_output_text(content, output);
            }
            if let Some(message) = map.get("message") {
                collect_response_output_text(message, output);
            }
            if let Some(output_field) = map.get("output") {
                collect_response_output_text(output_field, output);
            }
            if let Some(delta) = map.get("delta") {
                collect_response_output_text(delta, output);
            }
        }
        _ => {}
    }
}

pub(in super::super) fn output_text_limit_bytes() -> usize {
    let _ = OUTPUT_TEXT_LIMIT_LOADED.get_or_init(reload_from_env);
    OUTPUT_TEXT_LIMIT_BYTES.load(Ordering::Relaxed)
}

pub(in super::super) fn reload_from_env() {
    let raw = std::env::var(OUTPUT_TEXT_LIMIT_BYTES_ENV).unwrap_or_default();
    let limit = raw
        .trim()
        .parse::<usize>()
        .unwrap_or(DEFAULT_OUTPUT_TEXT_LIMIT_BYTES);
    OUTPUT_TEXT_LIMIT_BYTES.store(limit, Ordering::Relaxed);
}

fn truncate_str_to_bytes(text: &str, max_bytes: usize) -> &str {
    if max_bytes >= text.len() {
        return text;
    }
    let mut idx = max_bytes;
    while idx > 0 && !text.is_char_boundary(idx) {
        idx -= 1;
    }
    &text[..idx]
}

fn truncate_string_to_bytes(value: &mut String, max_bytes: usize) {
    if max_bytes >= value.len() {
        return;
    }
    let mut idx = max_bytes;
    while idx > 0 && !value.is_char_boundary(idx) {
        idx -= 1;
    }
    value.truncate(idx);
}

fn mark_output_text_truncated(buffer: &mut String, limit: usize) {
    if limit == 0 {
        return;
    }
    if buffer.ends_with(OUTPUT_TEXT_TRUNCATED_MARKER) {
        return;
    }
    let newline_bytes = if buffer.is_empty() { 0 } else { 1 };
    let marker_bytes = OUTPUT_TEXT_TRUNCATED_MARKER.len();
    if buffer.len() + newline_bytes + marker_bytes <= limit {
        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(OUTPUT_TEXT_TRUNCATED_MARKER);
        return;
    }
    if limit <= marker_bytes {
        truncate_string_to_bytes(buffer, limit);
        return;
    }
    let target = limit.saturating_sub(marker_bytes + newline_bytes);
    truncate_string_to_bytes(buffer, target);
    if !buffer.is_empty() {
        buffer.push('\n');
    }
    buffer.push_str(OUTPUT_TEXT_TRUNCATED_MARKER);
}

pub(in super::super) fn collect_output_text_from_event_fields(value: &Value, output: &mut String) {
    if let Some(item) = value.get("item") {
        collect_response_output_text(item, output);
    }
    if let Some(output_item) = value.get("output_item") {
        collect_response_output_text(output_item, output);
    }
    if let Some(part) = value.get("part") {
        collect_response_output_text(part, output);
    }
    if let Some(content_part) = value.get("content_part") {
        collect_response_output_text(content_part, output);
    }
}

fn extract_output_text_from_json(value: &Value) -> Option<String> {
    let mut output = String::new();
    if let Some(text) = value.get("output_text").and_then(Value::as_str) {
        append_output_text(&mut output, text);
    }
    if let Some(response) = value.get("response") {
        collect_response_output_text(response, &mut output);
    }
    if let Some(top_level_output) = value.get("output") {
        collect_response_output_text(top_level_output, &mut output);
    }
    if let Some(choices) = value.get("choices") {
        collect_response_output_text(choices, &mut output);
    }
    if let Some(item) = value.get("item") {
        collect_response_output_text(item, &mut output);
    }
    if let Some(part) = value.get("part") {
        collect_response_output_text(part, &mut output);
    }
    if output.trim().is_empty() {
        None
    } else {
        Some(output)
    }
}

pub(in super::super) fn parse_usage_from_json(value: &Value) -> UpstreamResponseUsage {
    let mut usage = parse_usage_from_object(value.get("usage").and_then(Value::as_object));
    let response_usage = value
        .get("response")
        .and_then(|response| response.get("usage"))
        .and_then(Value::as_object);
    merge_usage(&mut usage, parse_usage_from_object(response_usage));
    usage.output_text = extract_output_text_from_json(value);
    usage
}

pub(crate) fn extract_error_message_from_json(value: &Value) -> Option<String> {
    fn extract_message_from_error_map(err_obj: &Map<String, Value>) -> Option<String> {
        let message = err_obj
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| err_obj.get("error").and_then(Value::as_str))
            .or_else(|| err_obj.get("detail").and_then(Value::as_str))
            .map(str::trim)
            .filter(|msg| !msg.is_empty());
        let code = err_obj
            .get("code")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty());
        let kind = err_obj
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty());
        let param = err_obj
            .get("param")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty());

        if let Some(message) = message {
            let mut prefixes = Vec::new();
            if let Some(code) = code {
                prefixes.push(format!("code={code}"));
            }
            if let Some(kind) = kind {
                prefixes.push(format!("type={kind}"));
            }
            if let Some(param) = param {
                prefixes.push(format!("param={param}"));
            }
            return if prefixes.is_empty() {
                Some(message.to_string())
            } else {
                Some(format!("{} {}", prefixes.join(" "), message))
            };
        }

        serde_json::to_string(err_obj)
            .ok()
            .map(|text| text.trim().to_string())
            .filter(|v| !v.is_empty())
    }

    fn extract_message_from_error_value(err_value: Option<&Value>) -> Option<String> {
        let err_value = err_value?;
        if let Some(message) = err_value.as_str() {
            let msg = message.trim();
            if !msg.is_empty() {
                return Some(msg.to_string());
            }
            return None;
        }
        if let Some(err_obj) = err_value.as_object() {
            return extract_message_from_error_map(err_obj);
        }
        None
    }

    if let Some(message) = extract_message_from_error_value(value.get("error")) {
        return Some(message);
    }
    if let Some(message) = value.get("detail").and_then(Value::as_str) {
        let msg = message.trim();
        if !msg.is_empty() {
            return Some(msg.to_string());
        }
    }
    if let Some(message) = extract_message_from_error_value(value.pointer("/response/error")) {
        return Some(message);
    }
    if let Some(message) =
        extract_message_from_error_value(value.pointer("/response/status_details/error"))
    {
        return Some(message);
    }
    if value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|t| t.eq_ignore_ascii_case("error"))
    {
        if let Some(message) = value.get("message").and_then(Value::as_str) {
            let msg = message.trim();
            if !msg.is_empty() {
                return Some(msg.to_string());
            }
        }
    }
    None
}

pub(in super::super) fn extract_error_hint_from_body(
    status_code: u16,
    body: &[u8],
) -> Option<String> {
    if status_code < 400 || body.is_empty() {
        return None;
    }
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        let compact_json = serde_json::to_string(&value).ok();
        if let Some(message) = extract_error_message_from_json(&value) {
            return Some(limit_upstream_error_hint(
                summarize_upstream_error_hint(status_code, message.as_str()).as_str(),
            ));
        }
        if let Some(json_text) = compact_json
            .as_deref()
            .map(str::trim)
            .filter(|text| !text.is_empty())
        {
            return Some(limit_upstream_error_hint(
                summarize_upstream_error_hint(status_code, json_text).as_str(),
            ));
        }
    }
    std::str::from_utf8(body)
        .ok()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(|text| summarize_upstream_error_hint(status_code, text))
}

fn limit_upstream_error_hint(raw: &str) -> String {
    let text = raw.trim();
    if text.is_empty() {
        return String::new();
    }
    if text.len() <= UPSTREAM_ERROR_HINT_LIMIT_BYTES {
        return text.to_string();
    }
    let mut snippet = truncate_str_to_bytes(text, UPSTREAM_ERROR_HINT_LIMIT_BYTES).to_string();
    snippet.push_str("...[truncated]");
    snippet
}

fn summarize_upstream_error_hint(status_code: u16, raw: &str) -> String {
    let text = raw.trim();
    if text.is_empty() {
        return "上游返回异常".to_string();
    }

    let normalized = text.to_ascii_lowercase();
    let looks_like_html = normalized.contains("<html")
        || normalized.contains("<!doctype html")
        || normalized.contains("<body")
        || normalized.contains("</html>");
    let looks_like_challenge = normalized.contains("cloudflare")
        || normalized.contains("cf-chl")
        || normalized.contains("just a moment")
        || normalized.contains("attention required")
        || normalized.contains("captcha")
        || normalized.contains("security check")
        || normalized.contains("access denied")
        || normalized.contains("waf");

    if looks_like_challenge || (looks_like_html && matches!(status_code, 401 | 403)) {
        return summarize_cloudflare_challenge(text);
    }
    if looks_like_html {
        return summarize_html_error_page(text);
    }
    if let Some(summary) = summarize_model_not_supported(text) {
        return summary;
    }
    if normalized.contains("timed out") || normalized.contains("timeout") {
        return "上游请求超时".to_string();
    }
    if normalized.contains("connection reset")
        || normalized.contains("broken pipe")
        || normalized.contains("connection aborted")
        || normalized.contains("forcibly closed")
        || normalized.contains("unexpected eof")
    {
        return "上游连接中断".to_string();
    }
    text.to_string()
}

fn summarize_model_not_supported(raw: &str) -> Option<String> {
    let normalized = raw.trim().to_ascii_lowercase();
    let looks_unsupported = normalized.contains("model_not_found")
        || normalized.contains("model not found")
        || normalized.contains("unsupported model")
        || normalized.contains("not support")
        || normalized.contains("not supported")
        || normalized.contains("does not support")
        || normalized.contains("does not exist")
        || normalized.contains("unknown model");
    if !looks_unsupported {
        return None;
    }
    let model = extract_model_name(raw);
    Some(match model {
        Some(model) => format!("模型不支持（{model}）"),
        None => "模型不支持".to_string(),
    })
}

fn summarize_cloudflare_challenge(raw: &str) -> String {
    let title = extract_html_title(raw);
    let ray = extract_object_string_field(raw, "cRay");
    let zone = extract_object_string_field(raw, "cZone");
    let mut details = Vec::new();
    if let Some(title) = title.as_deref().filter(|text| !text.is_empty()) {
        details.push(format!("title={title}"));
    }
    if let Some(ray) = ray.as_deref().filter(|text| !text.is_empty()) {
        details.push(format!("ray={ray}"));
    }
    if let Some(zone) = zone.as_deref().filter(|text| !text.is_empty()) {
        details.push(format!("zone={zone}"));
    }
    if details.is_empty() {
        "Cloudflare 安全验证页".to_string()
    } else {
        format!("Cloudflare 安全验证页（{}）", details.join(", "))
    }
}

fn summarize_html_error_page(raw: &str) -> String {
    if let Some(title) = extract_html_title(raw) {
        if !title.is_empty() {
            return format!("上游返回 HTML 错误页（title={title}）");
        }
    }
    "上游返回 HTML 错误页".to_string()
}

fn extract_html_title(raw: &str) -> Option<String> {
    let lower = raw.to_ascii_lowercase();
    let start_tag = "<title>";
    let end_tag = "</title>";
    let start = lower.find(start_tag)? + start_tag.len();
    let end = lower[start..].find(end_tag)? + start;
    let title = raw.get(start..end)?.trim();
    (!title.is_empty()).then(|| title.to_string())
}

fn extract_object_string_field(raw: &str, key: &str) -> Option<String> {
    let start = raw.find(key)?;
    let after_key = raw.get(start + key.len()..)?;
    let colon = after_key.find(':')?;
    let after_colon = after_key.get(colon + 1..)?.trim_start();
    let quote = after_colon.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let value = after_colon.get(1..)?;
    let end = value.find(quote)?;
    let extracted = value.get(..end)?.trim();
    (!extracted.is_empty()).then(|| extracted.to_string())
}

fn extract_model_name(raw: &str) -> Option<String> {
    let lowered = raw.to_ascii_lowercase();
    if let Some(model) = extract_quoted_model_before_keyword(raw, &lowered) {
        return Some(model);
    }
    for marker in [
        "the model ",
        "model=",
        "model:",
        "model ",
        "unsupported model ",
        "unknown model ",
    ] {
        if let Some(start) = lowered.find(marker) {
            let offset = start + marker.len();
            if let Some(fragment) = raw.get(offset..) {
                if let Some(model) = extract_model_token(fragment.trim_start()) {
                    return Some(model);
                }
            }
        }
    }
    None
}

fn extract_quoted_model_before_keyword(raw: &str, lowered: &str) -> Option<String> {
    for keyword in [" model", " models"] {
        let keyword_idx = lowered.find(keyword)?;
        let prefix = raw.get(..keyword_idx)?.trim_end();
        for quote in ['\'', '"', '`'] {
            let end = prefix.rfind(quote)?;
            let before_end = prefix.get(..end)?;
            let start = before_end.rfind(quote)?;
            let model = before_end.get(start + 1..)?.trim();
            if !model.is_empty() {
                return Some(model.to_string());
            }
        }
    }
    None
}

fn extract_model_token(fragment: &str) -> Option<String> {
    let fragment = fragment
        .trim_start_matches(|c: char| c.is_whitespace() || matches!(c, '=' | ':' | '(' | '['));
    let mut chars = fragment.chars();
    let first = chars.next()?;
    let token = if first == '\'' || first == '"' || first == '`' {
        let end = fragment.get(1..)?.find(first)? + 1;
        fragment.get(1..end)?.trim()
    } else {
        let end = fragment
            .char_indices()
            .find(|(_, ch)| {
                !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':' | '/'))
            })
            .map(|(idx, _)| idx)
            .unwrap_or(fragment.len());
        fragment.get(..end)?.trim()
    };
    if token.is_empty()
        || token.eq_ignore_ascii_case("is")
        || token.eq_ignore_ascii_case("not")
        || token.eq_ignore_ascii_case("found")
        || token.eq_ignore_ascii_case("unsupported")
    {
        return None;
    }
    Some(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        extract_error_hint_from_body, limit_upstream_error_hint, summarize_upstream_error_hint,
        UpstreamResponseBridgeResult, UPSTREAM_ERROR_HINT_LIMIT_BYTES,
    };

    #[test]
    fn summarize_upstream_error_hint_recognizes_challenge_html() {
        assert_eq!(
            summarize_upstream_error_hint(403, "<html><title>Just a moment...</title>"),
            "Cloudflare 安全验证页（title=Just a moment...）"
        );
    }

    #[test]
    fn summarize_upstream_error_hint_recognizes_generic_html() {
        assert_eq!(
            summarize_upstream_error_hint(502, "<!doctype html><html><body>error</body></html>"),
            "上游返回 HTML 错误页"
        );
    }

    #[test]
    fn summarize_upstream_error_hint_recognizes_unsupported_model() {
        assert_eq!(
            summarize_upstream_error_hint(
                400,
                "code=model_not_found type=invalid_request_error The model 'gpt-5.4' does not exist"
            ),
            "模型不支持（gpt-5.4）"
        );
        assert_eq!(
            summarize_upstream_error_hint(400, "unsupported model"),
            "模型不支持"
        );
    }

    #[test]
    fn extract_error_hint_from_body_summarizes_html_body() {
        assert_eq!(
            extract_error_hint_from_body(403, b"<html><body>Cloudflare</body></html>").as_deref(),
            Some("Cloudflare 安全验证页")
        );
    }

    #[test]
    fn extract_error_hint_from_body_prefers_json_message() {
        let body = br#"{"error":{"message":"forbidden","type":"permission_error"}}"#;
        assert_eq!(
            extract_error_hint_from_body(403, body).as_deref(),
            Some("type=permission_error forbidden")
        );
    }

    #[test]
    fn extract_error_hint_from_body_summarizes_unsupported_model_json() {
        let body = br#"{"error":{"message":"The model 'gpt-5.4' does not exist","type":"invalid_request_error","code":"model_not_found"}}"#;
        assert_eq!(
            extract_error_hint_from_body(400, body).as_deref(),
            Some("模型不支持（gpt-5.4）")
        );
    }

    #[test]
    fn extract_error_hint_from_body_summarizes_unsupported_model_detail_json() {
        let body = br#"{"detail":"The 'gpt-5.4' model is not supported when using Codex with a ChatGPT account."}"#;
        assert_eq!(
            extract_error_hint_from_body(400, body).as_deref(),
            Some("模型不支持（gpt-5.4）")
        );
    }

    #[test]
    fn limit_upstream_error_hint_truncates_large_body() {
        let raw = "x".repeat(UPSTREAM_ERROR_HINT_LIMIT_BYTES + 32);
        let text = limit_upstream_error_hint(&raw);
        assert!(text.ends_with("...[truncated]"));
        assert!(text.len() > UPSTREAM_ERROR_HINT_LIMIT_BYTES);
    }

    #[test]
    fn bridge_error_message_reports_stream_incomplete_in_chinese() {
        let bridge = UpstreamResponseBridgeResult {
            stream_terminal_seen: false,
            ..UpstreamResponseBridgeResult::default()
        };
        assert_eq!(
            bridge.error_message(true).as_deref(),
            Some("上游流中途中断（未正常结束）")
        );
    }
}
