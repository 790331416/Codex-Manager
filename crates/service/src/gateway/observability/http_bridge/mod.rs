use tiny_http::{Header, Request};

mod aggregate;
mod openai;
use aggregate::{
    append_output_text, collect_non_stream_json_from_sse_bytes,
    collect_output_text_from_event_fields, collect_response_output_text,
    extract_error_hint_from_body, extract_sse_frame_payload, inspect_sse_frame,
    is_response_completed_event_name, looks_like_sse_payload, merge_usage,
    parse_sse_frame_json, parse_usage_from_json, reload_output_text_from_env, usage_has_signal,
    SseTerminal, UpstreamResponseBridgeResult, UpstreamResponseUsage,
};
#[cfg(test)]
use aggregate::{output_text_limit_bytes, parse_usage_from_sse_frame, OUTPUT_TEXT_TRUNCATED_MARKER};
use openai::{
    apply_openai_stream_meta_defaults, build_chat_fallback_content_chunk,
    build_completion_fallback_text_chunk, extract_openai_completed_output_text,
    map_chunk_has_chat_text, map_chunk_has_completion_text, normalize_chat_chunk_delta_role,
    should_skip_chat_live_text_event, should_skip_completion_live_text_event,
    synthesize_chat_completion_sse_from_json, synthesize_completions_sse_from_json,
    update_openai_stream_meta, OpenAIStreamMeta,
};

pub(super) fn reload_from_env() {
    reload_output_text_from_env();
}

fn push_trace_id_header(headers: &mut Vec<Header>, trace_id: &str) {
    let Some(trace_id) = Some(trace_id)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    if let Ok(header) = Header::from_bytes(
        crate::error_codes::TRACE_ID_HEADER_NAME.as_bytes(),
        trace_id.as_bytes(),
    ) {
        headers.push(header);
    }
}

mod delivery;
mod stream_readers;
pub(super) fn respond_with_upstream(
    request: Request,
    upstream: reqwest::blocking::Response,
    inflight_guard: super::AccountInFlightGuard,
    response_adapter: super::ResponseAdapter,
    tool_name_restore_map: Option<&super::ToolNameRestoreMap>,
    is_stream: bool,
    trace_id: Option<&str>,
) -> Result<UpstreamResponseBridgeResult, String> {
    delivery::respond_with_upstream(
        request,
        upstream,
        inflight_guard,
        response_adapter,
        tool_name_restore_map,
        is_stream,
        trace_id,
    )
}
pub(super) use stream_readers::{
    AnthropicSseReader, OpenAIChatCompletionsSseReader, OpenAICompletionsSseReader,
    PassthroughSseCollector, PassthroughSseUsageReader,
};

#[cfg(test)]
#[path = "../tests/http_bridge_tests.rs"]
mod tests;
