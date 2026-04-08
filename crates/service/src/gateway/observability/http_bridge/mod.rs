use tiny_http::{Header, Request};

mod aggregate;
mod openai;
use aggregate::{
    append_output_text, collect_non_stream_json_from_sse_bytes,
    collect_output_text_from_event_fields, collect_response_output_text,
    extract_error_hint_from_body, extract_error_message_from_json, extract_sse_frame_payload,
    inspect_sse_frame, inspect_sse_frame_for_protocol, is_response_completed_event_name,
    looks_like_sse_payload, merge_usage, parse_sse_frame_json, parse_usage_from_json,
    reload_output_text_from_env, usage_has_signal, SseTerminal,
    UpstreamResponseBridgeResult, UpstreamResponseUsage,
};
pub(crate) use aggregate::PassthroughSseProtocol;
#[cfg(test)]
use aggregate::{
    output_text_limit_bytes, parse_usage_from_sse_frame, OUTPUT_TEXT_TRUNCATED_MARKER,
};
use openai::{
    apply_openai_stream_meta_defaults, build_chat_fallback_content_chunk,
    build_completion_fallback_text_chunk, extract_openai_completed_output_text,
    map_chunk_has_chat_text, map_chunk_has_completion_text, normalize_chat_chunk_delta_role,
    should_skip_chat_live_text_event, should_skip_completion_live_text_event,
    synthesize_chat_completion_sse_from_json, synthesize_completions_sse_from_json,
    update_openai_stream_meta, OpenAIStreamMeta,
};

/// 函数 `reload_from_env`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 无
pub(super) fn reload_from_env() {
    reload_output_text_from_env();
    stream_readers::reload_from_env();
}

/// 函数 `current_sse_keepalive_interval_ms`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 返回函数执行结果
pub(super) fn current_sse_keepalive_interval_ms() -> u64 {
    stream_readers::current_sse_keepalive_interval_ms()
}

/// 函数 `set_sse_keepalive_interval_ms`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 返回函数执行结果
pub(super) fn set_sse_keepalive_interval_ms(interval_ms: u64) -> Result<u64, String> {
    stream_readers::set_sse_keepalive_interval_ms(interval_ms)
}

/// 函数 `summarize_upstream_error_hint_from_body`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - crate: 参数 crate
///
/// # 返回
/// 返回函数执行结果
pub(crate) fn summarize_upstream_error_hint_from_body(
    status_code: u16,
    body: &[u8],
) -> Option<String> {
    aggregate::extract_error_hint_from_body(status_code, body)
}

/// 函数 `push_trace_id_header`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - headers: 参数 headers
/// - trace_id: 参数 trace_id
///
/// # 返回
/// 无
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
/// 函数 `respond_with_upstream`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 返回函数执行结果
pub(super) fn respond_with_upstream(
    request: Request,
    upstream: reqwest::blocking::Response,
    inflight_guard: super::AccountInFlightGuard,
    response_adapter: super::ResponseAdapter,
    passthrough_sse_protocol: Option<PassthroughSseProtocol>,
    gemini_stream_output_mode: Option<super::GeminiStreamOutputMode>,
    request_path: &str,
    tool_name_restore_map: Option<&super::ToolNameRestoreMap>,
    is_stream: bool,
    allow_failover_for_deactivation: bool,
    trace_id: Option<&str>,
) -> Result<UpstreamResponseBridgeResult, String> {
    delivery::respond_with_upstream(
        request,
        upstream,
        inflight_guard,
        response_adapter,
        passthrough_sse_protocol,
        gemini_stream_output_mode,
        request_path,
        tool_name_restore_map,
        is_stream,
        allow_failover_for_deactivation,
        trace_id,
    )
}
pub(super) use stream_readers::{
    AnthropicSseReader, GeminiSseReader, OpenAIChatCompletionsSseReader,
    OpenAICompletionsSseReader, PassthroughSseCollector, PassthroughSseUsageReader,
    SseKeepAliveFrame,
};

#[cfg(test)]
#[path = "../tests/http_bridge_tests.rs"]
mod tests;
