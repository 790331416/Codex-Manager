use serde_json::{json, Map, Value};
use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex};

use super::super::{
    convert_openai_chat_stream_chunk_with_tool_name_restore_map,
    convert_openai_completions_stream_chunk, ToolNameRestoreMap,
};
use super::{
    append_output_text, apply_openai_stream_meta_defaults, build_chat_fallback_content_chunk,
    build_completion_fallback_text_chunk, collect_output_text_from_event_fields,
    collect_response_output_text, extract_openai_completed_output_text, extract_sse_frame_payload,
    inspect_sse_frame, inspect_sse_frame_for_protocol, is_response_completed_event_name,
    map_chunk_has_chat_text, map_chunk_has_completion_text, merge_usage,
    normalize_chat_chunk_delta_role, parse_sse_frame_json, should_skip_chat_live_text_event,
    should_skip_completion_live_text_event, update_openai_stream_meta, OpenAIStreamMeta,
    PassthroughSseProtocol, SseTerminal, UpstreamResponseUsage,
};

#[path = "stream_readers/anthropic.rs"]
mod anthropic;
#[path = "stream_readers/common.rs"]
mod common;
#[path = "stream_readers/gemini.rs"]
mod gemini;
#[path = "stream_readers/openai_chat.rs"]
mod openai_chat;
#[path = "stream_readers/openai_completions.rs"]
mod openai_completions;
#[path = "stream_readers/passthrough.rs"]
mod passthrough;

pub(crate) use anthropic::AnthropicSseReader;
use common::{
    classify_upstream_stream_read_error, collector_output_text_trimmed,
    mark_collector_terminal_success, sse_keepalive_interval, stream_reader_disconnected_message,
    upstream_hint_or_stream_incomplete_message,
};
pub(crate) use common::{
    PassthroughSseCollector, SseKeepAliveFrame, UpstreamSseFramePump, UpstreamSseFramePumpItem,
};
pub(crate) use gemini::GeminiSseReader;
pub(crate) use openai_chat::OpenAIChatCompletionsSseReader;
pub(crate) use openai_completions::OpenAICompletionsSseReader;
pub(crate) use passthrough::PassthroughSseUsageReader;

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
    common::reload_from_env();
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
    common::current_sse_keepalive_interval_ms()
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
    common::set_sse_keepalive_interval_ms(interval_ms)
}
