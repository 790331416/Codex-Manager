use crate::apikey_profile::{PROTOCOL_ANTHROPIC_NATIVE, PROTOCOL_OPENAI_COMPAT};
use serde_json::Value;

mod prompt_cache;
mod request_mapping;
mod response_conversion;

pub(super) type ToolNameRestoreMap = std::collections::BTreeMap<String, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponseAdapter {
    Passthrough,
    AnthropicJson,
    AnthropicSse,
    OpenAIChatCompletionsJson,
    OpenAIChatCompletionsSse,
    OpenAICompletionsJson,
    OpenAICompletionsSse,
}

#[derive(Debug)]
pub(super) struct AdaptedGatewayRequest {
    pub(super) path: String,
    pub(super) body: Vec<u8>,
    pub(super) response_adapter: ResponseAdapter,
    pub(super) tool_name_restore_map: ToolNameRestoreMap,
}

pub(super) fn adapt_request_for_protocol(
    protocol_type: &str,
    path: &str,
    body: Vec<u8>,
) -> Result<AdaptedGatewayRequest, String> {
    if protocol_type == PROTOCOL_OPENAI_COMPAT
        && (path == "/v1/chat/completions" || path.starts_with("/v1/chat/completions?"))
    {
        let (adapted_body, request_stream, tool_name_restore_map) =
            request_mapping::convert_openai_chat_completions_request(&body)?;
        let adapted_path = if let Some(suffix) = path.strip_prefix("/v1/chat/completions") {
            format!("/v1/responses{suffix}")
        } else {
            "/v1/responses".to_string()
        };
        return Ok(AdaptedGatewayRequest {
            path: adapted_path,
            body: adapted_body,
            response_adapter: if request_stream {
                ResponseAdapter::OpenAIChatCompletionsSse
            } else {
                ResponseAdapter::OpenAIChatCompletionsJson
            },
            tool_name_restore_map,
        });
    }

    if protocol_type == PROTOCOL_OPENAI_COMPAT
        && (path == "/v1/completions" || path.starts_with("/v1/completions?"))
    {
        let (chat_body, _) = request_mapping::convert_openai_completions_request(&body)?;
        let (adapted_body, request_stream, tool_name_restore_map) =
            request_mapping::convert_openai_chat_completions_request(&chat_body)?;
        let adapted_path = if let Some(suffix) = path.strip_prefix("/v1/completions") {
            format!("/v1/responses{suffix}")
        } else {
            "/v1/responses".to_string()
        };
        return Ok(AdaptedGatewayRequest {
            path: adapted_path,
            body: adapted_body,
            response_adapter: if request_stream {
                ResponseAdapter::OpenAICompletionsSse
            } else {
                ResponseAdapter::OpenAICompletionsJson
            },
            tool_name_restore_map,
        });
    }

    if protocol_type != PROTOCOL_ANTHROPIC_NATIVE {
        return Ok(AdaptedGatewayRequest {
            path: path.to_string(),
            body,
            response_adapter: ResponseAdapter::Passthrough,
            tool_name_restore_map: ToolNameRestoreMap::new(),
        });
    }

    if path == "/v1/messages" || path.starts_with("/v1/messages?") {
        let (adapted_body, request_stream) =
            request_mapping::convert_anthropic_messages_request(&body)?;
        // 说明：non-stream 也统一走 /v1/responses。
        // 在部分账号/环境下 /v1/responses/compact 更容易触发 challenge 或非预期拦截。
        let adapted_path = "/v1/responses".to_string();
        return Ok(AdaptedGatewayRequest {
            path: adapted_path,
            body: adapted_body,
            response_adapter: if request_stream {
                ResponseAdapter::AnthropicSse
            } else {
                ResponseAdapter::AnthropicJson
            },
            tool_name_restore_map: ToolNameRestoreMap::new(),
        });
    }

    Ok(AdaptedGatewayRequest {
        path: path.to_string(),
        body,
        response_adapter: ResponseAdapter::Passthrough,
        tool_name_restore_map: ToolNameRestoreMap::new(),
    })
}

pub(super) fn adapt_upstream_response(
    adapter: ResponseAdapter,
    upstream_content_type: Option<&str>,
    body: &[u8],
) -> Result<(Vec<u8>, &'static str), String> {
    response_conversion::adapt_upstream_response(adapter, upstream_content_type, body, None)
}

pub(super) fn adapt_upstream_response_with_tool_name_restore_map(
    adapter: ResponseAdapter,
    upstream_content_type: Option<&str>,
    body: &[u8],
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
) -> Result<(Vec<u8>, &'static str), String> {
    response_conversion::adapt_upstream_response(
        adapter,
        upstream_content_type,
        body,
        tool_name_restore_map,
    )
}

pub(super) fn build_anthropic_error_body(message: &str) -> Vec<u8> {
    response_conversion::build_anthropic_error_body(message)
}

pub(super) fn convert_openai_completions_stream_chunk(value: &Value) -> Option<Value> {
    response_conversion::convert_openai_completions_stream_chunk(value)
}

#[allow(dead_code)]
pub(super) fn convert_openai_chat_stream_chunk(value: &Value) -> Option<Value> {
    response_conversion::convert_openai_chat_stream_chunk(value)
}

pub(super) fn convert_openai_chat_stream_chunk_with_tool_name_restore_map(
    value: &Value,
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
) -> Option<Value> {
    response_conversion::convert_openai_chat_stream_chunk_with_tool_name_restore_map(
        value,
        tool_name_restore_map,
    )
}

pub(super) fn reload_env_dependent_state() {
    prompt_cache::clear_runtime_state();
    prompt_cache::reload_from_env();
}

#[cfg(test)]
#[path = "tests/protocol_adapter_tests.rs"]
mod tests;
