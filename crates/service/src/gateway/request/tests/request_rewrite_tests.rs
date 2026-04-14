use super::{
    apply_request_overrides, apply_request_overrides_with_forced_prompt_cache_key,
    apply_request_overrides_with_prompt_cache_key, apply_request_overrides_with_service_tier,
};
use serde_json::json;

const STRICT_REQUEST_PARAM_ALLOWLIST_ENV: &str = "CODEXMANAGER_STRICT_REQUEST_PARAM_ALLOWLIST";
const GATEWAY_MODE_ENV: &str = "CODEXMANAGER_GATEWAY_MODE";

struct RuntimeEnvGuard {
    name: &'static str,
    previous_value: Option<String>,
}

impl RuntimeEnvGuard {
    fn set(name: &'static str, value: &str) -> Self {
        let previous_value = std::env::var(name).ok();
        std::env::set_var(name, value);
        crate::gateway::reload_runtime_config_from_env();
        Self {
            name,
            previous_value,
        }
    }
}

impl Drop for RuntimeEnvGuard {
    fn drop(&mut self) {
        match self.previous_value.as_deref() {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
        crate::gateway::reload_runtime_config_from_env();
    }
}

/// 函数 `chat_completions_stream_enforces_include_usage`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn chat_completions_stream_enforces_include_usage() {
    let body = json!({
        "model": "gpt-4o",
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }]
    });
    let out = apply_request_overrides(
        "/v1/chat/completions",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        None,
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("stream_options")
            .and_then(|v| v.get("include_usage"))
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

/// 函数 `chat_completions_stream_preserves_options_while_enabling_usage`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn chat_completions_stream_preserves_options_while_enabling_usage() {
    let body = json!({
        "model": "gpt-4o",
        "stream": true,
        "stream_options": { "include_usage": false, "foo": "bar" },
        "messages": [{ "role": "user", "content": "hi" }]
    });
    let out = apply_request_overrides(
        "/v1/chat/completions",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        None,
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("stream_options")
            .and_then(|v| v.get("include_usage"))
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        value
            .get("stream_options")
            .and_then(|v| v.get("foo"))
            .and_then(serde_json::Value::as_str),
        Some("bar")
    );
}

/// 函数 `chat_completions_uses_reasoning_effort_and_drops_non_official_keys`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn chat_completions_uses_reasoning_effort_and_drops_non_official_keys() {
    let _env_guard = crate::test_env_guard();
    let _strict_guard = RuntimeEnvGuard::set(STRICT_REQUEST_PARAM_ALLOWLIST_ENV, "1");
    let body = json!({
        "model": "gpt-4.1",
        "messages": [{ "role": "user", "content": "hi" }],
        "reasoning": { "effort": "high" },
        "metadata": { "source": "test" },
        "unknown_field": "drop-me"
    });
    let out = apply_request_overrides(
        "/v1/chat/completions",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        Some("medium"),
        None,
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("reasoning_effort")
            .and_then(serde_json::Value::as_str),
        Some("medium")
    );
    assert!(value.get("reasoning").is_none());
    assert!(value.get("unknown_field").is_none());
    assert!(value.get("metadata").is_some());
}

/// 函数 `chat_completions_accepts_responses_style_payload`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn chat_completions_accepts_responses_style_payload() {
    let body = json!({
        "model": "gpt-4.1",
        "instructions": "act as reviewer",
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": "hello" }
                ]
            }
        ],
        "reasoning": { "effort": "high" },
        "stream": true
    });
    let out = apply_request_overrides(
        "/v1/chat/completions",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        None,
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert!(value.get("instructions").is_none());
    assert!(value.get("input").is_none());
    assert_eq!(
        value
            .get("messages")
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("role"))
            .and_then(serde_json::Value::as_str),
        Some("system")
    );
    assert_eq!(
        value
            .get("messages")
            .and_then(|v| v.get(1))
            .and_then(|v| v.get("content"))
            .and_then(serde_json::Value::as_str),
        Some("hello")
    );
    assert_eq!(
        value
            .get("reasoning_effort")
            .and_then(serde_json::Value::as_str),
        Some("high")
    );
    assert_eq!(
        value
            .get("stream_options")
            .and_then(|v| v.get("include_usage"))
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

/// 函数 `chat_completions_normalizes_responses_function_tools`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn chat_completions_normalizes_responses_function_tools() {
    let body = json!({
        "model": "gpt-4.1",
        "input": "ping",
        "tools": [
            {
                "type": "function",
                "name": "ping",
                "description": "ping tool",
                "parameters": { "type": "object", "properties": {} },
                "strict": true
            }
        ],
        "tool_choice": {
            "type": "function",
            "name": "ping"
        }
    });
    let out = apply_request_overrides(
        "/v1/chat/completions",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        None,
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("tools")
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("function"))
            .and_then(|v| v.get("name"))
            .and_then(serde_json::Value::as_str),
        Some("ping")
    );
    assert_eq!(
        value
            .get("tool_choice")
            .and_then(|v| v.get("function"))
            .and_then(|v| v.get("name"))
            .and_then(serde_json::Value::as_str),
        Some("ping")
    );
}

/// 函数 `responses_overrides_model_and_reasoning_effort`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_overrides_model_and_reasoning_effort() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "reasoning": { "effort": "high" },
        "input": [{ "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "hi" }] }]
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        Some("gpt-5.3-codex"),
        Some("medium"),
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value.get("model").and_then(serde_json::Value::as_str),
        Some("gpt-5.3-codex")
    );
    assert_eq!(
        value
            .get("reasoning")
            .and_then(|v| v.get("effort"))
            .and_then(serde_json::Value::as_str),
        Some("medium")
    );
    assert_eq!(
        value
            .get("instructions")
            .and_then(serde_json::Value::as_str),
        Some("")
    );
}

/// 函数 `responses_input_string_normalized_to_list`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_input_string_normalized_to_list() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("input")
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("type"))
            .and_then(serde_json::Value::as_str),
        Some("message")
    );
}

/// 函数 `responses_stream_and_store_are_forced_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_stream_and_store_are_forced_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "stream": false,
        "store": true
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value.get("stream").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        value.get("store").and_then(serde_json::Value::as_bool),
        Some(false)
    );
}

#[test]
fn responses_transparent_mode_preserves_native_codex_body_shape() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "transparent");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "stream": false,
        "store": true,
        "reasoning": { "effort": "medium" }
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value.get("input").and_then(serde_json::Value::as_str),
        Some("hello")
    );
    assert_eq!(
        value.get("stream").and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        value.get("store").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(value.get("instructions").is_none());
    assert!(value.get("tool_choice").is_none());
    assert!(value.get("parallel_tool_calls").is_none());
    assert!(value.get("include").is_none());
}

#[test]
fn responses_transparent_mode_skips_request_rewrite_layer_prompt_cache_inference() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "transparent");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello"
    });
    let out = apply_request_overrides_with_prompt_cache_key(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
        Some("thread_123"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert!(value.get("prompt_cache_key").is_none());
}

/// 函数 `responses_infers_prompt_cache_key_from_conversation_id_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_infers_prompt_cache_key_from_conversation_id_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello"
    });
    let out = apply_request_overrides_with_prompt_cache_key(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
        Some("thread_123"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("prompt_cache_key")
            .and_then(serde_json::Value::as_str),
        Some("thread_123")
    );
}

/// 函数 `responses_forced_prompt_cache_key_overrides_existing_value_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_forced_prompt_cache_key_overrides_existing_value_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "prompt_cache_key": "thread_old"
    });
    let out = apply_request_overrides_with_forced_prompt_cache_key(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
        Some("thread_new"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("prompt_cache_key")
            .and_then(serde_json::Value::as_str),
        Some("thread_new")
    );
}

/// 函数 `responses_stream_passthrough_keeps_client_stream_flag_when_enabled`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_stream_passthrough_keeps_client_stream_flag_when_enabled() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "stream": false,
        "stream_passthrough": true,
        "store": true
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value.get("stream").and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        value.get("store").and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert!(value.get("stream_passthrough").is_none());
}

/// 函数 `responses_dynamic_tools_are_mapped_to_tools_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_dynamic_tools_are_mapped_to_tools_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "tools": [{
            "type": "function",
            "name": "existing_tool",
            "parameters": { "type": "object", "properties": {} }
        }],
        "dynamicTools": [{
            "name": "dynamic_weather",
            "description": "lookup weather",
            "input_schema": {
                "type": "object",
                "properties": {
                    "city": { "type": "string" }
                },
                "required": ["city"]
            }
        }]
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    let tools = value
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .expect("tools array");
    assert_eq!(tools.len(), 2);
    assert!(value.get("dynamicTools").is_none());
    assert_eq!(
        tools[1].get("name").and_then(serde_json::Value::as_str),
        Some("dynamic_weather")
    );
    assert_eq!(
        tools[1]
            .get("description")
            .and_then(serde_json::Value::as_str),
        Some("lookup weather")
    );
    assert_eq!(
        tools[1]
            .get("parameters")
            .and_then(|parameters| parameters.get("properties"))
            .and_then(|properties| properties.get("city"))
            .and_then(|city| city.get("type"))
            .and_then(serde_json::Value::as_str),
        Some("string")
    );
}

/// 函数 `responses_preserves_priority_service_tier_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_preserves_priority_service_tier_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "instructions": "stay",
        "input": [{ "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "hello" }] }],
        "tools": [{ "type": "function", "name": "ping", "parameters": { "type": "object", "properties": {} } }],
        "tool_choice": "auto",
        "parallel_tool_calls": true,
        "reasoning": { "effort": "high" },
        "stream": true,
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "prompt_cache_key": "pc_1",
        "text": { "format": { "type": "text" } },
        "service_tier": "priority",
        "temperature": 0.7,
        "user": "cherry-studio"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert!(value.get("model").is_some());
    assert!(value.get("instructions").is_some());
    assert!(value.get("input").is_some());
    assert!(value.get("max_output_tokens").is_none());
    assert!(value.get("previous_response_id").is_none());
    assert!(value.get("tools").is_some());
    assert!(value.get("tool_choice").is_some());
    assert!(value.get("parallel_tool_calls").is_some());
    assert!(value.get("reasoning").is_some());
    assert!(value.get("stream").is_some());
    assert!(value.get("store").is_some());
    assert!(value.get("include").is_some());
    assert!(value.get("prompt_cache_key").is_some());
    assert!(value.get("text").is_some());
    assert_eq!(
        value
            .get("service_tier")
            .and_then(serde_json::Value::as_str),
        Some("priority")
    );
    assert!(value.get("temperature").is_none());
    assert!(value.get("user").is_none());
}

/// 函数 `responses_defaults_tool_choice_and_reasoning_include_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_defaults_tool_choice_and_reasoning_include_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "reasoning": { "effort": "medium" }
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value.get("tool_choice").and_then(serde_json::Value::as_str),
        Some("auto")
    );
    assert_eq!(
        value
            .get("parallel_tool_calls")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert!(value
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .is_some());
    let include = value
        .get("include")
        .and_then(serde_json::Value::as_array)
        .expect("include array");
    assert!(include.iter().any(|item| {
        item.as_str()
            .map(|entry| entry == "reasoning.encrypted_content")
            .unwrap_or(false)
    }));
}

#[test]
fn responses_preserve_specific_function_tool_choice_object() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": [{ "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "hello" }] }],
        "tools": [{ "type": "function", "name": "mcp__browser__take_screenshot", "parameters": { "type": "object", "properties": {} } }],
        "tool_choice": {
            "type": "function",
            "name": "mcp__browser__take_screenshot"
        }
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(value["tool_choice"]["type"], "function");
    assert_eq!(
        value["tool_choice"]["name"],
        "mcp__browser__take_screenshot"
    );
}

/// 函数 `responses_defaults_empty_include_without_reasoning_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_defaults_empty_include_without_reasoning_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert!(value
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .is_some());
    assert!(value.get("include").is_none());
}

/// 函数 `responses_normalizes_fast_service_tier_to_priority_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_normalizes_fast_service_tier_to_priority_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "service_tier": "Fast"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("service_tier")
            .and_then(serde_json::Value::as_str),
        Some("priority")
    );
}

/// 函数 `responses_applies_fast_service_tier_override_as_priority_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_applies_fast_service_tier_override_as_priority_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello"
    });
    let out = apply_request_overrides_with_service_tier(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("fast"),
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("service_tier")
            .and_then(serde_json::Value::as_str),
        Some("priority")
    );
}

#[test]
fn responses_transparent_mode_still_maps_fast_service_tier_to_priority_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "transparent");
    let body = json!({
        "model": "gpt-5.4",
        "input": "hello",
        "service_tier": "Fast"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("service_tier")
            .and_then(serde_json::Value::as_str),
        Some("priority")
    );
}

/// 函数 `responses_ignores_unsupported_flex_service_tier_override_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_ignores_unsupported_flex_service_tier_override_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello"
    });
    let out = apply_request_overrides_with_service_tier(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("flex"),
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert!(value.get("service_tier").is_none());
}

/// 函数 `responses_compact_uses_codex_compat_rewrite`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_compact_uses_codex_compat_rewrite() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "tools": [{ "type": "function", "name": "ping", "parameters": { "type": "object", "properties": {} } }],
        "parallel_tool_calls": true,
        "reasoning": { "effort": "high" },
        "text": { "verbosity": "low" },
        "input": "compact me",
        "stream": false,
        "store": true,
        "service_tier": "priority",
        "user": "drop-me"
    });
    let out = apply_request_overrides(
        "/v1/responses/compact",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("instructions")
            .and_then(serde_json::Value::as_str),
        Some("")
    );
    assert!(value.get("tools").is_some());
    assert_eq!(
        value
            .get("parallel_tool_calls")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        value
            .get("reasoning")
            .and_then(|reasoning| reasoning.get("effort"))
            .and_then(serde_json::Value::as_str),
        Some("high")
    );
    assert!(value.get("text").is_some());
    assert!(value.get("stream").is_none());
    assert!(value.get("store").is_none());
    assert!(value.get("service_tier").is_none());
    assert!(value.get("user").is_none());
    assert!(value
        .get("input")
        .and_then(serde_json::Value::as_array)
        .is_some());
}

/// 函数 `responses_compact_defaults_parallel_tool_calls_to_false_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_compact_defaults_parallel_tool_calls_to_false_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "compact me"
    });
    let out = apply_request_overrides(
        "/v1/responses/compact",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value
            .get("parallel_tool_calls")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert!(value.get("include").is_none());
    assert!(value
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .is_some());
}

#[test]
fn responses_compact_transparent_mode_omits_service_tier_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "transparent");
    let body = json!({
        "model": "gpt-5.4",
        "input": "compact me",
        "service_tier": "fast"
    });
    let out = apply_request_overrides(
        "/v1/responses/compact",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert!(value.get("service_tier").is_none());
}

/// 函数 `responses_omits_include_when_reasoning_missing_for_codex_backend`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_omits_include_when_reasoning_missing_for_codex_backend() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert!(value.get("include").is_none());
}

/// 函数 `responses_keeps_parallel_tool_calls_missing_when_tools_are_present`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_keeps_parallel_tool_calls_missing_when_tools_are_present() {
    let _guard = crate::test_env_guard();
    let _mode_guard = RuntimeEnvGuard::set(GATEWAY_MODE_ENV, "enhanced");
    let body = json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "tools": [{ "type": "function", "name": "ping", "parameters": { "type": "object", "properties": {} } }]
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://chatgpt.com/backend-api/codex"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert!(value.get("parallel_tool_calls").is_none());
}

/// 函数 `responses_passthrough_for_non_codex_upstream`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn responses_passthrough_for_non_codex_upstream() {
    let _guard = crate::test_env_guard();
    let body = json!({
        "model": "gpt-4.1",
        "input": "hello",
        "stream": false,
        "store": true,
        "service_tier": "default",
        "user": "cherry-studio",
        "encrypted_content": "gAAA_test_payload",
        "unknown_field": "drop-me"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://api.openai.com/v1"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");
    assert_eq!(
        value.get("stream").and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        value.get("store").and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        value.get("input").and_then(serde_json::Value::as_str),
        Some("hello")
    );
    assert!(value.get("service_tier").is_some());
    assert!(value.get("user").is_some());
    assert!(value.get("encrypted_content").is_some());
    assert!(value.get("unknown_field").is_some());
}

#[test]
fn responses_apply_global_model_forward_rules_when_platform_key_not_bound() {
    let _guard = crate::test_env_guard();
    let original_rules = crate::gateway::current_model_forward_rules();
    crate::gateway::set_model_forward_rules("spark*=gpt-5.4-mini")
        .expect("set model forward rules");

    let body = json!({
        "model": "spark",
        "input": "hello"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        None,
        None,
        Some("https://api.openai.com/v1"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");

    assert_eq!(
        value.get("model").and_then(serde_json::Value::as_str),
        Some("spark")
    );

    let _ = crate::gateway::set_model_forward_rules(original_rules.as_str());
}

#[test]
fn responses_platform_key_bound_model_overrides_global_model_forward_rules() {
    let _guard = crate::test_env_guard();
    let original_rules = crate::gateway::current_model_forward_rules();
    crate::gateway::set_model_forward_rules("spark*=gpt-5.4-mini")
        .expect("set model forward rules");

    let body = json!({
        "model": "spark",
        "input": "hello"
    });
    let out = apply_request_overrides(
        "/v1/responses",
        serde_json::to_vec(&body).expect("serialize request body"),
        Some("gpt-5.4"),
        None,
        Some("https://api.openai.com/v1"),
    );
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse output body");

    assert_eq!(
        value.get("model").and_then(serde_json::Value::as_str),
        Some("gpt-5.4")
    );

    let _ = crate::gateway::set_model_forward_rules(original_rules.as_str());
}

/// 函数 `non_matching_endpoint_keeps_non_json_body`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn non_matching_endpoint_keeps_non_json_body() {
    let body = b"foo=1&bar=2".to_vec();
    let out = apply_request_overrides("/v1/non-standard", body.clone(), None, None, None);
    assert_eq!(out, body);
}
