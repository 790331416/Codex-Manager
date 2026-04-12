use super::*;
use crate::gateway::{
    adapt_request_for_protocol, apply_request_overrides_with_service_tier_and_prompt_cache_key,
};
use codexmanager_core::rpc::types::{ModelInfo, ModelsResponse};
use codexmanager_core::storage::{now_ts, Storage};
use serde_json::Value;

/// 函数 `sample_api_key`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - protocol_type: 参数 protocol_type
/// - model_slug: 参数 model_slug
/// - reasoning: 参数 reasoning
/// - service_tier: 参数 service_tier
///
/// # 返回
/// 返回函数执行结果
fn sample_api_key(
    protocol_type: &str,
    model_slug: Option<&str>,
    reasoning: Option<&str>,
    service_tier: Option<&str>,
) -> ApiKey {
    ApiKey {
        id: "gk_test".to_string(),
        name: Some("test".to_string()),
        model_slug: model_slug.map(|value| value.to_string()),
        reasoning_effort: reasoning.map(|value| value.to_string()),
        service_tier: service_tier.map(|value| value.to_string()),
        client_type: "codex".to_string(),
        protocol_type: protocol_type.to_string(),
        auth_scheme: "authorization_bearer".to_string(),
        upstream_base_url: None,
        static_headers_json: None,
        key_hash: "hash".to_string(),
        status: "active".to_string(),
        created_at: 0,
        last_used_at: None,
        rotation_strategy: crate::apikey_profile::ROTATION_ACCOUNT.to_string(),
        aggregate_api_id: None,
        aggregate_api_url: None,
        account_plan_filter: None,
    }
}

/// 函数 `anthropic_key_keeps_empty_overrides`
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
fn anthropic_key_keeps_empty_overrides() {
    let api_key = sample_api_key(
        crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE,
        None,
        None,
        None,
    );
    let (model, reasoning, service_tier) = resolve_effective_request_overrides(&api_key);
    assert_eq!(model, None);
    assert_eq!(reasoning, None);
    assert_eq!(service_tier, None);
}

/// 函数 `anthropic_key_applies_custom_model_and_reasoning`
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
fn anthropic_key_applies_custom_model_and_reasoning() {
    let api_key = sample_api_key(
        crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE,
        Some("gpt-5.3-codex"),
        Some("extra_high"),
        Some("fast"),
    );
    let (model, reasoning, service_tier) = resolve_effective_request_overrides(&api_key);
    assert_eq!(model.as_deref(), Some("gpt-5.3-codex"));
    assert_eq!(reasoning.as_deref(), Some("xhigh"));
    assert_eq!(service_tier.as_deref(), Some("fast"));
}

#[test]
fn anthropic_key_maps_fast_service_tier_to_priority_on_adapted_responses_request() {
    let api_key = sample_api_key(
        crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE,
        Some("gpt-5.3-codex"),
        Some("high"),
        Some("fast"),
    );
    let body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "messages": [{ "role": "user", "content": "hi" }],
        "stream": false
    });
    let body = serde_json::to_vec(&body).expect("serialize anthropic request");

    let adapted = adapt_request_for_protocol(
        crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE,
        "/v1/messages",
        body,
    )
    .expect("adapt anthropic request");
    let (model, reasoning, service_tier) = resolve_effective_request_overrides(&api_key);
    let rewritten = apply_request_overrides_with_service_tier_and_prompt_cache_key(
        adapted.path.as_str(),
        adapted.body,
        model.as_deref(),
        reasoning.as_deref(),
        service_tier.as_deref(),
        None,
        None,
    );
    let normalized = normalize_compat_service_tier_for_codex_backend(rewritten);
    let payload: Value = serde_json::from_slice(&normalized).expect("json body");

    assert_eq!(
        payload.get("service_tier").and_then(Value::as_str),
        Some("priority")
    );
}

#[test]
fn anthropic_key_ignores_unsupported_flex_service_tier_on_responses_request() {
    let api_key = sample_api_key(
        crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE,
        Some("gpt-5.3-codex"),
        Some("high"),
        Some("flex"),
    );
    let body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "messages": [{ "role": "user", "content": "hi" }],
        "stream": false
    });
    let body = serde_json::to_vec(&body).expect("serialize anthropic request");

    let adapted = adapt_request_for_protocol(
        crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE,
        "/v1/messages",
        body,
    )
    .expect("adapt anthropic request");
    let (model, reasoning, service_tier) = resolve_effective_request_overrides(&api_key);
    let rewritten = apply_request_overrides_with_service_tier_and_prompt_cache_key(
        adapted.path.as_str(),
        adapted.body,
        model.as_deref(),
        reasoning.as_deref(),
        service_tier.as_deref(),
        None,
        None,
    );
    let payload: Value = serde_json::from_slice(&rewritten).expect("json body");

    assert!(payload.get("service_tier").is_none());
}

/// 函数 `openai_key_keeps_empty_overrides`
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
fn openai_key_keeps_empty_overrides() {
    let api_key = sample_api_key("openai_compat", None, None, None);
    let (model, reasoning, service_tier) = resolve_effective_request_overrides(&api_key);
    assert_eq!(model, None);
    assert_eq!(reasoning, None);
    assert_eq!(service_tier, None);
}

/// 函数 `aggregate_passthrough_applies_model_reasoning_and_service_tier_overrides_without_forcing_log_tier`
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
fn aggregate_passthrough_applies_model_reasoning_and_service_tier_overrides_without_forcing_log_tier(
) {
    let api_key = sample_api_key(
        crate::apikey_profile::PROTOCOL_OPENAI_COMPAT,
        Some("gpt-5.4"),
        Some("high"),
        Some("fast"),
    );
    let body = br#"{"model":"gpt-4.1","input":"hi","reasoning":{"effort":"low"}}"#.to_vec();

    let (
        rewritten_body,
        model_for_log,
        reasoning_for_log,
        service_tier_for_log,
        effective_service_tier_for_log,
        _has_prompt_cache_key,
        _request_shape,
    ) = apply_passthrough_request_overrides("/v1/responses", body, &api_key, None);
    let payload: Value = serde_json::from_slice(&rewritten_body).expect("json body");

    assert_eq!(
        payload.get("model").and_then(Value::as_str),
        Some("gpt-5.4")
    );
    assert_eq!(
        payload
            .get("reasoning")
            .and_then(Value::as_object)
            .and_then(|reasoning| reasoning.get("effort"))
            .and_then(Value::as_str),
        Some("high")
    );
    assert_eq!(
        payload.get("service_tier").and_then(Value::as_str),
        Some("priority")
    );
    assert_eq!(model_for_log.as_deref(), Some("gpt-5.4"));
    assert_eq!(reasoning_for_log.as_deref(), Some("high"));
    assert_eq!(service_tier_for_log, None);
    assert_eq!(effective_service_tier_for_log.as_deref(), Some("fast"));
}

#[test]
fn aggregate_passthrough_preserves_fast_service_tier_for_log_when_request_is_rewritten() {
    let api_key = sample_api_key(
        crate::apikey_profile::PROTOCOL_OPENAI_COMPAT,
        Some("gpt-5.4"),
        Some("high"),
        None,
    );
    let body =
        br#"{"model":"gpt-4.1","input":"hi","reasoning":{"effort":"low"},"service_tier":"Fast"}"#
            .to_vec();

    let (
        rewritten_body,
        model_for_log,
        reasoning_for_log,
        service_tier_for_log,
        effective_service_tier_for_log,
        _has_prompt_cache_key,
        _request_shape,
    ) = apply_passthrough_request_overrides(
        "/v1/responses",
        body,
        &api_key,
        Some("fast".to_string()),
    );
    let payload: Value = serde_json::from_slice(&rewritten_body).expect("json body");

    assert_eq!(
        payload.get("service_tier").and_then(Value::as_str),
        Some("priority")
    );
    assert_eq!(model_for_log.as_deref(), Some("gpt-5.4"));
    assert_eq!(reasoning_for_log.as_deref(), Some("high"));
    assert_eq!(service_tier_for_log.as_deref(), Some("fast"));
    assert_eq!(effective_service_tier_for_log.as_deref(), Some("fast"));
}

#[test]
fn codex_backend_passthrough_maps_fast_to_priority_but_keeps_fast_for_log() {
    let mut api_key = sample_api_key(
        crate::apikey_profile::PROTOCOL_OPENAI_COMPAT,
        Some("gpt-5.4"),
        Some("high"),
        Some("fast"),
    );
    api_key.upstream_base_url = Some("https://chatgpt.com/backend-api/codex".to_string());
    let body = br#"{"model":"gpt-4.1","input":"hi","reasoning":{"effort":"low"}}"#.to_vec();

    let (
        rewritten_body,
        model_for_log,
        reasoning_for_log,
        service_tier_for_log,
        effective_service_tier_for_log,
        _has_prompt_cache_key,
        _request_shape,
    ) = apply_passthrough_request_overrides("/v1/responses", body, &api_key, None);
    let payload: Value = serde_json::from_slice(&rewritten_body).expect("json body");
    let request_meta = crate::gateway::parse_request_metadata(&rewritten_body);

    assert_eq!(
        payload.get("service_tier").and_then(Value::as_str),
        Some("priority")
    );
    assert_eq!(request_meta.service_tier.as_deref(), Some("fast"));
    assert_eq!(model_for_log.as_deref(), Some("gpt-5.4"));
    assert_eq!(reasoning_for_log.as_deref(), Some("high"));
    assert_eq!(service_tier_for_log, None);
    assert_eq!(effective_service_tier_for_log.as_deref(), Some("fast"));
}

/// 函数 `anthropic_model_must_exist_in_cached_model_options`
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
fn anthropic_model_must_exist_in_cached_model_options() {
    let storage = Storage::open_in_memory().expect("open storage");
    storage.init().expect("init storage");
    storage
        .upsert_model_options_cache(
            "default",
            &serde_json::to_string(&ModelsResponse {
                models: vec![
                    ModelInfo {
                        slug: "claude-sonnet-4".to_string(),
                        display_name: "claude-sonnet-4".to_string(),
                        ..Default::default()
                    },
                    ModelInfo {
                        slug: "gpt-5.4-mini".to_string(),
                        display_name: "gpt-5.4-mini".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            })
            .expect("serialize model options"),
            now_ts(),
        )
        .expect("save model cache");

    assert!(ensure_anthropic_model_is_listed(
        &storage,
        crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE,
        Some("claude-sonnet-4")
    )
    .is_ok());
    let err = ensure_anthropic_model_is_listed(
        &storage,
        crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE,
        Some("claude-sonnet-4-5"),
    )
    .expect_err("missing model should fail");
    assert!(err.message.contains("claude model not found in model list"));
}
