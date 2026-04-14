use crate::apikey_profile::{
    is_gemini_generate_content_request_path, resolve_gateway_protocol_type,
    PROTOCOL_ANTHROPIC_NATIVE, PROTOCOL_GEMINI_NATIVE, ROTATION_AGGREGATE_API,
};
use bytes::Bytes;
use codexmanager_core::storage::ApiKey;
use reqwest::Method;
use tiny_http::Request;

use super::{LocalValidationError, LocalValidationResult};

/// 函数 `resolve_effective_request_overrides`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - api_key: 参数 api_key
///
/// # 返回
/// 返回函数执行结果
fn resolve_effective_request_overrides(
    api_key: &ApiKey,
) -> (Option<String>, Option<String>, Option<String>) {
    let normalized_model = api_key
        .model_slug
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let normalized_reasoning = api_key
        .reasoning_effort
        .as_deref()
        .and_then(crate::reasoning_effort::normalize_reasoning_effort)
        .map(str::to_string);
    let normalized_service_tier = api_key
        .service_tier
        .as_deref()
        .and_then(crate::apikey::service_tier::normalize_service_tier)
        .map(str::to_string);

    (
        normalized_model,
        normalized_reasoning,
        normalized_service_tier,
    )
}

/// 函数 `ensure_anthropic_model_is_listed`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - storage: 参数 storage
/// - protocol_type: 参数 protocol_type
/// - model: 参数 model
///
/// # 返回
/// 返回函数执行结果
fn ensure_anthropic_model_is_listed(
    storage: &codexmanager_core::storage::Storage,
    protocol_type: &str,
    model: Option<&str>,
) -> Result<(), LocalValidationError> {
    if protocol_type != PROTOCOL_ANTHROPIC_NATIVE {
        return Ok(());
    }

    let Some(model) = model.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(LocalValidationError::new(
            400,
            crate::gateway::bilingual_error("Claude 模型必填", "claude model is required"),
        ));
    };

    let models = crate::apikey_models::read_model_options_from_storage(storage).map_err(|err| {
        LocalValidationError::new(
            500,
            crate::gateway::bilingual_error(
                "读取模型缓存失败",
                format!("model options cache read failed: {err}"),
            ),
        )
    })?;
    if models.is_empty() {
        return Err(LocalValidationError::new(
            400,
            crate::gateway::bilingual_error(
                "Claude 模型不在模型列表中",
                format!("claude model not found in model list: {model}"),
            ),
        ));
    }
    let found = models
        .models
        .iter()
        .any(|item| item.slug.trim().eq_ignore_ascii_case(model));
    if found {
        Ok(())
    } else {
        Err(LocalValidationError::new(
            400,
            crate::gateway::bilingual_error(
                "Claude 模型不在模型列表中",
                format!("claude model not found in model list: {model}"),
            ),
        ))
    }
}

/// 函数 `allow_openai_responses_path_rewrite`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - protocol_type: 参数 protocol_type
/// - normalized_path: 参数 normalized_path
///
/// # 返回
/// 返回函数执行结果
fn allow_compat_responses_path_rewrite(protocol_type: &str, normalized_path: &str) -> bool {
    (protocol_type == crate::apikey_profile::PROTOCOL_OPENAI_COMPAT
        && (normalized_path.starts_with("/v1/chat/completions")
            || normalized_path.starts_with("/v1/completions")))
        || (protocol_type == PROTOCOL_GEMINI_NATIVE
            && is_gemini_generate_content_request_path(normalized_path))
}

/// 函数 `should_derive_compat_conversation_anchor`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - protocol_type: 参数 protocol_type
/// - normalized_path: 参数 normalized_path
///
/// # 返回
/// 返回函数执行结果
fn should_derive_compat_conversation_anchor(protocol_type: &str, normalized_path: &str) -> bool {
    (protocol_type == PROTOCOL_ANTHROPIC_NATIVE && normalized_path.starts_with("/v1/messages"))
        || allow_compat_responses_path_rewrite(protocol_type, normalized_path)
}

fn should_normalize_compat_service_tier_for_codex_backend(
    protocol_type: &str,
    normalized_path: &str,
    adapted_path: &str,
) -> bool {
    adapted_path.starts_with("/v1/responses")
        && ((protocol_type == PROTOCOL_ANTHROPIC_NATIVE
            && normalized_path.starts_with("/v1/messages"))
            || allow_compat_responses_path_rewrite(protocol_type, normalized_path))
}

fn normalize_compat_service_tier_for_codex_backend(body: Vec<u8>) -> Vec<u8> {
    let Ok(mut payload) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return body;
    };
    let Some(obj) = payload.as_object_mut() else {
        return body;
    };
    let Some(service_tier) = obj.get_mut("service_tier") else {
        return body;
    };
    let Some(raw_value) = service_tier.as_str() else {
        return body;
    };

    if raw_value.eq_ignore_ascii_case("fast") || raw_value.eq_ignore_ascii_case("priority") {
        *service_tier = serde_json::Value::String("priority".to_string());
    } else {
        obj.remove("service_tier");
    }

    serde_json::to_vec(&payload).unwrap_or(body)
}

/// 函数 `resolve_local_conversation_id`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - protocol_type: 参数 protocol_type
/// - normalized_path: 参数 normalized_path
/// - incoming_headers: 参数 incoming_headers
/// - client_has_prompt_cache_key: 参数 client_has_prompt_cache_key
///
/// # 返回
/// 返回函数执行结果
fn resolve_local_conversation_id(
    protocol_type: &str,
    normalized_path: &str,
    incoming_headers: &super::super::IncomingHeaderSnapshot,
    client_has_prompt_cache_key: bool,
) -> Option<String> {
    incoming_headers
        .conversation_id()
        .map(str::to_string)
        .or_else(|| {
            if client_has_prompt_cache_key
                || !should_derive_compat_conversation_anchor(protocol_type, normalized_path)
            {
                return None;
            }
            // 中文注释：Claude / chat.completions 兼容请求通常不会自带稳定线程锚点；
            // 这里退化到平台密钥派生出的 sticky conversation，确保 prompt cache key 跨轮次稳定。
            super::super::upstream::header_profile::derive_sticky_conversation_id_from_headers(
                incoming_headers,
            )
        })
}

/// 函数 `apply_passthrough_request_overrides`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - path: 参数 path
/// - body: 参数 body
/// - api_key: 参数 api_key
///
/// # 返回
/// 返回函数执行结果
fn apply_passthrough_request_overrides(
    path: &str,
    body: Vec<u8>,
    api_key: &ApiKey,
    explicit_service_tier_for_log: Option<String>,
) -> (
    Vec<u8>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
    Option<String>,
) {
    let (effective_model, effective_reasoning, effective_service_tier) =
        resolve_effective_request_overrides(api_key);
    let rewritten_body =
        super::super::apply_request_overrides_with_service_tier_and_prompt_cache_key(
            path,
            body,
            effective_model.as_deref(),
            effective_reasoning.as_deref(),
            effective_service_tier.as_deref(),
            api_key.upstream_base_url.as_deref(),
            None,
        );
    let request_meta = super::super::parse_request_metadata(&rewritten_body);
    (
        rewritten_body,
        request_meta.model.or(api_key.model_slug.clone()),
        request_meta
            .reasoning_effort
            .or(api_key.reasoning_effort.clone()),
        explicit_service_tier_for_log,
        request_meta.service_tier,
        request_meta.has_prompt_cache_key,
        request_meta.request_shape,
    )
}

/// 函数 `build_local_validation_result`
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
pub(super) fn build_local_validation_result(
    request: &Request,
    trace_id: String,
    incoming_headers: super::super::IncomingHeaderSnapshot,
    storage: crate::storage_helpers::StorageHandle,
    mut body: Vec<u8>,
    api_key: ApiKey,
) -> Result<LocalValidationResult, LocalValidationError> {
    // 按当前策略取消每次请求都更新 api_keys.last_used_at，减少并发写入冲突。
    let normalized_path = super::super::normalize_models_path(request.url());
    let effective_protocol_type =
        resolve_gateway_protocol_type(api_key.protocol_type.as_str(), normalized_path.as_str());
    let request_method = request.method().as_str().to_string();
    let method = Method::from_bytes(request_method.as_bytes())
        .map_err(|_| {
            LocalValidationError::new(
                405,
                crate::gateway::bilingual_error("不支持的请求方法", "unsupported method"),
            )
        })?;
    let initial_service_tier_diagnostic = super::super::inspect_service_tier_for_log(&body);
    super::super::log_client_service_tier(
        trace_id.as_str(),
        "http",
        normalized_path.as_str(),
        initial_service_tier_diagnostic.has_field,
        initial_service_tier_diagnostic.raw_value.as_deref(),
        initial_service_tier_diagnostic.normalized_value.as_deref(),
    );
    let initial_request_meta = super::super::parse_request_metadata(&body);
    let initial_local_conversation_id = resolve_local_conversation_id(
        effective_protocol_type,
        normalized_path.as_str(),
        &incoming_headers,
        initial_request_meta.has_prompt_cache_key,
    );

    if api_key.rotation_strategy == ROTATION_AGGREGATE_API {
        let (
            rewritten_body,
            model_for_log,
            reasoning_for_log,
            service_tier_for_log,
            effective_service_tier_for_log,
            has_prompt_cache_key,
            request_shape,
        ) = apply_passthrough_request_overrides(
            &normalized_path,
            body,
            &api_key,
            initial_request_meta.service_tier.clone(),
        );
        let incoming_headers = incoming_headers
            .with_conversation_id_override(initial_local_conversation_id.as_deref());
        return Ok(LocalValidationResult {
            trace_id,
            incoming_headers,
            storage,
            original_path: normalized_path.clone(),
            path: normalized_path,
            body: Bytes::from(rewritten_body),
            is_stream: initial_request_meta.is_stream,
            has_prompt_cache_key,
            request_shape,
            protocol_type: effective_protocol_type.to_string(),
            rotation_strategy: api_key.rotation_strategy,
            aggregate_api_id: api_key.aggregate_api_id,
            account_plan_filter: api_key.account_plan_filter,
            upstream_base_url: api_key.upstream_base_url,
            static_headers_json: api_key.static_headers_json,
            response_adapter: super::super::ResponseAdapter::Passthrough,
            gemini_stream_output_mode: None,
            tool_name_restore_map: super::super::ToolNameRestoreMap::default(),
            request_method,
            key_id: api_key.id,
            platform_key_hash: api_key.key_hash,
            local_conversation_id: initial_local_conversation_id,
            conversation_binding: None,
            model_for_log,
            reasoning_for_log,
            service_tier_for_log,
            effective_service_tier_for_log,
            method,
        });
    }

    let original_body = body.clone();
    let adapted =
        super::super::adapt_request_for_protocol(effective_protocol_type, &normalized_path, body)
            .map_err(|err| {
                LocalValidationError::new(
                    400,
                    crate::gateway::bilingual_error("请求协议适配失败", err),
                )
            })?;
    let mut path = adapted.path;
    let mut response_adapter = adapted.response_adapter;
    let mut gemini_stream_output_mode = adapted.gemini_stream_output_mode;
    let mut tool_name_restore_map = adapted.tool_name_restore_map;
    body = adapted.body;
    if effective_protocol_type != PROTOCOL_ANTHROPIC_NATIVE
        && !normalized_path.starts_with("/v1/responses")
        && path.starts_with("/v1/responses")
        && !allow_compat_responses_path_rewrite(effective_protocol_type, &normalized_path)
    {
        // 中文注释：防回归保护：仅已登记的兼容协议路径允许改写到 /v1/responses；
        // 其余协议和路径一律保持原路径透传，避免客户端按原生协议却拿到错误的流格式。
        log::warn!(
            "event=gateway_protocol_adapt_guard protocol_type={} from_path={} to_path={} action=force_passthrough",
            effective_protocol_type,
            normalized_path,
            path
        );
        path = normalized_path.clone();
        body = original_body;
        response_adapter = super::super::ResponseAdapter::Passthrough;
        gemini_stream_output_mode = None;
        tool_name_restore_map.clear();
    }
    // 中文注释：下游调用方的 stream 语义应在请求改写前确定；
    // 否则上游兼容改写（例如 /responses 强制 stream=true）会污染下游响应模式判断。
    let client_request_meta = super::super::parse_request_metadata(&body);
    let (effective_model, effective_reasoning, effective_service_tier) =
        resolve_effective_request_overrides(&api_key);
    let local_conversation_id = initial_local_conversation_id.clone();
    let conversation_binding = super::super::conversation_binding::load_conversation_binding(
        &storage,
        api_key.key_hash.as_str(),
        local_conversation_id.as_deref(),
    )
    .map_err(|err| LocalValidationError::new(500, err))?;
    let effective_thread_anchor = super::super::conversation_binding::effective_thread_anchor(
        local_conversation_id.as_deref(),
        conversation_binding.as_ref(),
    );
    // 中文注释：保留原始 local conversation_id 作为对外会话标识；
    // 线程世代只参与 prompt_cache_key 与路由绑定，不直接污染对外请求头。
    let incoming_headers =
        incoming_headers.with_conversation_id_override(local_conversation_id.as_deref());
    let should_normalize_compat_service_tier =
        should_normalize_compat_service_tier_for_codex_backend(
            effective_protocol_type,
            normalized_path.as_str(),
            path.as_str(),
        );
    body = if effective_thread_anchor.is_some() {
        super::super::apply_request_overrides_with_service_tier_and_forced_prompt_cache_key(
            &path,
            body,
            effective_model.as_deref(),
            effective_reasoning.as_deref(),
            effective_service_tier.as_deref(),
            api_key.upstream_base_url.as_deref(),
            effective_thread_anchor.as_deref(),
        )
    } else {
        super::super::apply_request_overrides_with_service_tier_and_prompt_cache_key(
            &path,
            body,
            effective_model.as_deref(),
            effective_reasoning.as_deref(),
            effective_service_tier.as_deref(),
            api_key.upstream_base_url.as_deref(),
            None,
        )
    };
    if should_normalize_compat_service_tier {
        body = normalize_compat_service_tier_for_codex_backend(body);
    }

    let request_meta = super::super::parse_request_metadata(&body);
    let model_for_log = request_meta.model.or(api_key.model_slug.clone());
    let reasoning_for_log = request_meta
        .reasoning_effort
        .or(api_key.reasoning_effort.clone());
    let service_tier_for_log = client_request_meta.service_tier;
    let effective_service_tier_for_log = request_meta.service_tier;
    let is_stream = client_request_meta.is_stream;
    let has_prompt_cache_key = client_request_meta.has_prompt_cache_key;
    let request_shape = client_request_meta.request_shape;

    ensure_anthropic_model_is_listed(&storage, effective_protocol_type, model_for_log.as_deref())?;

    Ok(LocalValidationResult {
        trace_id,
        incoming_headers,
        storage,
        original_path: normalized_path,
        path,
        body: Bytes::from(body),
        is_stream,
        has_prompt_cache_key,
        request_shape,
        protocol_type: effective_protocol_type.to_string(),
        upstream_base_url: api_key.upstream_base_url,
        static_headers_json: api_key.static_headers_json,
        response_adapter,
        gemini_stream_output_mode,
        tool_name_restore_map,
        request_method,
        key_id: api_key.id,
        platform_key_hash: api_key.key_hash,
        local_conversation_id,
        conversation_binding,
        rotation_strategy: api_key.rotation_strategy,
        aggregate_api_id: api_key.aggregate_api_id,
        account_plan_filter: api_key.account_plan_filter,
        model_for_log,
        reasoning_for_log,
        service_tier_for_log,
        effective_service_tier_for_log,
        method,
    })
}

#[cfg(test)]
#[path = "tests/request_tests.rs"]
mod tests;
