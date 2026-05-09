use crate::apikey_profile::PROTOCOL_ANTHROPIC_NATIVE;
use crate::gateway::request_log::RequestLogUsage;
use std::time::Instant;
use tiny_http::Request;

use super::super::local_validation::LocalValidationResult;
use super::executor::{
    resolve_gateway_upstream_execution_plan, GatewayUpstreamExecutorKind, GatewayUpstreamRouteKind,
};
use super::proxy_pipeline::candidate_executor::{
    execute_candidate_sequence, CandidateExecutionResult, CandidateExecutorParams,
};
use super::proxy_pipeline::execution_context::GatewayUpstreamExecutionContext;
use super::proxy_pipeline::request_gate::acquire_request_gate;
use super::proxy_pipeline::request_setup::prepare_request_setup;
use super::proxy_pipeline::response_finalize::respond_terminal;
use super::support::precheck::{prepare_candidates_for_proxy, CandidatePrecheckResult};

/// 函数 `exhausted_gateway_error_for_log`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - attempted_account_ids: 参数 attempted_account_ids
/// - skipped_cooldown: 参数 skipped_cooldown
/// - skipped_inflight: 参数 skipped_inflight
/// - last_attempt_error: 参数 last_attempt_error
///
/// # 返回
/// 返回函数执行结果
fn exhausted_gateway_error_for_log(
    attempted_account_ids: &[String],
    skipped_cooldown: usize,
    skipped_inflight: usize,
    last_attempt_error: Option<&str>,
) -> String {
    let kind = if !attempted_account_ids.is_empty() {
        "no_available_account_exhausted"
    } else if skipped_cooldown > 0 && skipped_inflight > 0 {
        "no_available_account_skipped"
    } else if skipped_cooldown > 0 {
        "no_available_account_cooldown"
    } else if skipped_inflight > 0 {
        "no_available_account_inflight"
    } else {
        "no_available_account"
    };
    let mut parts = vec![
        crate::gateway::bilingual_error("无可用账号", "no available account"),
        format!("kind={kind}"),
    ];
    if !attempted_account_ids.is_empty() {
        parts.push(format!("attempted={}", attempted_account_ids.join(",")));
    }
    if skipped_cooldown > 0 || skipped_inflight > 0 {
        parts.push(format!(
            "skipped(cooldown={}, inflight={})",
            skipped_cooldown, skipped_inflight
        ));
    }
    if let Some(last_attempt_error) = last_attempt_error
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        parts.push(format!("last_attempt={last_attempt_error}"));
    }
    parts.join("; ")
}

fn resolve_upstream_is_stream(client_is_stream: bool, path: &str) -> bool {
    let is_compact_path =
        path == "/v1/responses/compact" || path.starts_with("/v1/responses/compact?");
    client_is_stream || (path.starts_with("/v1/responses") && !is_compact_path)
}

fn should_try_provider_executor_aggregate_route(
    execution_plan: super::executor::GatewayUpstreamExecutionPlan,
) -> bool {
    matches!(
        execution_plan.route_kind,
        GatewayUpstreamRouteKind::AggregateApi
    )
}

fn is_hybrid_account_first_route(
    execution_plan: super::executor::GatewayUpstreamExecutionPlan,
) -> bool {
    matches!(
        execution_plan.route_kind,
        GatewayUpstreamRouteKind::HybridAccountFirst
    )
}

fn respond_when_account_candidates_empty(
    execution_plan: super::executor::GatewayUpstreamExecutionPlan,
) -> bool {
    !is_hybrid_account_first_route(execution_plan)
}

fn should_fallback_to_aggregate_after_account_exhaustion(
    execution_plan: super::executor::GatewayUpstreamExecutionPlan,
) -> bool {
    is_hybrid_account_first_route(execution_plan)
}

fn executor_kind_label(value: GatewayUpstreamExecutorKind) -> &'static str {
    match value {
        GatewayUpstreamExecutorKind::CodexResponses => "codex_responses",
        GatewayUpstreamExecutorKind::Claude => "claude",
        GatewayUpstreamExecutorKind::Gemini => "gemini",
    }
}

fn route_kind_label(value: GatewayUpstreamRouteKind) -> &'static str {
    match value {
        GatewayUpstreamRouteKind::AccountRotation => "account_rotation",
        GatewayUpstreamRouteKind::AggregateApi => "aggregate_api",
        GatewayUpstreamRouteKind::HybridAccountFirst => "hybrid_account_first",
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_aggregate_candidates_for_route(
    storage: &crate::storage_helpers::StorageHandle,
    protocol_type: &str,
    aggregate_api_id: Option<&str>,
) -> Result<Vec<codexmanager_core::storage::AggregateApi>, String> {
    super::protocol::aggregate_api::resolve_aggregate_api_rotation_candidates(
        storage,
        protocol_type,
        aggregate_api_id,
    )
}

fn hybrid_route_error_message(account_error: Option<&str>, aggregate_error: &str) -> String {
    match account_error.map(str::trim).filter(|value| !value.is_empty()) {
        Some(account_error) => crate::gateway::bilingual_error(
            format!("账号池与聚合 API 均不可用：{account_error}；聚合 API：{aggregate_error}"),
            format!("account pool and aggregate api are unavailable: {account_error}; aggregate api: {aggregate_error}"),
        ),
        None => crate::gateway::bilingual_error(
            format!("账号池与聚合 API 均不可用；聚合 API：{aggregate_error}"),
            format!("account pool and aggregate api are unavailable; aggregate api: {aggregate_error}"),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn respond_hybrid_route_error(
    request: Request,
    storage: &crate::storage_helpers::StorageHandle,
    trace_id: &str,
    key_id: &str,
    original_path: &str,
    path: &str,
    request_method: &str,
    response_adapter: super::super::ResponseAdapter,
    effective_service_tier_for_log: Option<&str>,
    model_for_log: Option<&str>,
    reasoning_for_log: Option<&str>,
    started_at: Instant,
    account_error: Option<&str>,
    aggregate_error: String,
) -> Result<(), String> {
    let message = hybrid_route_error_message(account_error, aggregate_error.as_str());
    respond_aggregate_route_error(
        request,
        storage,
        trace_id,
        key_id,
        original_path,
        path,
        request_method,
        response_adapter,
        effective_service_tier_for_log,
        model_for_log,
        reasoning_for_log,
        started_at,
        message,
    )
}

#[cfg(test)]
fn provider_upstream_hint(
    value: GatewayUpstreamExecutorKind,
) -> Option<(&'static str, &'static str)> {
    match value {
        GatewayUpstreamExecutorKind::Claude => Some(("Claude", "claude")),
        GatewayUpstreamExecutorKind::Gemini => Some(("Gemini", "gemini")),
        GatewayUpstreamExecutorKind::CodexResponses => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn respond_aggregate_route_error(
    request: Request,
    storage: &crate::storage_helpers::StorageHandle,
    trace_id: &str,
    key_id: &str,
    original_path: &str,
    path: &str,
    request_method: &str,
    response_adapter: super::super::ResponseAdapter,
    effective_service_tier_for_log: Option<&str>,
    model_for_log: Option<&str>,
    reasoning_for_log: Option<&str>,
    started_at: Instant,
    message: String,
) -> Result<(), String> {
    super::super::record_gateway_request_outcome(path, 404, Some("aggregate_api"));
    super::super::trace_log::log_request_final(
        trace_id,
        404,
        Some(key_id),
        None,
        Some(message.as_str()),
        started_at.elapsed().as_millis(),
    );
    super::super::write_request_log(
        storage,
        super::super::request_log::RequestLogTraceContext {
            trace_id: Some(trace_id),
            original_path: Some(original_path),
            adapted_path: Some(path),
            response_adapter: Some(response_adapter),
            effective_service_tier: effective_service_tier_for_log,
            ..Default::default()
        },
        Some(key_id),
        None,
        path,
        request_method,
        model_for_log,
        reasoning_for_log,
        None,
        Some(404),
        super::super::request_log::RequestLogUsage::default(),
        Some(message.as_str()),
        Some(started_at.elapsed().as_millis()),
    );
    let response = super::super::error_response::terminal_text_response(
        404,
        super::super::error_message_for_client(
            super::super::prefers_raw_errors_for_tiny_http_request(&request),
            message,
        ),
        Some(trace_id),
    );
    let _ = request.respond(response);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn proxy_with_aggregate_candidates(
    request: Request,
    storage: &crate::storage_helpers::StorageHandle,
    trace_id: &str,
    key_id: &str,
    original_path: &str,
    path: &str,
    request_method: &str,
    method: &reqwest::Method,
    body: &bytes::Bytes,
    client_is_stream: bool,
    model_for_log: Option<&str>,
    reasoning_for_log: Option<&str>,
    effective_service_tier_for_log: Option<&str>,
    aggregate_api_id: Option<&str>,
    request_deadline: Option<Instant>,
    started_at: Instant,
    aggregate_api_candidates: Vec<codexmanager_core::storage::AggregateApi>,
) -> Result<(), String> {
    let mut aggregate_api_candidates = aggregate_api_candidates;
    super::protocol::aggregate_api::apply_gateway_route_strategy_to_aggregate_candidates(
        &mut aggregate_api_candidates,
        key_id,
        model_for_log,
        aggregate_api_id,
    );

    super::protocol::aggregate_api::proxy_aggregate_request(
        super::protocol::aggregate_api::AggregateProxyRequest {
            request,
            storage,
            trace_id,
            key_id,
            original_path,
            path,
            request_method,
            method,
            body,
            is_stream: client_is_stream,
            response_adapter: super::super::ResponseAdapter::Passthrough,
            model_for_log,
            reasoning_for_log,
            effective_service_tier_for_log,
            aggregate_api_candidates,
            request_deadline,
            started_at,
        },
    )
}

/// 函数 `proxy_validated_request`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - in super: 参数 in super
///
/// # 返回
/// 返回函数执行结果
pub(in super::super) fn proxy_validated_request(
    request: Request,
    validated: LocalValidationResult,
    debug: bool,
) -> Result<(), String> {
    let LocalValidationResult {
        trace_id,
        incoming_headers,
        storage,
        original_path,
        passthrough_path,
        path,
        passthrough_body,
        body,
        is_stream,
        has_prompt_cache_key,
        request_shape,
        protocol_type,
        rotation_strategy,
        aggregate_api_id,
        account_plan_filter,
        response_adapter,
        gemini_stream_output_mode,
        tool_name_restore_map,
        request_method,
        key_id,
        platform_key_hash,
        local_conversation_id,
        conversation_binding,
        model_for_log,
        reasoning_for_log,
        service_tier_for_log,
        effective_service_tier_for_log,
        method,
    } = validated;
    let started_at = Instant::now();
    let client_is_stream = is_stream;
    // 中文注释：对齐 Codex 上游协议：/v1/responses 固定走 SSE。
    // 下游是否流式仍由客户端 `stream` 参数决定（在 response bridge 层聚合/透传）。
    let upstream_is_stream = resolve_upstream_is_stream(client_is_stream, path.as_str());
    let request_deadline = super::support::deadline::request_deadline(started_at, client_is_stream);

    super::super::trace_log::log_request_start(
        trace_id.as_str(),
        key_id.as_str(),
        request_method.as_str(),
        path.as_str(),
        model_for_log.as_deref(),
        reasoning_for_log.as_deref(),
        service_tier_for_log.as_deref(),
        client_is_stream,
        "http",
        protocol_type.as_str(),
    );
    super::super::trace_log::log_request_body_preview(trace_id.as_str(), body.as_ref());
    if protocol_type == crate::apikey_profile::PROTOCOL_GEMINI_NATIVE {
        super::super::trace_log::log_gemini_request_diagnostics(
            trace_id.as_str(),
            original_path.as_str(),
            path.as_str(),
            format!("{response_adapter:?}").as_str(),
            gemini_stream_output_mode.map(|mode| match mode {
                super::super::GeminiStreamOutputMode::Sse => "sse",
                super::super::GeminiStreamOutputMode::Raw => "raw",
            }),
            body.as_ref(),
        );
    }

    let execution_plan =
        resolve_gateway_upstream_execution_plan(protocol_type.as_str(), rotation_strategy.as_str());
    super::super::log_request_execution_plan(
        trace_id.as_str(),
        path.as_str(),
        protocol_type.as_str(),
        executor_kind_label(execution_plan.executor_kind),
        route_kind_label(execution_plan.route_kind),
    );

    if should_try_provider_executor_aggregate_route(execution_plan) {
        match resolve_aggregate_candidates_for_route(
            &storage,
            protocol_type.as_str(),
            aggregate_api_id.as_deref(),
        ) {
            Ok(aggregate_api_candidates) => {
                return proxy_with_aggregate_candidates(
                    request,
                    &storage,
                    trace_id.as_str(),
                    key_id.as_str(),
                    original_path.as_str(),
                    path.as_str(),
                    request_method.as_str(),
                    &method,
                    &body,
                    client_is_stream,
                    model_for_log.as_deref(),
                    reasoning_for_log.as_deref(),
                    effective_service_tier_for_log.as_deref(),
                    aggregate_api_id.as_deref(),
                    request_deadline,
                    started_at,
                    aggregate_api_candidates,
                );
            }
            Err(err) => {
                return respond_aggregate_route_error(
                    request,
                    &storage,
                    trace_id.as_str(),
                    key_id.as_str(),
                    original_path.as_str(),
                    path.as_str(),
                    request_method.as_str(),
                    super::super::ResponseAdapter::Passthrough,
                    effective_service_tier_for_log.as_deref(),
                    model_for_log.as_deref(),
                    reasoning_for_log.as_deref(),
                    started_at,
                    err,
                );
            }
        }
    }

    let (request, mut candidates) = match prepare_candidates_for_proxy(
        request,
        &storage,
        trace_id.as_str(),
        &key_id,
        &original_path,
        &path,
        response_adapter,
        &request_method,
        model_for_log.as_deref(),
        reasoning_for_log.as_deref(),
        account_plan_filter.as_deref(),
        respond_when_account_candidates_empty(execution_plan),
    ) {
        CandidatePrecheckResult::Ready {
            request,
            candidates,
        } => (request, candidates),
        CandidatePrecheckResult::Empty { request } => {
            match resolve_aggregate_candidates_for_route(
                &storage,
                protocol_type.as_str(),
                aggregate_api_id.as_deref(),
            ) {
                Ok(aggregate_api_candidates) => {
                    return proxy_with_aggregate_candidates(
                        request,
                        &storage,
                        trace_id.as_str(),
                        key_id.as_str(),
                        original_path.as_str(),
                        passthrough_path.as_str(),
                        request_method.as_str(),
                        &method,
                        &passthrough_body,
                        client_is_stream,
                        model_for_log.as_deref(),
                        reasoning_for_log.as_deref(),
                        effective_service_tier_for_log.as_deref(),
                        aggregate_api_id.as_deref(),
                        request_deadline,
                        started_at,
                        aggregate_api_candidates,
                    );
                }
                Err(err) => {
                    return respond_hybrid_route_error(
                        request,
                        &storage,
                        trace_id.as_str(),
                        key_id.as_str(),
                        original_path.as_str(),
                        passthrough_path.as_str(),
                        request_method.as_str(),
                        super::super::ResponseAdapter::Passthrough,
                        effective_service_tier_for_log.as_deref(),
                        model_for_log.as_deref(),
                        reasoning_for_log.as_deref(),
                        started_at,
                        Some("无可用账号(no available account)"),
                        err,
                    );
                }
            }
        }
        CandidatePrecheckResult::Responded => return Ok(()),
    };
    let setup = prepare_request_setup(
        path.as_str(),
        protocol_type.as_str(),
        has_prompt_cache_key,
        &incoming_headers,
        &body,
        &mut candidates,
        key_id.as_str(),
        platform_key_hash.as_str(),
        local_conversation_id.as_deref(),
        conversation_binding.as_ref(),
        model_for_log.as_deref(),
        trace_id.as_str(),
    );
    let base = setup.upstream_base.as_str();

    let context = GatewayUpstreamExecutionContext::new(
        &trace_id,
        &storage,
        &key_id,
        &original_path,
        &path,
        &request_method,
        response_adapter,
        protocol_type.as_str(),
        model_for_log.as_deref(),
        reasoning_for_log.as_deref(),
        service_tier_for_log.as_deref(),
        effective_service_tier_for_log.as_deref(),
        setup.candidate_count,
        setup.account_max_inflight,
    );
    let allow_openai_fallback = setup.upstream_fallback_base.is_some();
    let disable_challenge_stateless_retry = !(protocol_type == PROTOCOL_ANTHROPIC_NATIVE
        && body.len() <= 2 * 1024)
        && !path.starts_with("/v1/responses");
    let _request_gate_guard = acquire_request_gate(
        trace_id.as_str(),
        key_id.as_str(),
        path.as_str(),
        model_for_log.as_deref(),
        request_deadline,
    );
    let exhausted = match execute_candidate_sequence(
        request,
        candidates,
        CandidateExecutorParams {
            storage: &storage,
            method: &method,
            incoming_headers: &incoming_headers,
            body: &body,
            path: path.as_str(),
            request_shape: request_shape.as_deref(),
            trace_id: trace_id.as_str(),
            model_for_log: model_for_log.as_deref(),
            response_adapter,
            gemini_stream_output_mode,
            tool_name_restore_map: &tool_name_restore_map,
            context: &context,
            setup: &setup,
            request_deadline,
            started_at,
            client_is_stream,
            upstream_is_stream,
            debug,
            allow_openai_fallback,
            disable_challenge_stateless_retry,
        },
    )? {
        CandidateExecutionResult::Handled => return Ok(()),
        CandidateExecutionResult::Exhausted {
            request,
            attempted_account_ids,
            skipped_cooldown,
            skipped_inflight,
            last_attempt_url,
            last_attempt_error,
        } => (
            request,
            attempted_account_ids,
            skipped_cooldown,
            skipped_inflight,
            last_attempt_url,
            last_attempt_error,
        ),
    };
    let (
        request,
        attempted_account_ids,
        skipped_cooldown,
        skipped_inflight,
        last_attempt_url,
        last_attempt_error,
    ) = exhausted;
    let final_error = exhausted_gateway_error_for_log(
        attempted_account_ids.as_slice(),
        skipped_cooldown,
        skipped_inflight,
        last_attempt_error.as_deref(),
    );
    if should_fallback_to_aggregate_after_account_exhaustion(execution_plan) {
        match resolve_aggregate_candidates_for_route(
            &storage,
            protocol_type.as_str(),
            aggregate_api_id.as_deref(),
        ) {
            Ok(aggregate_api_candidates) => {
                return proxy_with_aggregate_candidates(
                    request,
                    &storage,
                    trace_id.as_str(),
                    key_id.as_str(),
                    original_path.as_str(),
                    passthrough_path.as_str(),
                    request_method.as_str(),
                    &method,
                    &passthrough_body,
                    client_is_stream,
                    model_for_log.as_deref(),
                    reasoning_for_log.as_deref(),
                    effective_service_tier_for_log.as_deref(),
                    aggregate_api_id.as_deref(),
                    request_deadline,
                    started_at,
                    aggregate_api_candidates,
                );
            }
            Err(err) => {
                return respond_hybrid_route_error(
                    request,
                    &storage,
                    trace_id.as_str(),
                    key_id.as_str(),
                    original_path.as_str(),
                    passthrough_path.as_str(),
                    request_method.as_str(),
                    super::super::ResponseAdapter::Passthrough,
                    effective_service_tier_for_log.as_deref(),
                    model_for_log.as_deref(),
                    reasoning_for_log.as_deref(),
                    started_at,
                    Some(final_error.as_str()),
                    err,
                );
            }
        }
    }

    context.log_final_result(
        None,
        last_attempt_url.as_deref().or(Some(base)),
        503,
        RequestLogUsage::default(),
        Some(final_error.as_str()),
        started_at.elapsed().as_millis(),
        (!attempted_account_ids.is_empty()).then_some(attempted_account_ids.as_slice()),
    );
    respond_terminal(
        request,
        503,
        crate::gateway::bilingual_error("无可用账号", "no available account"),
        Some(trace_id.as_str()),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        exhausted_gateway_error_for_log, hybrid_route_error_message, provider_upstream_hint,
        resolve_upstream_is_stream, respond_when_account_candidates_empty,
        should_fallback_to_aggregate_after_account_exhaustion,
        should_try_provider_executor_aggregate_route,
    };
    use crate::gateway::upstream::executor::{
        GatewayUpstreamExecutionPlan, GatewayUpstreamExecutorKind, GatewayUpstreamRouteKind,
    };

    /// 函数 `exhausted_gateway_error_includes_attempts_skips_and_last_error`
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
    fn exhausted_gateway_error_includes_attempts_skips_and_last_error() {
        let message = exhausted_gateway_error_for_log(
            &["acc-a".to_string(), "acc-b".to_string()],
            2,
            1,
            Some("upstream challenge blocked"),
        );

        assert!(message.contains("no available account"));
        assert!(message.contains("kind=no_available_account_exhausted"));
        assert!(message.contains("attempted=acc-a,acc-b"));
        assert!(message.contains("skipped(cooldown=2, inflight=1)"));
        assert!(message.contains("last_attempt=upstream challenge blocked"));
    }

    /// 函数 `exhausted_gateway_error_marks_cooldown_only_skip_kind`
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
    fn exhausted_gateway_error_marks_cooldown_only_skip_kind() {
        let message = exhausted_gateway_error_for_log(&[], 2, 0, None);

        assert!(message.contains("kind=no_available_account_cooldown"));
    }

    #[test]
    fn resolve_upstream_is_stream_keeps_non_compact_responses_on_sse_upstream() {
        assert!(resolve_upstream_is_stream(false, "/v1/responses"));
        assert!(resolve_upstream_is_stream(
            false,
            "/v1/responses?stream=false"
        ));
        assert!(!resolve_upstream_is_stream(false, "/v1/responses/compact"));
        assert!(!resolve_upstream_is_stream(false, "/v1/chat/completions"));
        assert!(resolve_upstream_is_stream(true, "/v1/chat/completions"));
    }

    #[test]
    fn only_explicit_aggregate_route_uses_aggregate_candidates() {
        assert!(should_try_provider_executor_aggregate_route(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::Claude,
                route_kind: GatewayUpstreamRouteKind::AggregateApi,
            }
        ));
        assert!(should_try_provider_executor_aggregate_route(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::Gemini,
                route_kind: GatewayUpstreamRouteKind::AggregateApi,
            }
        ));
        assert!(!should_try_provider_executor_aggregate_route(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::Claude,
                route_kind: GatewayUpstreamRouteKind::AccountRotation,
            }
        ));
        assert!(should_try_provider_executor_aggregate_route(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::CodexResponses,
                route_kind: GatewayUpstreamRouteKind::AggregateApi,
            }
        ));
        assert!(!should_try_provider_executor_aggregate_route(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::Gemini,
                route_kind: GatewayUpstreamRouteKind::AccountRotation,
            }
        ));
        assert!(!should_try_provider_executor_aggregate_route(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::CodexResponses,
                route_kind: GatewayUpstreamRouteKind::AccountRotation,
            }
        ));
    }

    #[test]
    fn hybrid_account_first_keeps_account_empty_for_aggregate_fallback() {
        let hybrid = GatewayUpstreamExecutionPlan {
            executor_kind: GatewayUpstreamExecutorKind::CodexResponses,
            route_kind: GatewayUpstreamRouteKind::HybridAccountFirst,
        };
        let account_only = GatewayUpstreamExecutionPlan {
            executor_kind: GatewayUpstreamExecutorKind::CodexResponses,
            route_kind: GatewayUpstreamRouteKind::AccountRotation,
        };
        let aggregate_only = GatewayUpstreamExecutionPlan {
            executor_kind: GatewayUpstreamExecutorKind::CodexResponses,
            route_kind: GatewayUpstreamRouteKind::AggregateApi,
        };

        assert!(!respond_when_account_candidates_empty(hybrid));
        assert!(respond_when_account_candidates_empty(account_only));
        assert!(respond_when_account_candidates_empty(aggregate_only));
    }

    #[test]
    fn only_hybrid_falls_back_to_aggregate_after_account_exhaustion() {
        assert!(should_fallback_to_aggregate_after_account_exhaustion(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::CodexResponses,
                route_kind: GatewayUpstreamRouteKind::HybridAccountFirst,
            }
        ));
        assert!(!should_fallback_to_aggregate_after_account_exhaustion(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::CodexResponses,
                route_kind: GatewayUpstreamRouteKind::AccountRotation,
            }
        ));
        assert!(!should_fallback_to_aggregate_after_account_exhaustion(
            GatewayUpstreamExecutionPlan {
                executor_kind: GatewayUpstreamExecutorKind::CodexResponses,
                route_kind: GatewayUpstreamRouteKind::AggregateApi,
            }
        ));
    }

    #[test]
    fn hybrid_route_error_mentions_both_pools() {
        let message = hybrid_route_error_message(
            Some("无可用账号(no available account)"),
            "aggregate api not found for provider codex",
        );

        assert!(message.contains("账号池与聚合 API 均不可用"));
        assert!(message.contains("no available account"));
        assert!(message.contains("aggregate api not found for provider codex"));
    }

    #[test]
    fn provider_upstream_hint_reports_expected_aggregate_provider_type() {
        assert_eq!(
            provider_upstream_hint(GatewayUpstreamExecutorKind::Claude),
            Some(("Claude", "claude"))
        );
        assert_eq!(
            provider_upstream_hint(GatewayUpstreamExecutorKind::Gemini),
            Some(("Gemini", "gemini"))
        );
        assert_eq!(
            provider_upstream_hint(GatewayUpstreamExecutorKind::CodexResponses),
            None
        );
    }
}
