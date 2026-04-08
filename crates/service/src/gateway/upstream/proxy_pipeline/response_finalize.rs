use tiny_http::Request;

use super::super::super::request_log::RequestLogUsage;
use super::execution_context::GatewayUpstreamExecutionContext;

pub(super) enum FinalizeUpstreamResponseOutcome {
    Handled,
    Failover,
}

/// 函数 `respond_terminal`
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
pub(in super::super) fn respond_terminal(
    request: Request,
    status_code: u16,
    message: String,
    trace_id: Option<&str>,
) -> Result<(), String> {
    let response =
        super::super::super::error_response::terminal_text_response(status_code, message, trace_id);
    let _ = request.respond(response);
    Ok(())
}

/// 函数 `is_client_disconnect_error`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - message: 参数 message
///
/// # 返回
/// 返回函数执行结果
fn is_client_disconnect_error(message: &str) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    normalized.contains("broken pipe")
        || normalized.contains("connection reset")
        || normalized.contains("connection aborted")
        || normalized.contains("connection was forcibly closed")
        || normalized.contains("os error 32")
        || normalized.contains("os error 54")
        || normalized.contains("os error 104")
}

/// 函数 `respond_total_timeout`
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
pub(super) fn respond_total_timeout(
    request: Request,
    context: &GatewayUpstreamExecutionContext<'_>,
    trace_id: &str,
    started_at: std::time::Instant,
    model_for_log: Option<&str>,
    attempted_account_ids: Option<&[String]>,
) -> Result<(), String> {
    let message = "upstream total timeout exceeded".to_string();
    context.log_final_result_with_model(
        None,
        None,
        model_for_log,
        504,
        RequestLogUsage::default(),
        Some(message.as_str()),
        started_at.elapsed().as_millis(),
        attempted_account_ids,
    );
    respond_terminal(request, 504, message, Some(trace_id))
}

/// 函数 `finalize_terminal_candidate`
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
pub(super) fn finalize_terminal_candidate(
    request: Request,
    context: &GatewayUpstreamExecutionContext<'_>,
    account_id: &str,
    last_attempt_url: Option<&str>,
    status_code: u16,
    message: String,
    trace_id: &str,
    started_at: std::time::Instant,
    model_for_log: Option<&str>,
    attempted_account_ids: Option<&[String]>,
) -> Result<(), String> {
    let _ = context.mark_account_unavailable_for_gateway_error(account_id, &message);
    context.log_final_result_with_model(
        Some(account_id),
        last_attempt_url,
        model_for_log,
        status_code,
        RequestLogUsage::default(),
        Some(message.as_str()),
        started_at.elapsed().as_millis(),
        attempted_account_ids,
    );
    respond_terminal(request, status_code, message, Some(trace_id))
}

/// 函数 `finalize_upstream_response`
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
#[allow(clippy::too_many_arguments)]
pub(super) fn finalize_upstream_response(
    request: Request,
    response: reqwest::blocking::Response,
    inflight_guard: super::super::super::AccountInFlightGuard,
    context: &GatewayUpstreamExecutionContext<'_>,
    account_id: &str,
    last_attempt_url: Option<&str>,
    last_attempt_error: Option<&str>,
    response_adapter: super::super::super::ResponseAdapter,
    gemini_stream_output_mode: Option<super::super::super::GeminiStreamOutputMode>,
    tool_name_restore_map: &super::super::super::ToolNameRestoreMap,
    client_is_stream: bool,
    path: &str,
    trace_id: &str,
    started_at: std::time::Instant,
    model_for_log: Option<&str>,
    attempted_account_ids: Option<&[String]>,
    has_more_candidates: bool,
) -> Result<FinalizeUpstreamResponseOutcome, String> {
    let status_code = response.status().as_u16();
    let mut final_error = None;

    let bridge = super::super::super::respond_with_upstream(
        request,
        response,
        inflight_guard,
        response_adapter,
        None,
        gemini_stream_output_mode,
        path,
        Some(tool_name_restore_map),
        client_is_stream,
        has_more_candidates,
        Some(trace_id),
    )?;
    let bridge_output_text_len = bridge
        .usage
        .output_text
        .as_deref()
        .map(str::trim)
        .map(str::len)
        .unwrap_or(0);
    super::super::super::trace_log::log_bridge_result(
        trace_id,
        format!("{response_adapter:?}").as_str(),
        path,
        client_is_stream,
        bridge.stream_terminal_seen,
        bridge.stream_terminal_error.as_deref(),
        bridge.delivery_error.as_deref(),
        bridge_output_text_len,
        bridge.usage.output_tokens,
        bridge.delivered_status_code,
        bridge.upstream_error_hint.as_deref(),
        bridge.upstream_request_id.as_deref(),
        bridge.upstream_cf_ray.as_deref(),
        bridge.upstream_auth_error.as_deref(),
        bridge.upstream_identity_error_code.as_deref(),
        bridge.upstream_content_type.as_deref(),
        bridge.last_sse_event_type.as_deref(),
    );
    if matches!(
        response_adapter,
        super::super::super::ResponseAdapter::GeminiJson
            | super::super::super::ResponseAdapter::GeminiSse
            | super::super::super::ResponseAdapter::GeminiCliJson
            | super::super::super::ResponseAdapter::GeminiCliSse
    ) {
        super::super::super::trace_log::log_gemini_bridge_diagnostics(
            trace_id,
            format!("{response_adapter:?}").as_str(),
            gemini_stream_output_mode.map(|mode| match mode {
                super::super::super::GeminiStreamOutputMode::Sse => "sse",
                super::super::super::GeminiStreamOutputMode::Raw => "raw",
            }),
            bridge.stream_terminal_seen,
            bridge.stream_terminal_error.as_deref(),
            bridge.last_sse_event_type.as_deref(),
            bridge.upstream_content_type.as_deref(),
        );
    }

    if let Some(upstream_hint) = bridge.upstream_error_hint.as_deref() {
        final_error = Some(upstream_hint.to_string());
    } else if status_code >= 400 {
        final_error = last_attempt_error.map(str::to_string);
    }

    let bridge_ok = bridge.is_ok(client_is_stream);
    if final_error.is_none() && !bridge_ok {
        final_error = Some(
            bridge
                .error_message(client_is_stream)
                .unwrap_or_else(|| "upstream response incomplete".to_string()),
        );
    }
    let gateway_failover = final_error.as_deref().is_some_and(|error| {
        crate::account_status::should_failover_for_gateway_error(error, has_more_candidates)
    });
    let usage_limit_failover = final_error
        .as_deref()
        .is_some_and(crate::account_status::is_usage_limit_gateway_error);

    let upstream_stream_failed = client_is_stream
        && (!bridge.stream_terminal_seen || bridge.stream_terminal_error.is_some());
    let client_delivery_failed = bridge
        .delivery_error
        .as_deref()
        .is_some_and(is_client_disconnect_error);
    let status_for_log = if client_delivery_failed {
        499
    } else if let Some(delivered_status_code) = bridge.delivered_status_code {
        delivered_status_code
    } else if status_code >= 400 {
        status_code
    } else if upstream_stream_failed {
        502
    } else if gateway_failover {
        502
    } else if bridge_ok {
        status_code
    } else {
        502
    };

    if upstream_stream_failed {
        super::super::super::mark_account_cooldown(
            account_id,
            super::super::super::CooldownReason::Network,
        );
        super::super::super::record_route_quality(account_id, 502);
    }

    if usage_limit_failover {
        super::super::super::mark_account_cooldown(
            account_id,
            super::super::super::CooldownReason::Default,
        );
    }

    if let Some(error) = final_error.as_deref() {
        let _ = context.mark_account_unavailable_for_gateway_error(account_id, error);
    }

    let usage = bridge.usage;
    context.log_final_result_with_model(
        Some(account_id),
        last_attempt_url,
        model_for_log,
        status_for_log,
        RequestLogUsage {
            input_tokens: usage.input_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            output_tokens: usage.output_tokens,
            total_tokens: usage.total_tokens,
            reasoning_output_tokens: usage.reasoning_output_tokens,
        },
        final_error.as_deref(),
        started_at.elapsed().as_millis(),
        attempted_account_ids,
    );
    if gateway_failover {
        return Ok(FinalizeUpstreamResponseOutcome::Failover);
    }
    Ok(FinalizeUpstreamResponseOutcome::Handled)
}
