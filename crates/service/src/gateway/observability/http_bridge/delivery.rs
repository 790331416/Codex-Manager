use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tiny_http::{Header, Request, Response, StatusCode};

use crate::gateway::error_log::GatewayErrorLogInput;
use crate::gateway::upstream::GatewayStreamResponse;

use super::super::{
    adapt_upstream_response_with_tool_name_restore_map, build_anthropic_error_body,
    build_gemini_error_body, GeminiStreamOutputMode, ResponseAdapter, ToolNameRestoreMap,
};
use super::{
    collect_non_stream_json_from_sse_bytes, extract_error_hint_from_body,
    extract_error_message_from_json, looks_like_sse_payload, merge_usage, parse_usage_from_json,
    push_trace_id_header, usage_has_signal, AnthropicSseReader, GeminiSseReader,
    OpenAIChatCompletionsSseReader, OpenAICompletionsSseReader,
    OpenAIResponsesPassthroughSseReader, PassthroughSseCollector, PassthroughSseProtocol,
    PassthroughSseUsageReader, SseKeepAliveFrame, UpstreamResponseBridgeResult,
    UpstreamResponseUsage,
};

const REQUEST_ID_HEADER_CANDIDATES: &[&str] = &["x-request-id", "x-oai-request-id"];
const CF_RAY_HEADER_NAME: &str = "cf-ray";
const AUTH_ERROR_HEADER_NAME: &str = "x-openai-authorization-error";

/// 函数 `is_compact_request_path`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - path: 参数 path
///
/// # 返回
/// 返回函数执行结果
fn is_compact_request_path(path: &str) -> bool {
    path == "/v1/responses/compact" || path.starts_with("/v1/responses/compact?")
}

/// 函数 `first_upstream_header`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - headers: 参数 headers
/// - names: 参数 names
///
/// # 返回
/// 返回函数执行结果
fn first_upstream_header(headers: &reqwest::header::HeaderMap, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

/// 函数 `compact_debug_suffix`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - kind: 参数 kind
/// - request_id: 参数 request_id
/// - cf_ray: 参数 cf_ray
/// - auth_error: 参数 auth_error
/// - identity_error_code: 参数 identity_error_code
///
/// # 返回
/// 返回函数执行结果
fn compact_debug_suffix(
    kind: Option<&str>,
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> String {
    let mut details = Vec::new();
    if let Some(kind) = kind.map(str::trim).filter(|value| !value.is_empty()) {
        details.push(format!("kind={kind}"));
    }
    if let Some(request_id) = request_id.map(str::trim).filter(|value| !value.is_empty()) {
        details.push(format!("request_id={request_id}"));
    }
    if let Some(cf_ray) = cf_ray.map(str::trim).filter(|value| !value.is_empty()) {
        details.push(format!("cf_ray={cf_ray}"));
    }
    if let Some(auth_error) = auth_error.map(str::trim).filter(|value| !value.is_empty()) {
        details.push(format!("auth_error={auth_error}"));
    }
    if let Some(identity_error_code) = identity_error_code
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        details.push(format!("identity_error_code={identity_error_code}"));
    }
    if details.is_empty() {
        String::new()
    } else {
        format!(" [{}]", details.join(", "))
    }
}

/// 函数 `with_upstream_debug_suffix`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - message: 参数 message
/// - kind: 参数 kind
/// - request_id: 参数 request_id
/// - cf_ray: 参数 cf_ray
/// - auth_error: 参数 auth_error
/// - identity_error_code: 参数 identity_error_code
///
/// # 返回
/// 返回函数执行结果
fn with_upstream_debug_suffix(
    message: Option<String>,
    kind: Option<&str>,
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> Option<String> {
    let message = message?;
    let suffix = compact_debug_suffix(kind, request_id, cf_ray, auth_error, identity_error_code);
    if suffix.is_empty() {
        Some(message)
    } else {
        Some(format!("{message}{suffix}"))
    }
}

/// 函数 `should_suppress_deactivation_delivery`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - upstream_error_hint: 参数 upstream_error_hint
/// - allow_failover_for_deactivation: 参数 allow_failover_for_deactivation
///
/// # 返回
/// 返回函数执行结果
fn should_suppress_deactivation_delivery(
    upstream_error_hint: Option<&str>,
    allow_failover_for_deactivation: bool,
) -> bool {
    allow_failover_for_deactivation
        && upstream_error_hint.is_some_and(|message| {
            crate::account_status::deactivation_reason_from_message(message).is_some()
        })
}

/// 函数 `looks_like_blocked_marker`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - value: 参数 value
///
/// # 返回
/// 返回函数执行结果
fn looks_like_blocked_marker(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized.contains("blocked")
        || normalized.contains("unsupported_country_region_territory")
        || normalized.contains("unsupported_country")
        || normalized.contains("region_restricted")
}

fn body_as_trimmed_text(body: &[u8]) -> Option<&str> {
    std::str::from_utf8(body)
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn text_looks_like_html(text: &str) -> bool {
    let normalized = text.trim().to_ascii_lowercase();
    normalized.contains("<html")
        || normalized.contains("<!doctype html")
        || normalized.contains("<body")
        || normalized.contains("</html>")
}

fn body_looks_like_html(body: &[u8]) -> bool {
    body_as_trimmed_text(body).is_some_and(text_looks_like_html)
}

fn body_looks_like_cloudflare_challenge(status_code: u16, body: &[u8]) -> bool {
    body_as_trimmed_text(body).is_some_and(|text| {
        let normalized = text.to_ascii_lowercase();
        let looks_like_challenge = normalized.contains("cloudflare")
            || normalized.contains("cf-chl")
            || normalized.contains("just a moment")
            || normalized.contains("attention required")
            || normalized.contains("captcha")
            || normalized.contains("security check")
            || normalized.contains("access denied")
            || normalized.contains("waf");
        looks_like_challenge || (text_looks_like_html(text) && matches!(status_code, 401 | 403))
    })
}

fn replace_content_type_header(headers: &mut Vec<Header>, content_type: &str) {
    headers.retain(|header| {
        !header
            .field
            .as_str()
            .as_str()
            .eq_ignore_ascii_case("Content-Type")
    });
    if let Ok(header) = Header::from_bytes(b"Content-Type".as_slice(), content_type.as_bytes()) {
        headers.push(header);
    }
}

fn force_openai_responses_stream_content_type(
    headers: &mut Vec<Header>,
    request_path: &str,
    is_stream: bool,
) {
    if is_stream && request_path.starts_with("/v1/responses") {
        replace_content_type_header(headers, "text/event-stream");
    }
}

/// 函数 `classify_compact_invalid_success_kind`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - body: 参数 body
/// - cf_ray: 参数 cf_ray
/// - auth_error: 参数 auth_error
/// - identity_error_code: 参数 identity_error_code
///
/// # 返回
/// 返回函数执行结果
fn classify_compact_invalid_success_kind(
    body: &[u8],
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> &'static str {
    if auth_error.is_some_and(looks_like_blocked_marker)
        || identity_error_code.is_some_and(looks_like_blocked_marker)
    {
        return "cloudflare_blocked";
    }
    if body_looks_like_cloudflare_challenge(502, body) {
        return "cloudflare_challenge";
    }
    if body_looks_like_html(body) {
        return "html";
    }
    if identity_error_code.is_some() {
        return "identity_error";
    }
    if auth_error.is_some() {
        return "auth_error";
    }
    if cf_ray.is_some() {
        return "cloudflare_edge";
    }
    if serde_json::from_slice::<Value>(body).is_ok() {
        "invalid_success_body"
    } else if body.is_empty() {
        "empty"
    } else {
        "non_json"
    }
}

/// 函数 `classify_compact_non_success_kind`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - status_code: 参数 status_code
/// - content_type: 参数 content_type
/// - body: 参数 body
/// - cf_ray: 参数 cf_ray
/// - auth_error: 参数 auth_error
/// - identity_error_code: 参数 identity_error_code
///
/// # 返回
/// 返回函数执行结果
fn classify_compact_non_success_kind(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> &'static str {
    if auth_error.is_some_and(looks_like_blocked_marker)
        || identity_error_code.is_some_and(looks_like_blocked_marker)
    {
        return "cloudflare_blocked";
    }
    if body_looks_like_cloudflare_challenge(status_code, body) {
        return "cloudflare_challenge";
    }
    if body_looks_like_html(body) {
        return "html";
    }
    if content_type
        .map(crate::gateway::is_html_content_type)
        .unwrap_or(false)
    {
        return "html";
    }
    if identity_error_code.is_some() {
        return "identity_error";
    }
    if auth_error.is_some() {
        return "auth_error";
    }
    if cf_ray.is_some() {
        return "cloudflare_edge";
    }
    if serde_json::from_slice::<Value>(body).is_ok() {
        "json_error"
    } else if body.is_empty() {
        "empty"
    } else {
        "non_json"
    }
}

/// 函数 `compact_success_body_is_valid`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - body: 参数 body
///
/// # 返回
/// 返回函数执行结果
fn compact_success_body_is_valid(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("output").cloned())
        .is_some_and(|output| output.is_array())
}

/// 函数 `build_invalid_compact_success_message`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - body: 参数 body
/// - request_id: 参数 request_id
/// - cf_ray: 参数 cf_ray
/// - auth_error: 参数 auth_error
/// - identity_error_code: 参数 identity_error_code
///
/// # 返回
/// 返回函数执行结果
fn build_invalid_compact_success_message(
    body: &[u8],
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> String {
    let kind = classify_compact_invalid_success_kind(body, cf_ray, auth_error, identity_error_code);
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        if let Some(message) = extract_error_message_from_json(&value) {
            return format!(
                "invalid upstream compact response: {message}{}",
                compact_debug_suffix(
                    Some(kind),
                    request_id,
                    cf_ray,
                    auth_error,
                    identity_error_code
                )
            );
        }
    }
    if let Some(hint) = extract_error_hint_from_body(502, body) {
        return format!(
            "invalid upstream compact response: {hint}{}",
            compact_debug_suffix(
                Some(kind),
                request_id,
                cf_ray,
                auth_error,
                identity_error_code
            )
        );
    }
    format!(
        "invalid upstream compact response: missing output array{}",
        compact_debug_suffix(
            Some(kind),
            request_id,
            cf_ray,
            auth_error,
            identity_error_code
        )
    )
}

/// 函数 `compact_non_success_body_should_be_normalized`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - status_code: 参数 status_code
/// - content_type: 参数 content_type
/// - body: 参数 body
/// - auth_error: 参数 auth_error
/// - identity_error_code: 参数 identity_error_code
///
/// # 返回
/// 返回函数执行结果
fn non_success_body_should_be_normalized(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> bool {
    if status_code < 400 {
        return false;
    }
    if auth_error
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
        || identity_error_code
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
    {
        return true;
    }
    if content_type
        .map(crate::gateway::is_html_content_type)
        .unwrap_or(false)
    {
        return true;
    }
    body_looks_like_cloudflare_challenge(status_code, body) || body_looks_like_html(body)
}

fn compact_non_success_body_should_be_normalized(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> bool {
    non_success_body_should_be_normalized(
        status_code,
        content_type,
        body,
        auth_error,
        identity_error_code,
    )
}

fn build_passthrough_non_success_message(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> String {
    let kind = classify_compact_non_success_kind(
        status_code,
        content_type,
        body,
        cf_ray,
        auth_error,
        identity_error_code,
    );
    if let Some(hint) = extract_error_hint_from_body(status_code, body) {
        return format!(
            "upstream server error: {hint}{}",
            compact_debug_suffix(
                Some(kind),
                request_id,
                cf_ray,
                auth_error,
                identity_error_code
            )
        );
    }
    format!(
        "upstream server error: status={status_code}{}",
        compact_debug_suffix(
            Some(kind),
            request_id,
            cf_ray,
            auth_error,
            identity_error_code
        )
    )
}

/// 函数 `respond_synthesized_compact_error_body`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - request: 参数 request
/// - status_code: 参数 status_code
/// - usage: 参数 usage
/// - message: 参数 message
/// - request_id: 参数 request_id
/// - cf_ray: 参数 cf_ray
/// - trace_id: 参数 trace_id
///
/// # 返回
/// 返回函数执行结果
fn respond_synthesized_compact_error_body(
    request: Request,
    status_code: u16,
    usage: UpstreamResponseUsage,
    message: String,
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    trace_id: Option<&str>,
) -> UpstreamResponseBridgeResult {
    let response_message = crate::gateway::error_message_for_client(
        crate::gateway::prefers_raw_errors_for_tiny_http_request(&request),
        message.as_str(),
    );
    let response = crate::gateway::error_response::terminal_text_response(
        status_code,
        response_message,
        trace_id,
    );
    let delivery_error = request.respond(response).err().map(|err| err.to_string());
    UpstreamResponseBridgeResult {
        usage,
        stream_terminal_seen: true,
        stream_terminal_error: None,
        delivery_error,
        upstream_error_hint: Some(message),
        delivered_status_code: Some(status_code),
        upstream_request_id: request_id.map(str::to_string),
        upstream_cf_ray: cf_ray.map(str::to_string),
        upstream_auth_error: None,
        upstream_identity_error_code: None,
        upstream_content_type: Some("application/json".to_string()),
        last_sse_event_type: None,
    }
}

/// 函数 `with_bridge_debug_meta`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - result: 参数 result
/// - upstream_request_id: 参数 upstream_request_id
/// - upstream_cf_ray: 参数 upstream_cf_ray
/// - upstream_auth_error: 参数 upstream_auth_error
/// - upstream_identity_error_code: 参数 upstream_identity_error_code
/// - upstream_content_type: 参数 upstream_content_type
/// - last_sse_event_type: 参数 last_sse_event_type
///
/// # 返回
/// 返回函数执行结果
fn with_bridge_debug_meta(
    mut result: UpstreamResponseBridgeResult,
    upstream_request_id: &Option<String>,
    upstream_cf_ray: &Option<String>,
    upstream_auth_error: &Option<String>,
    upstream_identity_error_code: &Option<String>,
    upstream_content_type: &Option<String>,
    last_sse_event_type: Option<String>,
) -> UpstreamResponseBridgeResult {
    result.upstream_request_id = upstream_request_id.clone();
    result.upstream_cf_ray = upstream_cf_ray.clone();
    result.upstream_auth_error = upstream_auth_error.clone();
    result.upstream_identity_error_code = upstream_identity_error_code.clone();
    result.upstream_content_type = upstream_content_type.clone();
    result.last_sse_event_type = last_sse_event_type;
    result
}

fn log_bridge_stream_diagnostics(
    response_adapter: ResponseAdapter,
    request_path: &str,
    result: &UpstreamResponseBridgeResult,
) {
    if result.delivery_error.is_none()
        && result.stream_terminal_seen
        && result.stream_terminal_error.is_none()
    {
        return;
    }

    log::warn!(
        "event=gateway_bridge_stream_diagnostics adapter={:?} path={} stream_terminal_seen={} stream_terminal_error={} delivery_error={} upstream_error_hint={} last_sse_event_type={} upstream_request_id={} upstream_cf_ray={} upstream_content_type={}",
        response_adapter,
        request_path,
        if result.stream_terminal_seen { "true" } else { "false" },
        result.stream_terminal_error.as_deref().unwrap_or("-"),
        result.delivery_error.as_deref().unwrap_or("-"),
        result.upstream_error_hint.as_deref().unwrap_or("-"),
        result.last_sse_event_type.as_deref().unwrap_or("-"),
        result.upstream_request_id.as_deref().unwrap_or("-"),
        result.upstream_cf_ray.as_deref().unwrap_or("-"),
        result.upstream_content_type.as_deref().unwrap_or("-"),
    );
}

/// 函数 `respond_invalid_compact_success_body`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - request: 参数 request
/// - usage: 参数 usage
/// - body: 参数 body
/// - request_id: 参数 request_id
/// - cf_ray: 参数 cf_ray
/// - auth_error: 参数 auth_error
/// - identity_error_code: 参数 identity_error_code
/// - trace_id: 参数 trace_id
///
/// # 返回
/// 返回函数执行结果
fn respond_invalid_compact_success_body(
    request: Request,
    usage: UpstreamResponseUsage,
    body: &[u8],
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
    trace_id: Option<&str>,
) -> UpstreamResponseBridgeResult {
    with_bridge_debug_meta(
        respond_synthesized_compact_error_body(
            request,
            502,
            usage,
            build_invalid_compact_success_message(
                body,
                request_id,
                cf_ray,
                auth_error,
                identity_error_code,
            ),
            request_id,
            cf_ray,
            trace_id,
        ),
        &request_id.map(str::to_string),
        &cf_ray.map(str::to_string),
        &auth_error.map(str::to_string),
        &identity_error_code.map(str::to_string),
        &Some("application/json".to_string()),
        None,
    )
}

/// 函数 `respond_invalid_compact_non_success_body`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - request: 参数 request
/// - status_code: 参数 status_code
/// - usage: 参数 usage
/// - body: 参数 body
/// - content_type: 参数 content_type
/// - request_id: 参数 request_id
/// - cf_ray: 参数 cf_ray
/// - auth_error: 参数 auth_error
/// - identity_error_code: 参数 identity_error_code
/// - trace_id: 参数 trace_id
///
/// # 返回
/// 返回函数执行结果
fn respond_invalid_compact_non_success_body(
    request: Request,
    status_code: u16,
    usage: UpstreamResponseUsage,
    body: &[u8],
    content_type: Option<&str>,
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
    trace_id: Option<&str>,
) -> UpstreamResponseBridgeResult {
    let gateway_status_code = 502;
    let request_method = request.method().as_str().to_string();
    let request_path = request.url().to_string();
    let error_kind = classify_compact_non_success_kind(
        status_code,
        content_type,
        body,
        cf_ray,
        auth_error,
        identity_error_code,
    );
    let message = build_passthrough_non_success_message(
        status_code,
        content_type,
        body,
        request_id,
        cf_ray,
        auth_error,
        identity_error_code,
    );
    crate::gateway::write_gateway_error_log(GatewayErrorLogInput {
        trace_id,
        request_path: request_path.as_str(),
        method: request_method.as_str(),
        stage: "compact_bridge_non_success",
        error_kind: Some(error_kind),
        cf_ray,
        status_code: Some(gateway_status_code),
        compression_enabled: false,
        compression_retry_attempted: false,
        message: message.as_str(),
        ..GatewayErrorLogInput::default()
    });

    with_bridge_debug_meta(
        respond_synthesized_compact_error_body(
            request,
            gateway_status_code,
            usage,
            message,
            request_id,
            cf_ray,
            trace_id,
        ),
        &request_id.map(str::to_string),
        &cf_ray.map(str::to_string),
        &auth_error.map(str::to_string),
        &identity_error_code.map(str::to_string),
        &Some("application/json".to_string()),
        None,
    )
}

fn respond_normalized_passthrough_non_success_body(
    request: Request,
    usage: UpstreamResponseUsage,
    body: &[u8],
    content_type: Option<&str>,
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
    trace_id: Option<&str>,
) -> UpstreamResponseBridgeResult {
    let request_method = request.method().as_str().to_string();
    let request_path = request.url().to_string();
    let error_kind = classify_compact_non_success_kind(
        502,
        content_type,
        body,
        cf_ray,
        auth_error,
        identity_error_code,
    );
    let message = build_passthrough_non_success_message(
        502,
        content_type,
        body,
        request_id,
        cf_ray,
        auth_error,
        identity_error_code,
    );
    crate::gateway::write_gateway_error_log(GatewayErrorLogInput {
        trace_id,
        request_path: request_path.as_str(),
        method: request_method.as_str(),
        stage: "passthrough_bridge_non_success",
        error_kind: Some(error_kind),
        cf_ray,
        status_code: Some(502),
        compression_enabled: false,
        compression_retry_attempted: false,
        message: message.as_str(),
        ..GatewayErrorLogInput::default()
    });

    with_bridge_debug_meta(
        respond_synthesized_compact_error_body(
            request, 502, usage, message, request_id, cf_ray, trace_id,
        ),
        &request_id.map(str::to_string),
        &cf_ray.map(str::to_string),
        &auth_error.map(str::to_string),
        &identity_error_code.map(str::to_string),
        &Some("application/json".to_string()),
        None,
    )
}

/// 函数 `respond_with_upstream`
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
pub(crate) fn respond_with_upstream(
    request: Request,
    upstream: reqwest::blocking::Response,
    _inflight_guard: super::super::AccountInFlightGuard,
    response_adapter: ResponseAdapter,
    passthrough_sse_protocol: Option<PassthroughSseProtocol>,
    gemini_stream_output_mode: Option<GeminiStreamOutputMode>,
    request_path: &str,
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
    is_stream: bool,
    allow_failover_for_deactivation: bool,
    trace_id: Option<&str>,
    fallback_model: Option<&str>,
    request_started_at: std::time::Instant,
) -> Result<UpstreamResponseBridgeResult, String> {
    let keepalive_frame = resolve_stream_keepalive_frame(response_adapter, request_path);
    let passthrough_sse_protocol =
        passthrough_sse_protocol.unwrap_or(PassthroughSseProtocol::Generic);
    let upstream_request_id =
        first_upstream_header(upstream.headers(), REQUEST_ID_HEADER_CANDIDATES);
    let upstream_cf_ray = first_upstream_header(upstream.headers(), &[CF_RAY_HEADER_NAME]);
    let upstream_auth_error = first_upstream_header(upstream.headers(), &[AUTH_ERROR_HEADER_NAME]);
    let upstream_identity_error_code =
        crate::gateway::extract_identity_error_code_from_headers(upstream.headers());
    let upstream_content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());
    match response_adapter {
        ResponseAdapter::Passthrough => {
            let status = StatusCode(upstream.status().as_u16());
            let mut headers = Vec::new();
            for (name, value) in upstream.headers().iter() {
                let name_str = name.as_str();
                if name_str.eq_ignore_ascii_case("transfer-encoding")
                    || name_str.eq_ignore_ascii_case("content-length")
                    || name_str.eq_ignore_ascii_case("connection")
                {
                    continue;
                }
                if let Ok(header) = Header::from_bytes(name_str.as_bytes(), value.as_bytes()) {
                    headers.push(header);
                }
            }
            if let Some(trace_id) = trace_id {
                push_trace_id_header(&mut headers, trace_id);
            }
            let is_json = upstream_content_type
                .as_deref()
                .map(|value| value.to_ascii_lowercase().contains("application/json"))
                .unwrap_or(false);
            let is_sse = upstream_content_type
                .as_deref()
                .map(|value| value.to_ascii_lowercase().starts_with("text/event-stream"))
                .unwrap_or(false);
            if !is_stream {
                let upstream_body = upstream
                    .bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let detected_sse =
                    is_sse || (!is_json && looks_like_sse_payload(upstream_body.as_ref()));
                let is_compact_request = is_compact_request_path(request_path);
                if detected_sse {
                    let (synthesized_body, mut usage) =
                        collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                    let synthesized_response = synthesized_body.is_some();
                    let body = synthesized_body.unwrap_or_else(|| upstream_body.to_vec());
                    if let Ok(value) = serde_json::from_slice::<Value>(&body) {
                        merge_usage(&mut usage, parse_usage_from_json(&value));
                    }
                    let upstream_error_hint = with_upstream_debug_suffix(
                        extract_error_hint_from_body(status.0, &body),
                        None,
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    );
                    if should_suppress_deactivation_delivery(
                        upstream_error_hint.as_deref(),
                        allow_failover_for_deactivation,
                    ) {
                        return Ok(with_bridge_debug_meta(
                            UpstreamResponseBridgeResult {
                                usage,
                                stream_terminal_seen: true,
                                stream_terminal_error: None,
                                delivery_error: None,
                                upstream_error_hint,
                                delivered_status_code: None,
                                upstream_request_id: None,
                                upstream_cf_ray: None,
                                upstream_auth_error: None,
                                upstream_identity_error_code: None,
                                upstream_content_type: None,
                                last_sse_event_type: None,
                            },
                            &upstream_request_id,
                            &upstream_cf_ray,
                            &upstream_auth_error,
                            &upstream_identity_error_code,
                            &upstream_content_type,
                            None,
                        ));
                    }
                    if synthesized_response {
                        headers.retain(|header| {
                            !header
                                .field
                                .as_str()
                                .as_str()
                                .eq_ignore_ascii_case("Content-Type")
                        });
                        if let Ok(content_type_header) = Header::from_bytes(
                            b"Content-Type".as_slice(),
                            b"application/json".as_slice(),
                        ) {
                            headers.push(content_type_header);
                        }
                    }
                    if status.0 < 400
                        && is_compact_request
                        && !compact_success_body_is_valid(body.as_ref())
                    {
                        return Ok(respond_invalid_compact_success_body(
                            request,
                            usage,
                            body.as_ref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    if is_compact_request
                        && compact_non_success_body_should_be_normalized(
                            status.0,
                            upstream_content_type.as_deref(),
                            body.as_ref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        )
                    {
                        return Ok(respond_invalid_compact_non_success_body(
                            request,
                            status.0,
                            usage,
                            body.as_ref(),
                            upstream_content_type.as_deref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    if status.0 >= 400
                        && non_success_body_should_be_normalized(
                            status.0,
                            upstream_content_type.as_deref(),
                            body.as_ref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        )
                    {
                        return Ok(respond_normalized_passthrough_non_success_body(
                            request,
                            usage,
                            body.as_ref(),
                            upstream_content_type.as_deref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    let len = Some(body.len());
                    let response =
                        Response::new(status, headers, std::io::Cursor::new(body), len, None);
                    let delivery_error = request.respond(response).err().map(|err| err.to_string());
                    return Ok(with_bridge_debug_meta(
                        UpstreamResponseBridgeResult {
                            usage,
                            stream_terminal_seen: true,
                            stream_terminal_error: None,
                            delivery_error,
                            upstream_error_hint,
                            delivered_status_code: None,
                            upstream_request_id: None,
                            upstream_cf_ray: None,
                            upstream_auth_error: None,
                            upstream_identity_error_code: None,
                            upstream_content_type: None,
                            last_sse_event_type: None,
                        },
                        &upstream_request_id,
                        &upstream_cf_ray,
                        &upstream_auth_error,
                        &upstream_identity_error_code,
                        &upstream_content_type,
                        None,
                    ));
                }

                let (_, sse_usage) = collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                let usage = if is_json {
                    serde_json::from_slice::<Value>(upstream_body.as_ref())
                        .ok()
                        .map(|value| parse_usage_from_json(&value))
                        .unwrap_or_default()
                } else if usage_has_signal(&sse_usage) {
                    sse_usage
                } else {
                    UpstreamResponseUsage::default()
                };
                if status.0 < 400
                    && is_compact_request
                    && !compact_success_body_is_valid(upstream_body.as_ref())
                {
                    return Ok(respond_invalid_compact_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                if is_compact_request
                    && compact_non_success_body_should_be_normalized(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                {
                    return Ok(respond_invalid_compact_non_success_body(
                        request,
                        status.0,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                if status.0 >= 400
                    && non_success_body_should_be_normalized(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                {
                    return Ok(respond_normalized_passthrough_non_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                let upstream_error_hint = with_upstream_debug_suffix(
                    extract_error_hint_from_body(status.0, upstream_body.as_ref()),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                );
                if should_suppress_deactivation_delivery(
                    upstream_error_hint.as_deref(),
                    allow_failover_for_deactivation,
                ) {
                    return Ok(with_bridge_debug_meta(
                        UpstreamResponseBridgeResult {
                            usage,
                            stream_terminal_seen: true,
                            stream_terminal_error: None,
                            delivery_error: None,
                            upstream_error_hint,
                            delivered_status_code: None,
                            upstream_request_id: None,
                            upstream_cf_ray: None,
                            upstream_auth_error: None,
                            upstream_identity_error_code: None,
                            upstream_content_type: None,
                            last_sse_event_type: None,
                        },
                        &upstream_request_id,
                        &upstream_cf_ray,
                        &upstream_auth_error,
                        &upstream_identity_error_code,
                        &upstream_content_type,
                        None,
                    ));
                }
                let len = Some(upstream_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(upstream_body.to_vec()),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage,
                        stream_terminal_seen: true,
                        stream_terminal_error: None,
                        delivery_error,
                        upstream_error_hint,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    None,
                ));
            }
            if is_stream && !is_sse && status.0 >= 400 {
                let upstream_body = upstream
                    .bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage = if is_json {
                    serde_json::from_slice::<Value>(upstream_body.as_ref())
                        .ok()
                        .map(|value| parse_usage_from_json(&value))
                        .unwrap_or_default()
                } else {
                    UpstreamResponseUsage::default()
                };
                if non_success_body_should_be_normalized(
                    status.0,
                    upstream_content_type.as_deref(),
                    upstream_body.as_ref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                ) {
                    return Ok(respond_normalized_passthrough_non_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                let upstream_error_hint = with_upstream_debug_suffix(
                    extract_error_hint_from_body(status.0, upstream_body.as_ref()),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                );
                let len = Some(upstream_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(upstream_body.to_vec()),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage,
                        stream_terminal_seen: true,
                        stream_terminal_error: None,
                        delivery_error,
                        upstream_error_hint,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    None,
                ));
            }
            if is_sse || is_stream {
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    if request_path.starts_with("/v1/responses") {
                        Box::new(OpenAIResponsesPassthroughSseReader::new(
                            upstream,
                            Arc::clone(&usage_collector),
                            keepalive_frame,
                            request_started_at,
                        ))
                    } else {
                        Box::new(PassthroughSseUsageReader::new(
                            upstream,
                            Arc::clone(&usage_collector),
                            keepalive_frame,
                            passthrough_sse_protocol,
                            request_started_at,
                        ))
                    };
                force_openai_responses_stream_content_type(&mut headers, request_path, is_stream);
                let response = Response::new(status, headers, response_body, None, None);
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                let collector = usage_collector
                    .lock()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                let last_sse_event_type = collector.last_event_type.clone();
                let result = with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage: collector.usage,
                        stream_terminal_seen: collector.saw_terminal,
                        stream_terminal_error: collector.terminal_error,
                        delivery_error,
                        upstream_error_hint: with_upstream_debug_suffix(
                            collector.upstream_error_hint,
                            None,
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        ),
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    last_sse_event_type,
                );
                log_bridge_stream_diagnostics(response_adapter, request_path, &result);
                return Ok(result);
            }
            let len = upstream.content_length().map(|v| v as usize);
            let response = Response::new(status, headers, upstream, len, None);
            let delivery_error = request.respond(response).err().map(|err| err.to_string());
            Ok(with_bridge_debug_meta(
                UpstreamResponseBridgeResult {
                    usage: UpstreamResponseUsage::default(),
                    stream_terminal_seen: true,
                    stream_terminal_error: None,
                    delivery_error,
                    upstream_error_hint: None,
                    delivered_status_code: None,
                    upstream_request_id: None,
                    upstream_cf_ray: None,
                    upstream_auth_error: None,
                    upstream_identity_error_code: None,
                    upstream_content_type: None,
                    last_sse_event_type: None,
                },
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
                None,
            ))
        }
        ResponseAdapter::OpenAIChatCompletionsJson
        | ResponseAdapter::OpenAIChatCompletionsSse
        | ResponseAdapter::OpenAICompletionsJson
        | ResponseAdapter::OpenAICompletionsSse => {
            let status = StatusCode(upstream.status().as_u16());
            let mut headers = Vec::new();
            for (name, value) in upstream.headers().iter() {
                let name_str = name.as_str();
                if name_str.eq_ignore_ascii_case("transfer-encoding")
                    || name_str.eq_ignore_ascii_case("content-length")
                    || name_str.eq_ignore_ascii_case("connection")
                    || name_str.eq_ignore_ascii_case("content-type")
                {
                    continue;
                }
                if let Ok(header) = Header::from_bytes(name_str.as_bytes(), value.as_bytes()) {
                    headers.push(header);
                }
            }
            if let Some(trace_id) = trace_id {
                push_trace_id_header(&mut headers, trace_id);
            }
            let is_sse = upstream_content_type
                .as_deref()
                .map(|value| value.to_ascii_lowercase().starts_with("text/event-stream"))
                .unwrap_or(false);
            let use_openai_sse_adapter = matches!(
                response_adapter,
                ResponseAdapter::OpenAIChatCompletionsSse | ResponseAdapter::OpenAICompletionsSse
            );

            if use_openai_sse_adapter && is_stream && !is_sse {
                log::warn!(
                    "event=gateway_openai_stream_content_type_mismatch adapter={:?} upstream_content_type={}",
                    response_adapter,
                    upstream_content_type
                        .as_deref()
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or("-")
                );
            }

            if use_openai_sse_adapter && (is_stream || is_sse) && is_sse {
                if let Ok(content_type_header) =
                    Header::from_bytes(b"Content-Type".as_slice(), b"text/event-stream".as_slice())
                {
                    headers.push(content_type_header);
                }
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let delivery_error =
                    if response_adapter == ResponseAdapter::OpenAIChatCompletionsSse {
                        let response = Response::new(
                            status,
                            headers,
                            OpenAIChatCompletionsSseReader::new(
                                upstream,
                                Arc::clone(&usage_collector),
                                tool_name_restore_map.cloned(),
                                request_started_at,
                            ),
                            None,
                            None,
                        );
                        request.respond(response).err().map(|err| err.to_string())
                    } else {
                        let response = Response::new(
                            status,
                            headers,
                            OpenAICompletionsSseReader::new(
                                upstream,
                                Arc::clone(&usage_collector),
                                request_started_at,
                            ),
                            None,
                            None,
                        );
                        request.respond(response).err().map(|err| err.to_string())
                    };
                let collector = usage_collector
                    .lock()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                let last_sse_event_type = collector.last_event_type.clone();
                let output_text_empty = collector
                    .usage
                    .output_text
                    .as_deref()
                    .map(str::trim)
                    .is_none_or(str::is_empty);
                if output_text_empty {
                    log::warn!(
                        "event=gateway_openai_stream_empty_output adapter={:?} terminal_seen={} terminal_error={} output_tokens={:?}",
                        response_adapter,
                        collector.saw_terminal,
                        collector.terminal_error.as_deref().unwrap_or("-"),
                        collector.usage.output_tokens
                    );
                }
                let result = with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage: collector.usage,
                        stream_terminal_seen: collector.saw_terminal,
                        stream_terminal_error: collector.terminal_error,
                        delivery_error,
                        upstream_error_hint: None,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    last_sse_event_type,
                );
                log_bridge_stream_diagnostics(response_adapter, request_path, &result);
                return Ok(result);
            }

            let upstream_body = upstream
                .bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let mut usage = if is_sse {
                let (_, parsed) = collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                parsed
            } else {
                UpstreamResponseUsage::default()
            };
            if let Ok(value) = serde_json::from_slice::<Value>(upstream_body.as_ref()) {
                merge_usage(&mut usage, parse_usage_from_json(&value));
            }
            let (mut body, mut content_type) =
                match adapt_upstream_response_with_tool_name_restore_map(
                    response_adapter,
                    upstream_content_type.as_deref(),
                    upstream_body.as_ref(),
                    tool_name_restore_map,
                ) {
                    Ok(result) => result,
                    Err(err) => (
                        serde_json::to_vec(&json!({
                            "error": {
                                "message": format!("response conversion failed: {err}"),
                                "type": "server_error"
                            }
                        }))
                        .unwrap_or_else(|_| {
                            b"{\"error\":{\"message\":\"response conversion failed\",\"type\":\"server_error\"}}"
                                .to_vec()
                        }),
                        "application/json",
                    ),
                };
            if use_openai_sse_adapter
                && is_stream
                && status.0 < 400
                && !content_type.eq_ignore_ascii_case("text/event-stream")
            {
                if let Ok(mapped_json) = serde_json::from_slice::<Value>(body.as_ref()) {
                    merge_usage(&mut usage, parse_usage_from_json(&mapped_json));
                    body = if response_adapter == ResponseAdapter::OpenAIChatCompletionsSse {
                        super::synthesize_chat_completion_sse_from_json(&mapped_json)
                    } else {
                        super::synthesize_completions_sse_from_json(&mapped_json)
                    };
                    content_type = "text/event-stream";
                    log::warn!(
                        "event=gateway_openai_stream_synthetic_sse adapter={:?} status={} upstream_content_type={}",
                        response_adapter,
                        status.0,
                        upstream_content_type
                            .as_deref()
                            .filter(|value| !value.trim().is_empty())
                            .unwrap_or("-")
                    );
                }
            }
            if let Ok(content_type_header) =
                Header::from_bytes(b"Content-Type".as_slice(), content_type.as_bytes())
            {
                headers.push(content_type_header);
            }
            let upstream_error_hint = with_upstream_debug_suffix(
                extract_error_hint_from_body(status.0, upstream_body.as_ref()),
                None,
                upstream_request_id.as_deref(),
                upstream_cf_ray.as_deref(),
                upstream_auth_error.as_deref(),
                upstream_identity_error_code.as_deref(),
            );
            if should_suppress_deactivation_delivery(
                upstream_error_hint.as_deref(),
                allow_failover_for_deactivation,
            ) {
                return Ok(with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage,
                        stream_terminal_seen: true,
                        stream_terminal_error: None,
                        delivery_error: None,
                        upstream_error_hint,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    None,
                ));
            }
            let len = Some(body.len());
            let response = Response::new(status, headers, std::io::Cursor::new(body), len, None);
            let delivery_error = request.respond(response).err().map(|err| err.to_string());
            Ok(with_bridge_debug_meta(
                UpstreamResponseBridgeResult {
                    usage,
                    stream_terminal_seen: true,
                    stream_terminal_error: None,
                    delivery_error,
                    upstream_error_hint,
                    delivered_status_code: None,
                    upstream_request_id: None,
                    upstream_cf_ray: None,
                    upstream_auth_error: None,
                    upstream_identity_error_code: None,
                    upstream_content_type: None,
                    last_sse_event_type: None,
                },
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
                None,
            ))
        }
        ResponseAdapter::AnthropicJson
        | ResponseAdapter::AnthropicSse
        | ResponseAdapter::GeminiJson
        | ResponseAdapter::GeminiSse
        | ResponseAdapter::GeminiCliJson
        | ResponseAdapter::GeminiCliSse => {
            let status = StatusCode(upstream.status().as_u16());
            let mut headers = Vec::new();
            for (name, value) in upstream.headers().iter() {
                let name_str = name.as_str();
                if name_str.eq_ignore_ascii_case("transfer-encoding")
                    || name_str.eq_ignore_ascii_case("content-length")
                    || name_str.eq_ignore_ascii_case("connection")
                    || name_str.eq_ignore_ascii_case("content-type")
                {
                    continue;
                }
                if let Ok(header) = Header::from_bytes(name_str.as_bytes(), value.as_bytes()) {
                    headers.push(header);
                }
            }
            if let Some(trace_id) = trace_id {
                push_trace_id_header(&mut headers, trace_id);
            }
            if response_adapter == ResponseAdapter::AnthropicSse
                && (is_stream
                    || upstream_content_type
                        .as_deref()
                        .map(|value| value.to_ascii_lowercase().starts_with("text/event-stream"))
                        .unwrap_or(false))
            {
                if let Ok(content_type_header) =
                    Header::from_bytes(b"Content-Type".as_slice(), b"text/event-stream".as_slice())
                {
                    headers.push(content_type_header);
                }
                let usage_collector = Arc::new(Mutex::new(UpstreamResponseUsage::default()));
                let response = Response::new(
                    status,
                    headers,
                    AnthropicSseReader::new(
                        upstream,
                        Arc::clone(&usage_collector),
                        fallback_model,
                        request_started_at,
                    ),
                    None,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                let usage = usage_collector
                    .lock()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                return Ok(with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage,
                        stream_terminal_seen: true,
                        stream_terminal_error: None,
                        delivery_error,
                        upstream_error_hint: None,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    None,
                ));
            }
            if matches!(
                response_adapter,
                ResponseAdapter::GeminiSse | ResponseAdapter::GeminiCliSse
            ) && (is_stream
                || upstream_content_type
                    .as_deref()
                    .map(|value| value.to_ascii_lowercase().starts_with("text/event-stream"))
                    .unwrap_or(false))
            {
                let gemini_stream_output_mode =
                    gemini_stream_output_mode.unwrap_or(GeminiStreamOutputMode::Sse);
                if gemini_stream_output_mode == GeminiStreamOutputMode::Sse {
                    if let Ok(content_type_header) = Header::from_bytes(
                        b"Content-Type".as_slice(),
                        b"text/event-stream".as_slice(),
                    ) {
                        headers.push(content_type_header);
                    }
                } else if let Ok(content_type_header) =
                    Header::from_bytes(b"Content-Type".as_slice(), b"application/json".as_slice())
                {
                    headers.push(content_type_header);
                }
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response = Response::new(
                    status,
                    headers,
                    GeminiSseReader::new(
                        upstream,
                        Arc::clone(&usage_collector),
                        tool_name_restore_map.cloned(),
                        gemini_stream_output_mode,
                        response_adapter == ResponseAdapter::GeminiCliSse,
                        request_started_at,
                    ),
                    None,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                let collector = usage_collector
                    .lock()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                let last_sse_event_type = collector.last_event_type.clone();
                let result = with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage: collector.usage,
                        stream_terminal_seen: collector.saw_terminal,
                        stream_terminal_error: collector.terminal_error,
                        delivery_error,
                        upstream_error_hint: collector.upstream_error_hint,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: last_sse_event_type.clone(),
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    last_sse_event_type,
                );
                log_bridge_stream_diagnostics(response_adapter, request_path, &result);
                return Ok(result);
            }

            let upstream_body = upstream
                .bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let usage = serde_json::from_slice::<Value>(upstream_body.as_ref())
                .ok()
                .map(|value| parse_usage_from_json(&value))
                .unwrap_or_default();

            let (body, content_type) = match adapt_upstream_response_with_tool_name_restore_map(
                response_adapter,
                upstream_content_type.as_deref(),
                upstream_body.as_ref(),
                tool_name_restore_map,
            ) {
                Ok(result) => result,
                Err(err) => (
                    if matches!(
                        response_adapter,
                        ResponseAdapter::GeminiJson
                            | ResponseAdapter::GeminiSse
                            | ResponseAdapter::GeminiCliJson
                            | ResponseAdapter::GeminiCliSse
                    ) {
                        build_gemini_error_body(&format!("response conversion failed: {err}"))
                    } else {
                        build_anthropic_error_body(&format!("response conversion failed: {err}"))
                    },
                    "application/json",
                ),
            };
            if let Ok(content_type_header) =
                Header::from_bytes(b"Content-Type".as_slice(), content_type.as_bytes())
            {
                headers.push(content_type_header);
            }
            let upstream_error_hint = with_upstream_debug_suffix(
                extract_error_hint_from_body(status.0, upstream_body.as_ref()),
                None,
                upstream_request_id.as_deref(),
                upstream_cf_ray.as_deref(),
                upstream_auth_error.as_deref(),
                upstream_identity_error_code.as_deref(),
            );
            if should_suppress_deactivation_delivery(
                upstream_error_hint.as_deref(),
                allow_failover_for_deactivation,
            ) {
                return Ok(with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage,
                        stream_terminal_seen: true,
                        stream_terminal_error: None,
                        delivery_error: None,
                        upstream_error_hint,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    None,
                ));
            }
            let len = Some(body.len());
            let response = Response::new(status, headers, std::io::Cursor::new(body), len, None);
            let delivery_error = request.respond(response).err().map(|err| err.to_string());
            Ok(with_bridge_debug_meta(
                UpstreamResponseBridgeResult {
                    usage,
                    stream_terminal_seen: true,
                    stream_terminal_error: None,
                    delivery_error,
                    upstream_error_hint,
                    delivered_status_code: None,
                    upstream_request_id: None,
                    upstream_cf_ray: None,
                    upstream_auth_error: None,
                    upstream_identity_error_code: None,
                    upstream_content_type: None,
                    last_sse_event_type: None,
                },
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
                None,
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn respond_with_stream_upstream(
    request: Request,
    upstream: GatewayStreamResponse,
    _inflight_guard: super::super::AccountInFlightGuard,
    response_adapter: ResponseAdapter,
    _passthrough_sse_protocol: Option<PassthroughSseProtocol>,
    _gemini_stream_output_mode: Option<GeminiStreamOutputMode>,
    request_path: &str,
    _tool_name_restore_map: Option<&ToolNameRestoreMap>,
    is_stream: bool,
    _allow_failover_for_deactivation: bool,
    trace_id: Option<&str>,
    _fallback_model: Option<&str>,
    request_started_at: std::time::Instant,
) -> Result<UpstreamResponseBridgeResult, String> {
    let keepalive_frame = resolve_stream_keepalive_frame(response_adapter, request_path);
    let upstream_request_id =
        first_upstream_header(upstream.headers(), REQUEST_ID_HEADER_CANDIDATES);
    let upstream_cf_ray = first_upstream_header(upstream.headers(), &[CF_RAY_HEADER_NAME]);
    let upstream_auth_error = first_upstream_header(upstream.headers(), &[AUTH_ERROR_HEADER_NAME]);
    let upstream_identity_error_code =
        crate::gateway::extract_identity_error_code_from_headers(upstream.headers());
    let upstream_content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());

    match response_adapter {
        ResponseAdapter::Passthrough => {
            let status = StatusCode(upstream.status().as_u16());
            let mut headers = Vec::new();
            for (name, value) in upstream.headers().iter() {
                let name_str = name.as_str();
                if name_str.eq_ignore_ascii_case("transfer-encoding")
                    || name_str.eq_ignore_ascii_case("content-length")
                    || name_str.eq_ignore_ascii_case("connection")
                {
                    continue;
                }
                if let Ok(header) = Header::from_bytes(name_str.as_bytes(), value.as_bytes()) {
                    headers.push(header);
                }
            }
            if let Some(trace_id) = trace_id {
                push_trace_id_header(&mut headers, trace_id);
            }
            let is_sse = upstream_content_type
                .as_deref()
                .map(|value| value.to_ascii_lowercase().starts_with("text/event-stream"))
                .unwrap_or(false);
            let is_json = upstream_content_type
                .as_deref()
                .map(|value| value.to_ascii_lowercase().contains("application/json"))
                .unwrap_or(false);

            if !is_stream {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let detected_sse =
                    is_sse || (!is_json && looks_like_sse_payload(upstream_body.as_ref()));
                let is_compact_request = is_compact_request_path(request_path);
                if detected_sse {
                    let (synthesized_body, mut usage) =
                        collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                    let synthesized_response = synthesized_body.is_some();
                    let body = synthesized_body.unwrap_or_else(|| upstream_body.to_vec());
                    if let Ok(value) = serde_json::from_slice::<Value>(&body) {
                        merge_usage(&mut usage, parse_usage_from_json(&value));
                    }
                    let upstream_error_hint = with_upstream_debug_suffix(
                        extract_error_hint_from_body(status.0, &body),
                        None,
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    );
                    if should_suppress_deactivation_delivery(
                        upstream_error_hint.as_deref(),
                        _allow_failover_for_deactivation,
                    ) {
                        return Ok(with_bridge_debug_meta(
                            UpstreamResponseBridgeResult {
                                usage,
                                stream_terminal_seen: true,
                                stream_terminal_error: None,
                                delivery_error: None,
                                upstream_error_hint,
                                delivered_status_code: None,
                                upstream_request_id: None,
                                upstream_cf_ray: None,
                                upstream_auth_error: None,
                                upstream_identity_error_code: None,
                                upstream_content_type: None,
                                last_sse_event_type: None,
                            },
                            &upstream_request_id,
                            &upstream_cf_ray,
                            &upstream_auth_error,
                            &upstream_identity_error_code,
                            &upstream_content_type,
                            None,
                        ));
                    }
                    if synthesized_response {
                        headers.retain(|header| {
                            !header
                                .field
                                .as_str()
                                .as_str()
                                .eq_ignore_ascii_case("Content-Type")
                        });
                        if let Ok(content_type_header) = Header::from_bytes(
                            b"Content-Type".as_slice(),
                            b"application/json".as_slice(),
                        ) {
                            headers.push(content_type_header);
                        }
                    }
                    if status.0 < 400
                        && is_compact_request
                        && !compact_success_body_is_valid(body.as_ref())
                    {
                        return Ok(respond_invalid_compact_success_body(
                            request,
                            usage,
                            body.as_ref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    if is_compact_request
                        && compact_non_success_body_should_be_normalized(
                            status.0,
                            upstream_content_type.as_deref(),
                            body.as_ref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        )
                    {
                        return Ok(respond_invalid_compact_non_success_body(
                            request,
                            status.0,
                            usage,
                            body.as_ref(),
                            upstream_content_type.as_deref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    if status.0 >= 400
                        && non_success_body_should_be_normalized(
                            status.0,
                            upstream_content_type.as_deref(),
                            body.as_ref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        )
                    {
                        return Ok(respond_normalized_passthrough_non_success_body(
                            request,
                            usage,
                            body.as_ref(),
                            upstream_content_type.as_deref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    let len = Some(body.len());
                    let response =
                        Response::new(status, headers, std::io::Cursor::new(body), len, None);
                    let delivery_error = request.respond(response).err().map(|err| err.to_string());
                    return Ok(with_bridge_debug_meta(
                        UpstreamResponseBridgeResult {
                            usage,
                            stream_terminal_seen: true,
                            stream_terminal_error: None,
                            delivery_error,
                            upstream_error_hint,
                            delivered_status_code: None,
                            upstream_request_id: None,
                            upstream_cf_ray: None,
                            upstream_auth_error: None,
                            upstream_identity_error_code: None,
                            upstream_content_type: None,
                            last_sse_event_type: None,
                        },
                        &upstream_request_id,
                        &upstream_cf_ray,
                        &upstream_auth_error,
                        &upstream_identity_error_code,
                        &upstream_content_type,
                        None,
                    ));
                }

                let (_, sse_usage) = collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                let usage = if is_json {
                    serde_json::from_slice::<Value>(upstream_body.as_ref())
                        .ok()
                        .map(|value| parse_usage_from_json(&value))
                        .unwrap_or_default()
                } else if usage_has_signal(&sse_usage) {
                    sse_usage
                } else {
                    UpstreamResponseUsage::default()
                };
                if status.0 >= 400
                    && non_success_body_should_be_normalized(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                {
                    return Ok(respond_normalized_passthrough_non_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                let upstream_error_hint = with_upstream_debug_suffix(
                    extract_error_hint_from_body(status.0, upstream_body.as_ref()),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                );
                if should_suppress_deactivation_delivery(
                    upstream_error_hint.as_deref(),
                    _allow_failover_for_deactivation,
                ) {
                    return Ok(with_bridge_debug_meta(
                        UpstreamResponseBridgeResult {
                            usage,
                            stream_terminal_seen: true,
                            stream_terminal_error: None,
                            delivery_error: None,
                            upstream_error_hint,
                            delivered_status_code: None,
                            upstream_request_id: None,
                            upstream_cf_ray: None,
                            upstream_auth_error: None,
                            upstream_identity_error_code: None,
                            upstream_content_type: None,
                            last_sse_event_type: None,
                        },
                        &upstream_request_id,
                        &upstream_cf_ray,
                        &upstream_auth_error,
                        &upstream_identity_error_code,
                        &upstream_content_type,
                        None,
                    ));
                }
                let len = Some(upstream_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(upstream_body.to_vec()),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage,
                        stream_terminal_seen: true,
                        stream_terminal_error: None,
                        delivery_error,
                        upstream_error_hint,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    None,
                ));
            }

            if is_stream && !is_sse && status.0 >= 400 {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage = UpstreamResponseUsage::default();
                if non_success_body_should_be_normalized(
                    status.0,
                    upstream_content_type.as_deref(),
                    upstream_body.as_ref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                ) {
                    return Ok(respond_normalized_passthrough_non_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                let upstream_error_hint = with_upstream_debug_suffix(
                    extract_error_hint_from_body(status.0, upstream_body.as_ref()),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                );
                let len = Some(upstream_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(upstream_body.to_vec()),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage,
                        stream_terminal_seen: true,
                        stream_terminal_error: None,
                        delivery_error,
                        upstream_error_hint,
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    None,
                ));
            }

            if is_sse || is_stream {
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    if request_path.starts_with("/v1/responses") {
                        Box::new(OpenAIResponsesPassthroughSseReader::from_stream_response(
                            upstream,
                            Arc::clone(&usage_collector),
                            keepalive_frame,
                            request_started_at,
                        ))
                    } else {
                        return Err(format!(
                            "stream upstream response is not supported for path {request_path}"
                        ));
                    };
                force_openai_responses_stream_content_type(&mut headers, request_path, is_stream);
                let response = Response::new(status, headers, response_body, None, None);
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                let collector = usage_collector
                    .lock()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                let last_sse_event_type = collector.last_event_type.clone();
                let result = with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage: collector.usage,
                        stream_terminal_seen: collector.saw_terminal,
                        stream_terminal_error: collector.terminal_error,
                        delivery_error,
                        upstream_error_hint: with_upstream_debug_suffix(
                            collector.upstream_error_hint,
                            None,
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        ),
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    last_sse_event_type,
                );
                log_bridge_stream_diagnostics(response_adapter, request_path, &result);
                return Ok(result);
            }

            let upstream_body = upstream
                .read_all_bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let len = Some(upstream_body.len());
            let response = Response::new(
                status,
                headers,
                std::io::Cursor::new(upstream_body.to_vec()),
                len,
                None,
            );
            let delivery_error = request.respond(response).err().map(|err| err.to_string());
            Ok(with_bridge_debug_meta(
                UpstreamResponseBridgeResult {
                    usage: UpstreamResponseUsage::default(),
                    stream_terminal_seen: true,
                    stream_terminal_error: None,
                    delivery_error,
                    upstream_error_hint: None,
                    delivered_status_code: None,
                    upstream_request_id: None,
                    upstream_cf_ray: None,
                    upstream_auth_error: None,
                    upstream_identity_error_code: None,
                    upstream_content_type: None,
                    last_sse_event_type: None,
                },
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
                None,
            ))
        }
        _ => Err(format!(
            "stream upstream response is not supported for adapter {response_adapter:?}"
        )),
    }
}

/// 函数 `resolve_stream_keepalive_frame`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - response_adapter: 参数 response_adapter
/// - request_path: 参数 request_path
///
/// # 返回
/// 返回函数执行结果
fn resolve_stream_keepalive_frame(
    response_adapter: ResponseAdapter,
    request_path: &str,
) -> SseKeepAliveFrame {
    match response_adapter {
        ResponseAdapter::Passthrough => {
            if request_path.starts_with("/v1/responses") {
                SseKeepAliveFrame::OpenAIResponses
            } else {
                SseKeepAliveFrame::Comment
            }
        }
        ResponseAdapter::OpenAIChatCompletionsSse => SseKeepAliveFrame::OpenAIChatCompletions,
        ResponseAdapter::OpenAICompletionsSse => SseKeepAliveFrame::OpenAICompletions,
        ResponseAdapter::AnthropicSse => SseKeepAliveFrame::Anthropic,
        ResponseAdapter::GeminiSse | ResponseAdapter::GeminiCliSse => SseKeepAliveFrame::Comment,
        ResponseAdapter::OpenAIChatCompletionsJson
        | ResponseAdapter::OpenAICompletionsJson
        | ResponseAdapter::AnthropicJson
        | ResponseAdapter::GeminiJson
        | ResponseAdapter::GeminiCliJson => SseKeepAliveFrame::Comment,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        classify_compact_non_success_kind, compact_non_success_body_should_be_normalized,
        force_openai_responses_stream_content_type, Header,
    };

    /// 函数 `compact_header_only_identity_error_is_normalized_and_classified`
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
    fn compact_header_only_identity_error_is_normalized_and_classified() {
        assert!(compact_non_success_body_should_be_normalized(
            403,
            Some("text/plain"),
            b"",
            None,
            Some("org_membership_required"),
        ));
        assert_eq!(
            classify_compact_non_success_kind(
                403,
                Some("text/plain"),
                b"",
                None,
                None,
                Some("org_membership_required"),
            ),
            "identity_error"
        );
    }

    /// 函数 `compact_header_only_cf_ray_is_classified_as_cloudflare_edge`
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
    fn compact_header_only_cf_ray_is_classified_as_cloudflare_edge() {
        assert_eq!(
            classify_compact_non_success_kind(
                502,
                Some("text/plain"),
                b"",
                Some("ray_compact_edge"),
                None,
                None,
            ),
            "cloudflare_edge"
        );
    }

    #[test]
    fn streaming_responses_passthrough_forces_sse_content_type() {
        let mut headers = vec![
            Header::from_bytes(
                b"Content-Type".as_slice(),
                b"application/json; charset=utf-8".as_slice(),
            )
            .expect("content-type header"),
            Header::from_bytes(b"x-request-id".as_slice(), b"req_test".as_slice())
                .expect("request id header"),
        ];

        force_openai_responses_stream_content_type(&mut headers, "/v1/responses", true);

        let content_type = headers
            .iter()
            .find(|header| {
                header
                    .field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("Content-Type")
            })
            .map(|header| header.value.as_str());
        assert_eq!(content_type, Some("text/event-stream"));
        assert!(headers
            .iter()
            .any(|header| header.field.as_str().as_str() == "x-request-id"));
    }
}
