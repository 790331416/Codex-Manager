use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response as AxumResponse};
use codexmanager_core::rpc::types::{
    JsonRpcError, JsonRpcErrorObject, JsonRpcMessage, JsonRpcRequest, JsonRpcResponse,
};
use std::panic::AssertUnwindSafe;
use tiny_http::Request;
use tiny_http::Response;
use url::Url;

/// 函数 `rpc_response_failed`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - resp: 参数 resp
///
/// # 返回
/// 返回函数执行结果
fn rpc_response_failed(resp: &codexmanager_core::rpc::types::JsonRpcResponse) -> bool {
    if resp.result.get("error").is_some() {
        return true;
    }
    matches!(
        resp.result.get("ok").and_then(|value| value.as_bool()),
        Some(false)
    )
}

/// 函数 `get_header_value`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - request: 参数 request
/// - name: 参数 name
///
/// # 返回
/// 返回函数执行结果
fn get_header_value<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers()
        .iter()
        .find(|header| header.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|header| header.value.as_str().trim())
        .filter(|value| !value.is_empty())
}

/// 函数 `is_json_content_type`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - request: 参数 request
///
/// # 返回
/// 返回函数执行结果
fn is_json_content_type(request: &Request) -> bool {
    get_header_value(request, "Content-Type")
        .and_then(|value| value.split(';').next())
        .map(|value| value.trim().eq_ignore_ascii_case("application/json"))
        .unwrap_or(false)
}

fn rpc_actor_from_request_headers(request: &Request) -> crate::RpcActor {
    crate::RpcActor::from_parts(
        get_header_value(request, "X-CodexManager-Rpc-Actor-Role"),
        get_header_value(request, "X-CodexManager-Rpc-Actor-User-Id"),
    )
}

fn rpc_actor_from_axum_headers(headers: &HeaderMap) -> crate::RpcActor {
    let role = headers
        .get("X-CodexManager-Rpc-Actor-Role")
        .and_then(|value| value.to_str().ok());
    let user_id = headers
        .get("X-CodexManager-Rpc-Actor-User-Id")
        .and_then(|value| value.to_str().ok());
    crate::RpcActor::from_parts(role, user_id)
}

/// 函数 `is_loopback_origin`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - origin: 参数 origin
///
/// # 返回
/// 返回函数执行结果
fn is_loopback_origin(origin: &str) -> bool {
    let Ok(url) = Url::parse(origin) else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
}

/// 函数 `panic_payload_message`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - payload: 参数 payload
///
/// # 返回
/// 返回函数执行结果
fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_string()
}

/// 函数 `jsonrpc_message_success`
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
fn jsonrpc_message_success(message: &JsonRpcMessage) -> bool {
    match message {
        JsonRpcMessage::Response(resp) => !rpc_response_failed(resp),
        JsonRpcMessage::Notification(_) => true,
        JsonRpcMessage::Error(_) => false,
        JsonRpcMessage::Request(_) => true,
    }
}

/// 函数 `handle_parsed_rpc_request`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - req: 参数 req
/// - handler: 参数 handler
///
/// # 返回
/// 返回函数执行结果
fn handle_parsed_rpc_request<F>(req: JsonRpcRequest, handler: F) -> (String, bool)
where
    F: FnOnce(JsonRpcRequest) -> JsonRpcMessage,
{
    let request_id = req.id.clone();
    let request_method = req.method.clone();
    match std::panic::catch_unwind(AssertUnwindSafe(|| handler(req))) {
        Ok(message) => {
            let success = jsonrpc_message_success(&message);
            let json = match message {
                JsonRpcMessage::Notification(_) => String::new(),
                _ => serde_json::to_string(&message).unwrap_or_else(|_| "{}".to_string()),
            };
            (json, success)
        }
        Err(payload) => {
            let panic_message = panic_payload_message(payload.as_ref());
            log::error!(
                "rpc handler panicked: method={} id={} panic={}",
                request_method,
                request_id,
                panic_message
            );
            let message = JsonRpcMessage::Error(JsonRpcError {
                id: request_id,
                error: JsonRpcErrorObject {
                    code: -32603,
                    data: None,
                    message: format!("internal_error: {panic_message}"),
                },
            });
            let json = serde_json::to_string(&message).unwrap_or_else(|_| "{}".to_string());
            (json, false)
        }
    }
}

/// 函数 `handle_rpc_body`
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
fn handle_rpc_body(body: &str, actor: crate::RpcActor) -> (u16, String, bool) {
    if body.trim().is_empty() {
        return (400, "{}".to_string(), false);
    }

    let msg: JsonRpcMessage = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return (400, "{}".to_string(), false),
    };
    let (json, success) = match msg {
        JsonRpcMessage::Request(req) => {
            handle_parsed_rpc_request(req, |req| crate::handle_request_with_actor(req, actor))
        }
        JsonRpcMessage::Notification(_) => (String::new(), true),
        JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_) => {
            return (400, "{}".to_string(), false)
        }
    };
    (200, json, success)
}

/// 函数 `is_axum_json_content_type`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - headers: 参数 headers
///
/// # 返回
/// 返回函数执行结果
fn is_axum_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(|value| value.trim().eq_ignore_ascii_case("application/json"))
        .unwrap_or(false)
}

/// 函数 `validate_axum_headers`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - headers: 参数 headers
///
/// # 返回
/// 返回函数执行结果
fn validate_axum_headers(headers: &HeaderMap) -> Option<AxumResponse> {
    if !is_axum_json_content_type(headers) {
        return Some((StatusCode::UNSUPPORTED_MEDIA_TYPE, "{}").into_response());
    }

    match headers
        .get("X-CodexManager-Rpc-Token")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(token) => {
            if !crate::rpc_auth_token_matches(token) {
                return Some((StatusCode::UNAUTHORIZED, "{}").into_response());
            }
        }
        None => return Some((StatusCode::UNAUTHORIZED, "{}").into_response()),
    }

    if let Some(fetch_site) = headers
        .get("Sec-Fetch-Site")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
    {
        if fetch_site.eq_ignore_ascii_case("cross-site") {
            return Some((StatusCode::FORBIDDEN, "{}").into_response());
        }
    }
    if let Some(origin) = headers
        .get("Origin")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
    {
        if !is_loopback_origin(origin) {
            return Some((StatusCode::FORBIDDEN, "{}").into_response());
        }
    }

    None
}

/// 函数 `handle_rpc_http`
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
pub(crate) async fn handle_rpc_http(headers: HeaderMap, body: String) -> AxumResponse {
    let mut rpc_metrics_guard = crate::gateway::begin_rpc_request();
    if let Some(response) = validate_axum_headers(&headers) {
        return response;
    }
    let actor = rpc_actor_from_axum_headers(&headers);
    let body_for_task = body;
    let (status, response_body, success) =
        match tokio::task::spawn_blocking(move || handle_rpc_body(&body_for_task, actor)).await {
            Ok(result) => result,
            Err(err) => {
                log::error!("rpc http blocking task failed: {}", err);
                let fallback = JsonRpcResponse {
                    id: 0.into(),
                    result: crate::error_codes::rpc_error_payload(
                        "internal_error: rpc task failed".to_string(),
                    ),
                };
                let body = serde_json::to_string(&fallback).unwrap_or_else(|_| "{}".to_string());
                (200, body, false)
            }
        };
    if success {
        rpc_metrics_guard.mark_success();
    }
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
        response_body,
    )
        .into_response()
}

/// 函数 `handle_rpc`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - request: 参数 request
///
/// # 返回
/// 无
pub fn handle_rpc(mut request: Request) {
    let mut rpc_metrics_guard = crate::gateway::begin_rpc_request();
    if request.method().as_str() != "POST" {
        let _ = request.respond(Response::from_string("{}").with_status_code(405));
        return;
    }
    if !is_json_content_type(&request) {
        let _ = request.respond(Response::from_string("{}").with_status_code(415));
        return;
    }

    match get_header_value(&request, "X-CodexManager-Rpc-Token") {
        Some(token) => {
            if !crate::rpc_auth_token_matches(token) {
                let _ = request.respond(Response::from_string("{}").with_status_code(401));
                return;
            }
        }
        None => {
            let _ = request.respond(Response::from_string("{}").with_status_code(401));
            return;
        }
    }

    if let Some(fetch_site) = get_header_value(&request, "Sec-Fetch-Site") {
        if fetch_site.eq_ignore_ascii_case("cross-site") {
            let _ = request.respond(Response::from_string("{}").with_status_code(403));
            return;
        }
    }
    if let Some(origin) = get_header_value(&request, "Origin") {
        if !is_loopback_origin(origin) {
            let _ = request.respond(Response::from_string("{}").with_status_code(403));
            return;
        }
    }

    let actor = rpc_actor_from_request_headers(&request);
    let mut body = String::new();
    if request.as_reader().read_to_string(&mut body).is_err() {
        let _ = request.respond(Response::from_string("{}").with_status_code(400));
        return;
    }
    if body.trim().is_empty() {
        let _ = request.respond(Response::from_string("{}").with_status_code(400));
        return;
    }

    let (status, response_body, success) = handle_rpc_body(&body, actor);
    if success {
        rpc_metrics_guard.mark_success();
    }
    let _ = request.respond(Response::from_string(response_body).with_status_code(status));
}

#[cfg(test)]
mod tests {
    use super::handle_parsed_rpc_request;
    use codexmanager_core::rpc::types::{
        JsonRpcMessage, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
    };

    /// 函数 `panicking_rpc_handler_returns_structured_json_error`
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
    fn panicking_rpc_handler_returns_structured_json_error() {
        let request = JsonRpcRequest {
            id: 7.into(),
            method: "account/usage/refresh".to_string(),
            params: None,
            trace: None,
        };

        let (body, success) = handle_parsed_rpc_request(request, |_req| {
            panic!("usage refresh boom");
        });

        assert!(!success);

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(parsed.get("id").and_then(|value| value.as_u64()), Some(7));
        assert_eq!(
            parsed
                .get("error")
                .and_then(|value| value.get("message"))
                .and_then(|value| value.as_str()),
            Some("internal_error: usage refresh boom")
        );
        assert_eq!(
            parsed
                .get("error")
                .and_then(|value| value.get("code"))
                .and_then(|value| value.as_i64()),
            Some(-32603)
        );
    }

    /// 函数 `normal_rpc_handler_keeps_success_shape`
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
    fn normal_rpc_handler_keeps_success_shape() {
        let request = JsonRpcRequest {
            id: 9.into(),
            method: "noop".to_string(),
            params: None,
            trace: None,
        };

        let (body, success) = handle_parsed_rpc_request(request, |req| {
            JsonRpcMessage::Response(JsonRpcResponse {
                id: req.id,
                result: serde_json::json!({ "ok": true }),
            })
        });

        assert!(success);
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(parsed.get("id").and_then(|value| value.as_u64()), Some(9));
        assert_eq!(
            parsed
                .get("result")
                .and_then(|value| value.get("ok"))
                .and_then(|value| value.as_bool()),
            Some(true)
        );
    }

    /// 函数 `notification_handler_returns_empty_body`
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
    fn notification_handler_returns_empty_body() {
        let request = JsonRpcRequest {
            id: 11.into(),
            method: "noop".to_string(),
            params: None,
            trace: None,
        };

        let (body, success) = handle_parsed_rpc_request(request, |_req| {
            JsonRpcMessage::Notification(JsonRpcNotification {
                method: "initialized".to_string(),
                params: None,
            })
        });

        assert!(success);
        assert!(body.is_empty());
    }
}
