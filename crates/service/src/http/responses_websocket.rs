use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::FromRequestParts;
use axum::http::header::{self, HeaderMap, HeaderValue};
use axum::http::{Request as HttpRequest, Response, StatusCode};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use std::time::Instant;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;

use crate::http::proxy_response::{text_error_response, text_response};
use crate::storage_helpers::{hash_platform_key, open_storage};

const RESPONSES_PATH: &str = "/v1/responses";
const RESPONSES_WS_BETA_HEADER_VALUE: &str = "responses_websockets=2026-02-06";
const RESPONSES_WS_ERROR_CODE: &str = "responses_websocket_error";
const OPENAI_ORGANIZATION_ENV: &str = "OPENAI_ORGANIZATION";
const OPENAI_PROJECT_ENV: &str = "OPENAI_PROJECT";

#[derive(Clone)]
struct WsRequestContext {
    api_key: codexmanager_core::storage::ApiKey,
    incoming_headers: crate::gateway::IncomingHeaderSnapshot,
    prompt_cache_key: Option<String>,
    effective_upstream_base: String,
    include_timing_metrics: bool,
    prefer_raw_errors: bool,
}

struct PreparedClientFrame {
    text: String,
    model: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    effective_service_tier: Option<String>,
    raw_service_tier: Option<String>,
    has_service_tier_field: bool,
}

struct ConnectedUpstreamWebsocket {
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    account_id: String,
    upstream_url: String,
}

struct PendingWsRequestLog {
    trace_id: String,
    model: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    effective_service_tier: Option<String>,
    started_at: Instant,
}

struct WsSessionError {
    status: u16,
    code: String,
    message: String,
}

impl WsSessionError {
    fn new(status: u16, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, "invalid_request_error", message)
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(502, RESPONSES_WS_ERROR_CODE, message)
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self::new(503, RESPONSES_WS_ERROR_CODE, message)
    }

    fn bad_request_bilingual(
        chinese_description: impl AsRef<str>,
        english_raw_message: impl AsRef<str>,
    ) -> Self {
        Self::bad_request(crate::gateway::bilingual_error(
            chinese_description,
            english_raw_message,
        ))
    }

    fn bad_gateway_bilingual(
        chinese_description: impl AsRef<str>,
        english_raw_message: impl AsRef<str>,
    ) -> Self {
        Self::bad_gateway(crate::gateway::bilingual_error(
            chinese_description,
            english_raw_message,
        ))
    }

    fn service_unavailable_bilingual(
        chinese_description: impl AsRef<str>,
        english_raw_message: impl AsRef<str>,
    ) -> Self {
        Self::service_unavailable(crate::gateway::bilingual_error(
            chinese_description,
            english_raw_message,
        ))
    }
}

/// 函数 `is_websocket_upgrade_request`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-05
///
/// # 参数
/// - headers: 参数 headers
///
/// # 返回
/// 返回函数执行结果
pub(super) fn is_websocket_upgrade_request(headers: &HeaderMap) -> bool {
    let upgrade_is_websocket = headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
    let connection_has_upgrade = headers
        .get(header::CONNECTION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        });
    upgrade_is_websocket && connection_has_upgrade
}

/// 函数 `upgrade_responses_websocket`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-05
///
/// # 参数
/// - request: 参数 request
///
/// # 返回
/// 返回函数执行结果
pub(super) async fn upgrade_responses_websocket(request: HttpRequest<Body>) -> Response<Body> {
    let (mut parts, _) = request.into_parts();

    let context = match authorize_websocket_request(&parts.headers) {
        Ok(context) => context,
        Err(response) => return response,
    };

    let ws = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(ws) => ws,
        Err(err) => {
            return text_error_response(
                StatusCode::BAD_REQUEST,
                crate::gateway::error_message_for_client(
                    context.prefer_raw_errors,
                    crate::gateway::bilingual_error(
                        "WebSocket 升级失败",
                        format!("websocket upgrade rejected: {err}"),
                    ),
                ),
            );
        }
    };

    ws.on_upgrade(move |socket| async move {
        run_responses_websocket_session(socket, context).await;
    })
}

async fn run_responses_websocket_session(mut socket: WebSocket, context: WsRequestContext) {
    let first_text = match receive_initial_request(&mut socket).await {
        Ok(Some(text)) => text,
        Ok(None) => return,
        Err(err) => {
            send_ws_error_and_close(&mut socket, err, context.prefer_raw_errors).await;
            return;
        }
    };

    let prepared_first = match rewrite_client_frame(first_text.as_str(), &context) {
        Ok(prepared) => prepared,
        Err(err) => {
            send_ws_error_and_close(&mut socket, err, context.prefer_raw_errors).await;
            return;
        }
    };

    let mut upstream =
        match connect_upstream_websocket(&context, prepared_first.model.as_deref()).await {
            Ok(stream) => stream,
            Err(err) => {
                send_ws_error_and_close(&mut socket, err, context.prefer_raw_errors).await;
                return;
            }
        };
    let first_pending = begin_ws_request_log(&context, &prepared_first);

    if let Err(err) = upstream
        .stream
        .send(UpstreamMessage::Text(prepared_first.text.clone().into()))
        .await
    {
        finalize_ws_request_log(
            &context,
            &first_pending,
            Some(upstream.account_id.as_str()),
            Some(upstream.upstream_url.as_str()),
            502,
            crate::gateway::RequestLogUsage::default(),
            Some(crate::gateway::bilingual_error(
                "发送上游 WebSocket 首帧失败",
                format!("send first upstream websocket frame failed: {err}"),
            )),
        );
        send_ws_error_and_close(
            &mut socket,
            WsSessionError::bad_gateway_bilingual(
                "发送上游 WebSocket 首帧失败",
                format!("send first upstream websocket frame failed: {err}"),
            ),
            context.prefer_raw_errors,
        )
        .await;
        return;
    }
    let mut pending_request = Some(first_pending);

    loop {
        tokio::select! {
            maybe_client = socket.recv() => {
                let Some(client_result) = maybe_client else {
                    if let Some(pending) = pending_request.take() {
                        finalize_ws_request_log(
                            &context,
                            &pending,
                            Some(upstream.account_id.as_str()),
                            Some(upstream.upstream_url.as_str()),
                            499,
                            crate::gateway::RequestLogUsage::default(),
                            Some(crate::gateway::bilingual_error(
                                "客户端 WebSocket 在完成前关闭",
                                "client websocket closed before completion",
                            )),
                        );
                    }
                    let _ = upstream.stream.close(None).await;
                    break;
                };
                match client_result {
                    Ok(Message::Text(text)) => {
                        match rewrite_client_frame(text.as_str(), &context) {
                            Ok(prepared) => {
                                if let Some(previous_pending) = pending_request.take() {
                                    finalize_ws_request_log(
                                        &context,
                                        &previous_pending,
                                        Some(upstream.account_id.as_str()),
                                        Some(upstream.upstream_url.as_str()),
                                        499,
                                        crate::gateway::RequestLogUsage::default(),
                                        Some(crate::gateway::bilingual_error(
                                            "WebSocket 请求在完成前被覆盖",
                                            "websocket request superseded before completion",
                                        )),
                                    );
                                }
                                let current_pending = begin_ws_request_log(&context, &prepared);
                                if let Err(err) = upstream.stream.send(UpstreamMessage::Text(prepared.text.into())).await {
                                    finalize_ws_request_log(
                                        &context,
                                        &current_pending,
                                        Some(upstream.account_id.as_str()),
                                        Some(upstream.upstream_url.as_str()),
                                        502,
                                        crate::gateway::RequestLogUsage::default(),
                                        Some(crate::gateway::bilingual_error(
                                            "发送上游 WebSocket 帧失败",
                                            format!("send upstream websocket frame failed: {err}"),
                                        )),
                                    );
                                    send_ws_error_and_close(
                                        &mut socket,
                                        WsSessionError::bad_gateway_bilingual(
                                            "发送上游 WebSocket 帧失败",
                                            format!("send upstream websocket frame failed: {err}"),
                                        ),
                                        context.prefer_raw_errors,
                                    ).await;
                                    let _ = upstream.stream.close(None).await;
                                    break;
                                }
                                pending_request = Some(current_pending);
                            }
                            Err(err) => {
                                send_ws_error_and_close(&mut socket, err, context.prefer_raw_errors).await;
                                let _ = upstream.stream.close(None).await;
                                break;
                            }
                        }
                    }
                    Ok(Message::Binary(bytes)) => {
                        if let Err(err) = upstream.stream.send(UpstreamMessage::Binary(bytes)).await {
                            send_ws_error_and_close(
                                &mut socket,
                                WsSessionError::bad_gateway_bilingual(
                                    "发送上游 WebSocket 二进制消息失败",
                                    format!("send upstream websocket binary failed: {err}"),
                                ),
                                context.prefer_raw_errors,
                            ).await;
                            break;
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if let Err(err) = upstream.stream.send(UpstreamMessage::Ping(payload)).await {
                            send_ws_error_and_close(
                                &mut socket,
                                WsSessionError::bad_gateway_bilingual(
                                    "转发 WebSocket Ping 失败",
                                    format!("forward websocket ping failed: {err}"),
                                ),
                                context.prefer_raw_errors,
                            ).await;
                            break;
                        }
                    }
                    Ok(Message::Pong(payload)) => {
                        if let Err(err) = upstream.stream.send(UpstreamMessage::Pong(payload)).await {
                            send_ws_error_and_close(
                                &mut socket,
                                WsSessionError::bad_gateway_bilingual(
                                    "转发 WebSocket Pong 失败",
                                    format!("forward websocket pong failed: {err}"),
                                ),
                                context.prefer_raw_errors,
                            ).await;
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        if let Some(pending) = pending_request.take() {
                            finalize_ws_request_log(
                                &context,
                                &pending,
                                Some(upstream.account_id.as_str()),
                                Some(upstream.upstream_url.as_str()),
                                499,
                                crate::gateway::RequestLogUsage::default(),
                                Some(crate::gateway::bilingual_error(
                                    "客户端 WebSocket 在完成前关闭",
                                    "client websocket closed before completion",
                                )),
                            );
                        }
                        let _ = upstream.stream.close(None).await;
                        break;
                    }
                    Err(err) => {
                        if let Some(pending) = pending_request.take() {
                            finalize_ws_request_log(
                                &context,
                                &pending,
                                Some(upstream.account_id.as_str()),
                                Some(upstream.upstream_url.as_str()),
                                400,
                                crate::gateway::RequestLogUsage::default(),
                                Some(crate::gateway::bilingual_error(
                                    "接收客户端 WebSocket 帧失败",
                                    format!("receive client websocket frame failed: {err}"),
                                )),
                            );
                        }
                        send_ws_error_and_close(
                            &mut socket,
                            WsSessionError::bad_request_bilingual(
                                "接收客户端 WebSocket 帧失败",
                                format!("receive client websocket frame failed: {err}"),
                            ),
                            context.prefer_raw_errors,
                        ).await;
                        let _ = upstream.stream.close(None).await;
                        break;
                    }
                }
            }
            maybe_upstream = upstream.stream.next() => {
                let Some(upstream_result) = maybe_upstream else {
                    if let Some(pending) = pending_request.take() {
                        finalize_ws_request_log(
                            &context,
                            &pending,
                            Some(upstream.account_id.as_str()),
                            Some(upstream.upstream_url.as_str()),
                            502,
                            crate::gateway::RequestLogUsage::default(),
                            Some(crate::gateway::bilingual_error(
                                "上游 WebSocket 在完成前关闭",
                                "upstream websocket closed before completion",
                            )),
                        );
                    }
                    let _ = socket.close().await;
                    break;
                };
        match upstream_result {
                    Ok(UpstreamMessage::Text(text)) => {
                        if let Some(terminal) = inspect_ws_terminal_event(text.as_str()) {
                            let terminal_status = terminal.status_code;
                            if let Some(pending) = pending_request.take() {
                                finalize_ws_request_log(
                                    &context,
                                    &pending,
                                    Some(upstream.account_id.as_str()),
                                    Some(upstream.upstream_url.as_str()),
                                    terminal_status,
                                    terminal.usage,
                                    terminal.error,
                                );
                            }
                            try_rotate_ws_upstream_after_terminal(
                                &context,
                                &mut upstream,
                                prepared_first.model.as_deref(),
                                terminal_status,
                            )
                            .await;
                        }
                        if socket
                            .send(Message::Text(text.to_string().into()))
                            .await
                            .is_err()
                        {
                            let _ = upstream.stream.close(None).await;
                            break;
                        }
                    }
                    Ok(UpstreamMessage::Binary(bytes)) => {
                        if socket.send(Message::Binary(bytes)).await.is_err() {
                            let _ = upstream.stream.close(None).await;
                            break;
                        }
                    }
                    Ok(UpstreamMessage::Ping(payload)) => {
                        if socket.send(Message::Ping(payload)).await.is_err() {
                            let _ = upstream.stream.close(None).await;
                            break;
                        }
                    }
                    Ok(UpstreamMessage::Pong(payload)) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            let _ = upstream.stream.close(None).await;
                            break;
                        }
                    }
                    Ok(UpstreamMessage::Close(_)) => {
                        if let Some(pending) = pending_request.take() {
                            finalize_ws_request_log(
                                &context,
                                &pending,
                                Some(upstream.account_id.as_str()),
                                Some(upstream.upstream_url.as_str()),
                                502,
                                crate::gateway::RequestLogUsage::default(),
                                Some(crate::gateway::bilingual_error(
                                    "上游 WebSocket 在完成前关闭",
                                    "upstream websocket closed before completion",
                                )),
                            );
                        }
                        let _ = socket.close().await;
                        break;
                    }
                    Ok(UpstreamMessage::Frame(_)) => {}
                    Err(err) => {
                        if let Some(pending) = pending_request.take() {
                            finalize_ws_request_log(
                                &context,
                                &pending,
                                Some(upstream.account_id.as_str()),
                                Some(upstream.upstream_url.as_str()),
                                502,
                                crate::gateway::RequestLogUsage::default(),
                                Some(crate::gateway::bilingual_error(
                                    "接收上游 WebSocket 帧失败",
                                    format!("receive upstream websocket frame failed: {err}"),
                                )),
                            );
                        }
                        send_ws_error_and_close(
                            &mut socket,
                            WsSessionError::bad_gateway_bilingual(
                                "接收上游 WebSocket 帧失败",
                                format!("receive upstream websocket frame failed: {err}"),
                            ),
                            context.prefer_raw_errors,
                        ).await;
                        break;
                    }
                }
            }
        }
    }
}

fn authorize_websocket_request(headers: &HeaderMap) -> Result<WsRequestContext, Response<Body>> {
    let prefer_raw_errors = crate::gateway::prefers_raw_errors_for_http_headers(headers);
    let incoming_headers = crate::gateway::IncomingHeaderSnapshot::from_http_headers(headers);
    let Some(platform_key) = incoming_headers.platform_key() else {
        return Err(text_error_response(
            StatusCode::UNAUTHORIZED,
            crate::gateway::error_message_for_client(
                prefer_raw_errors,
                crate::gateway::bilingual_error("缺少平台 API Key", "missing platform api key"),
            ),
        ));
    };

    let storage = open_storage().ok_or_else(|| {
        text_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            crate::gateway::error_message_for_client(
                prefer_raw_errors,
                crate::gateway::bilingual_error("存储不可用", "storage unavailable"),
            ),
        )
    })?;
    let key_hash = hash_platform_key(platform_key);
    let api_key = storage
        .find_api_key_by_hash(&key_hash)
        .map_err(|err| {
            text_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                crate::gateway::error_message_for_client(
                    prefer_raw_errors,
                    crate::gateway::bilingual_error(
                        "读取存储失败",
                        format!("storage read failed: {err}"),
                    ),
                ),
            )
        })?
        .ok_or_else(|| {
            text_error_response(
                StatusCode::FORBIDDEN,
                crate::gateway::error_message_for_client(
                    prefer_raw_errors,
                    crate::gateway::MISSING_AUTH_JSON_OPENAI_API_KEY_ERROR,
                ),
            )
        })?;

    if api_key.status != "active" {
        return Err(text_error_response(
            StatusCode::FORBIDDEN,
            crate::gateway::error_message_for_client(
                prefer_raw_errors,
                crate::gateway::bilingual_error("API Key 已禁用", "api key disabled"),
            ),
        ));
    }
    if !crate::gateway::gateway_supports_official_responses_websocket(&api_key) {
        return Err(upgrade_required_response(
            crate::gateway::error_message_for_client(
                prefer_raw_errors,
                crate::gateway::bilingual_error(
                    "Responses WebSocket 仅支持官方 Codex 上游",
                    "responses websocket is only available for official Codex upstream",
                ),
            ),
        ));
    }
    let (incoming_headers, prompt_cache_key) =
        crate::gateway::gateway_resolve_ws_prompt_cache_key(&storage, &api_key, &incoming_headers)
            .map_err(|err| {
                text_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    crate::gateway::error_message_for_client(
                        prefer_raw_errors,
                        crate::gateway::bilingual_error("读取会话绑定失败", err),
                    ),
                )
            })?;

    Ok(WsRequestContext {
        effective_upstream_base: crate::gateway::gateway_resolve_effective_upstream_base(&api_key),
        api_key,
        incoming_headers,
        prompt_cache_key,
        include_timing_metrics: parse_bool_header(
            headers.get("x-responsesapi-include-timing-metrics"),
        ),
        prefer_raw_errors,
    })
}

async fn receive_initial_request(socket: &mut WebSocket) -> Result<Option<String>, WsSessionError> {
    loop {
        let Some(message) = socket.recv().await else {
            return Ok(None);
        };
        match message {
            Ok(Message::Text(text)) => return Ok(Some(text.to_string())),
            Ok(Message::Ping(payload)) => {
                let _ = socket.send(Message::Pong(payload)).await;
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Close(_)) => return Ok(None),
            Ok(Message::Binary(_)) => {
                return Err(WsSessionError::bad_request_bilingual(
                    "首个 WebSocket 帧必须是 response.create 文本帧",
                    "initial websocket frame must be a response.create text frame",
                ));
            }
            Err(err) => {
                return Err(WsSessionError::bad_request_bilingual(
                    "接收首个 WebSocket 帧失败",
                    format!("receive initial websocket frame failed: {err}"),
                ));
            }
        }
    }
}

fn rewrite_client_frame(
    text: &str,
    context: &WsRequestContext,
) -> Result<PreparedClientFrame, WsSessionError> {
    let mut payload = serde_json::from_str::<Value>(text).map_err(|err| {
        WsSessionError::bad_request_bilingual(
            "WebSocket JSON 载荷无效",
            format!("invalid websocket json payload: {err}"),
        )
    })?;
    let Some(mut object) = payload.as_object_mut().cloned() else {
        return Err(WsSessionError::bad_request_bilingual(
            "WebSocket 载荷必须是 JSON 对象",
            "websocket payload must be a JSON object",
        ));
    };
    let Some(message_type) = object
        .remove("type")
        .and_then(|value| value.as_str().map(str::to_string))
    else {
        return Err(WsSessionError::bad_request_bilingual(
            "WebSocket 载荷缺少 type=response.create",
            "websocket payload missing type=response.create",
        ));
    };
    if message_type != "response.create" {
        return Err(WsSessionError::bad_request_bilingual(
            "不支持的 WebSocket 消息类型",
            format!("unsupported websocket message type: {message_type}"),
        ));
    }
    let service_tier_diagnostic =
        crate::gateway::inspect_service_tier_value(object.get("service_tier"));
    let explicit_service_tier_for_log = service_tier_diagnostic.normalized_value.clone();
    let previous_response_id = object.remove("previous_response_id");
    let generate = object.remove("generate");
    let client_metadata = merge_turn_metadata(
        object.remove("client_metadata"),
        context.incoming_headers.turn_metadata(),
    );

    let rewritten_body = crate::gateway::gateway_rewrite_ws_responses_body(
        RESPONSES_PATH,
        serde_json::to_vec(&Value::Object(object)).map_err(|err| {
            WsSessionError::bad_request_bilingual(
                "序列化 WebSocket 请求失败",
                format!("serialize websocket request failed: {err}"),
            )
        })?,
        &context.api_key,
        context.prompt_cache_key.as_deref(),
    );
    let rewritten_value = serde_json::from_slice::<Value>(&rewritten_body).map_err(|err| {
        WsSessionError::bad_request_bilingual(
            "解析改写后的 WebSocket 请求失败",
            format!("parse rewritten websocket request failed: {err}"),
        )
    })?;
    let Some(mut rewritten_object) = rewritten_value.as_object().cloned() else {
        return Err(WsSessionError::bad_request_bilingual(
            "改写后的 WebSocket 载荷必须仍为 JSON 对象",
            "rewritten websocket payload must stay a JSON object",
        ));
    };
    let effective_service_tier_for_log =
        crate::gateway::inspect_service_tier_value(rewritten_object.get("service_tier"))
            .normalized_value;
    rewritten_object.insert("type".to_string(), Value::String(message_type));
    if let Some(value) = previous_response_id {
        rewritten_object.insert("previous_response_id".to_string(), value);
    }
    if let Some(value) = generate {
        rewritten_object.insert("generate".to_string(), value);
    }
    if let Some(value) = client_metadata {
        rewritten_object.insert("client_metadata".to_string(), value);
    }

    Ok(PreparedClientFrame {
        model: rewritten_object
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string),
        reasoning_effort: rewritten_object
            .get("reasoning")
            .and_then(|value| value.get("effort"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                rewritten_object
                    .get("reasoning_effort")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }),
        service_tier: explicit_service_tier_for_log,
        effective_service_tier: effective_service_tier_for_log,
        raw_service_tier: service_tier_diagnostic.raw_value,
        has_service_tier_field: service_tier_diagnostic.has_field,
        text: serde_json::to_string(&Value::Object(rewritten_object)).map_err(|err| {
            WsSessionError::bad_request_bilingual(
                "序列化 WebSocket 请求失败",
                format!("serialize websocket request failed: {err}"),
            )
        })?,
    })
}

fn merge_turn_metadata(
    client_metadata: Option<Value>,
    turn_metadata: Option<&str>,
) -> Option<Value> {
    let Some(turn_metadata) = turn_metadata
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return client_metadata;
    };
    match client_metadata {
        Some(Value::Object(mut object)) => {
            object
                .entry("x-codex-turn-metadata".to_string())
                .or_insert_with(|| Value::String(turn_metadata.to_string()));
            Some(Value::Object(object))
        }
        Some(other) => Some(other),
        None => {
            let mut object = Map::new();
            object.insert(
                "x-codex-turn-metadata".to_string(),
                Value::String(turn_metadata.to_string()),
            );
            Some(Value::Object(object))
        }
    }
}

async fn connect_upstream_websocket(
    context: &WsRequestContext,
    model: Option<&str>,
) -> Result<ConnectedUpstreamWebsocket, WsSessionError> {
    let storage = open_storage().ok_or_else(|| {
        WsSessionError::service_unavailable_bilingual("存储不可用", "storage unavailable")
    })?;
    let candidates =
        crate::gateway::gateway_collect_routed_candidates(&storage, &context.api_key.id, model)?;
    if candidates.is_empty() {
        return Err(WsSessionError::service_unavailable_bilingual(
            "没有可用的上游账号",
            "no available upstream accounts",
        ));
    }

    let ws_url = build_upstream_websocket_url(&context.effective_upstream_base)?;
    let mut last_error = None;
    ensure_rustls_crypto_provider();
    for (account, token) in candidates {
        let bearer = match resolve_bearer_token_for_websocket(account.clone(), token).await {
            Ok(token) => token,
            Err(err) => {
                last_error = Some(format!(
                    "resolve bearer token for account {} failed: {err}",
                    account.id
                ));
                continue;
            }
        };

        let request = match build_upstream_websocket_request(
            ws_url.as_str(),
            &account,
            bearer.as_str(),
            context,
        ) {
            Ok(request) => request,
            Err(err) => return Err(err),
        };

        match connect_async_tls_with_config(request, None, false, None).await {
            Ok((stream, _)) => {
                return Ok(ConnectedUpstreamWebsocket {
                    stream,
                    account_id: account.id,
                    upstream_url: ws_url.clone(),
                });
            }
            Err(err) => {
                last_error = Some(format!(
                    "connect upstream websocket for account {} failed: {err}",
                    account.id
                ));
            }
        }
    }

    Err(WsSessionError::bad_gateway_bilingual(
        "连接上游 WebSocket 失败",
        last_error.unwrap_or_else(|| "connect upstream websocket failed".to_string()),
    ))
}

async fn resolve_bearer_token_for_websocket(
    account: codexmanager_core::storage::Account,
    token: codexmanager_core::storage::Token,
) -> Result<String, String> {
    let join_result = tokio::task::spawn_blocking(move || {
        let storage = open_storage()
            .ok_or_else(|| crate::gateway::bilingual_error("存储不可用", "storage unavailable"))?;
        let mut token = token;
        crate::gateway::gateway_resolve_openai_bearer_token(&storage, &account, &mut token)
    })
    .await;

    match join_result {
        Ok(result) => result,
        Err(err) => Err(crate::gateway::bilingual_error(
            "Bearer Token 任务合并失败",
            format!("bearer token task join failed: {err}"),
        )),
    }
}

fn build_upstream_websocket_url(upstream_base: &str) -> Result<String, WsSessionError> {
    let (target_url, _) =
        crate::gateway::gateway_compute_upstream_url(upstream_base, RESPONSES_PATH);
    let mut url = url::Url::parse(target_url.as_str()).map_err(|err| {
        WsSessionError::bad_gateway_bilingual(
            "上游 WebSocket URL 无效",
            format!("invalid upstream websocket url: {err}"),
        )
    })?;
    match url.scheme() {
        "http" => {
            let _ = url.set_scheme("ws");
        }
        "https" => {
            let _ = url.set_scheme("wss");
        }
        "ws" | "wss" => {}
        other => {
            return Err(WsSessionError::bad_gateway_bilingual(
                "不支持的上游 WebSocket 协议",
                format!("unsupported upstream websocket scheme: {other}"),
            ));
        }
    }
    Ok(url.to_string())
}

fn build_upstream_websocket_request(
    ws_url: &str,
    account: &codexmanager_core::storage::Account,
    bearer_token: &str,
    context: &WsRequestContext,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, WsSessionError> {
    let mut request = ws_url.into_client_request().map_err(|err| {
        WsSessionError::bad_gateway_bilingual(
            "构建上游 WebSocket 请求失败",
            format!("build upstream websocket request failed: {err}"),
        )
    })?;
    let headers = request.headers_mut();
    insert_header(headers, "Authorization", &format!("Bearer {bearer_token}"))?;
    if let Some(account_id) = account
        .chatgpt_account_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        insert_header(headers, "ChatGPT-Account-ID", account_id)?;
    }
    insert_header(
        headers,
        "User-Agent",
        &crate::gateway::current_codex_user_agent(),
    )?;
    insert_header(
        headers,
        "version",
        &crate::gateway::current_codex_user_agent_version(),
    )?;
    append_optional_env_header(headers, "OpenAI-Organization", OPENAI_ORGANIZATION_ENV)?;
    append_optional_env_header(headers, "OpenAI-Project", OPENAI_PROJECT_ENV)?;
    insert_header(
        headers,
        "originator",
        &crate::gateway::current_wire_originator(),
    )?;
    insert_header(headers, "OpenAI-Beta", RESPONSES_WS_BETA_HEADER_VALUE)?;
    if let Some(residency_requirement) = crate::gateway::current_residency_requirement() {
        insert_header(
            headers,
            "x-openai-internal-codex-residency",
            residency_requirement.as_str(),
        )?;
    }
    if let Some(session_id) = context.incoming_headers.session_id() {
        insert_header(headers, "session_id", session_id)?;
    }
    if let Some(window_id) = context.incoming_headers.window_id() {
        insert_header(headers, "x-codex-window-id", window_id)?;
    } else if let Some(session_id) = context.incoming_headers.session_id() {
        let fallback_window_id = format!("{session_id}:0");
        insert_header(headers, "x-codex-window-id", fallback_window_id.as_str())?;
    }
    if let Some(client_request_id) = context.incoming_headers.client_request_id() {
        insert_header(headers, "x-client-request-id", client_request_id)?;
    }
    if let Some(subagent) = context.incoming_headers.subagent() {
        insert_header(headers, "x-openai-subagent", subagent)?;
    }
    if let Some(parent_thread_id) = context.incoming_headers.parent_thread_id() {
        insert_header(headers, "x-codex-parent-thread-id", parent_thread_id)?;
    }
    if let Some(beta_features) = context.incoming_headers.beta_features() {
        insert_header(headers, "x-codex-beta-features", beta_features)?;
    }
    if let Some(turn_metadata) = context.incoming_headers.turn_metadata() {
        insert_header(headers, "x-codex-turn-metadata", turn_metadata)?;
    }
    if let Some(turn_state) = context.incoming_headers.turn_state() {
        insert_header(headers, "x-codex-turn-state", turn_state)?;
    }
    append_passthrough_codex_headers(
        headers,
        context.incoming_headers.passthrough_codex_headers(),
    )?;
    if context.include_timing_metrics {
        insert_header(headers, "x-responsesapi-include-timing-metrics", "true")?;
    }
    Ok(request)
}

fn append_passthrough_codex_headers(
    headers: &mut HeaderMap,
    passthrough_headers: &[(String, String)],
) -> Result<(), WsSessionError> {
    for (name, value) in passthrough_headers {
        if headers.contains_key(name.as_str()) {
            continue;
        }
        insert_header(headers, name, value)?;
    }
    Ok(())
}

fn append_optional_env_header(
    headers: &mut HeaderMap,
    header_name: &str,
    env_name: &str,
) -> Result<(), WsSessionError> {
    if let Some(value) = std::env::var(env_name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        insert_header(headers, header_name, value.as_str())?;
    }
    Ok(())
}

fn ensure_rustls_crypto_provider() {
    static RUSTLS_PROVIDER_READY: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    let _ = RUSTLS_PROVIDER_READY.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn begin_ws_request_log(
    context: &WsRequestContext,
    prepared: &PreparedClientFrame,
) -> PendingWsRequestLog {
    let trace_id = crate::gateway::next_trace_id();
    let effective_protocol_type = crate::apikey_profile::resolve_gateway_protocol_type(
        context.api_key.protocol_type.as_str(),
        RESPONSES_PATH,
    );
    crate::gateway::log_request_start(
        trace_id.as_str(),
        context.api_key.id.as_str(),
        "GET",
        RESPONSES_PATH,
        prepared.model.as_deref(),
        prepared.reasoning_effort.as_deref(),
        prepared.service_tier.as_deref(),
        true,
        "ws",
        effective_protocol_type,
    );
    crate::gateway::log_client_service_tier(
        trace_id.as_str(),
        "ws",
        RESPONSES_PATH,
        prepared.has_service_tier_field,
        prepared.raw_service_tier.as_deref(),
        prepared.service_tier.as_deref(),
    );
    PendingWsRequestLog {
        trace_id,
        model: prepared.model.clone(),
        reasoning_effort: prepared.reasoning_effort.clone(),
        service_tier: prepared.service_tier.clone(),
        effective_service_tier: prepared.effective_service_tier.clone(),
        started_at: Instant::now(),
    }
}

fn finalize_ws_request_log(
    context: &WsRequestContext,
    pending: &PendingWsRequestLog,
    account_id: Option<&str>,
    upstream_url: Option<&str>,
    status_code: u16,
    usage: crate::gateway::RequestLogUsage,
    error: Option<String>,
) {
    let Some(storage) = open_storage() else {
        return;
    };
    crate::gateway::write_request_log(
        &storage,
        crate::gateway::RequestLogTraceContext {
            trace_id: Some(pending.trace_id.as_str()),
            original_path: Some(RESPONSES_PATH),
            adapted_path: Some(RESPONSES_PATH),
            request_type: Some("ws"),
            service_tier: pending.service_tier.as_deref(),
            effective_service_tier: pending.effective_service_tier.as_deref(),
            ..Default::default()
        },
        Some(context.api_key.id.as_str()),
        account_id,
        RESPONSES_PATH,
        "GET",
        pending.model.as_deref(),
        pending.reasoning_effort.as_deref(),
        upstream_url,
        Some(status_code),
        usage,
        error.as_deref(),
        Some(pending.started_at.elapsed().as_millis()),
    );
    crate::gateway::log_request_final(
        pending.trace_id.as_str(),
        status_code,
        account_id,
        upstream_url,
        error.as_deref(),
        pending.started_at.elapsed().as_millis(),
    );
}

struct WsTerminalEvent {
    status_code: u16,
    usage: crate::gateway::RequestLogUsage,
    error: Option<String>,
}

fn should_rotate_ws_upstream(status_code: u16) -> bool {
    matches!(status_code, 401 | 403 | 404 | 408 | 409 | 429)
}

async fn try_rotate_ws_upstream_after_terminal(
    context: &WsRequestContext,
    upstream: &mut ConnectedUpstreamWebsocket,
    model: Option<&str>,
    status_code: u16,
) {
    if !should_rotate_ws_upstream(status_code) {
        return;
    }

    let current_account_id = upstream.account_id.clone();
    crate::gateway::gateway_mark_account_cooldown_for_status(
        current_account_id.as_str(),
        status_code,
    );
    if status_code == 429 {
        let _ =
            crate::usage_refresh::enqueue_usage_refresh_for_account(current_account_id.as_str());
    }

    let storage = match open_storage() {
        Some(storage) => storage,
        None => return,
    };
    let candidates = match crate::gateway::gateway_collect_routed_candidates(
        &storage,
        &context.api_key.id,
        model,
    ) {
        Ok(candidates) => candidates,
        Err(err) => {
            log::warn!(
                "event=responses_ws_failover_candidates_failed account_id={} status={} err={}",
                current_account_id,
                status_code,
                err
            );
            return;
        }
    };
    let replacement_candidate = candidates
        .into_iter()
        .find(|(account, _)| account.id != current_account_id);
    let Some((account, token)) = replacement_candidate else {
        return;
    };

    let ws_url = upstream.upstream_url.clone();
    let bearer = match resolve_bearer_token_for_websocket(account.clone(), token).await {
        Ok(token) => token,
        Err(err) => {
            log::warn!(
                "event=responses_ws_failover_bearer_failed account_id={} next_account_id={} status={} err={}",
                current_account_id,
                account.id,
                status_code,
                err
            );
            return;
        }
    };
    let request = match build_upstream_websocket_request(
        ws_url.as_str(),
        &account,
        bearer.as_str(),
        context,
    ) {
        Ok(request) => request,
        Err(err) => {
            log::warn!(
                "event=responses_ws_failover_request_failed account_id={} next_account_id={} status={} err={}",
                current_account_id,
                account.id,
                status_code,
                err.message
            );
            return;
        }
    };

    ensure_rustls_crypto_provider();
    let replacement = match connect_async_tls_with_config(request, None, false, None).await {
        Ok((stream, _)) => ConnectedUpstreamWebsocket {
            stream,
            account_id: account.id,
            upstream_url: ws_url,
        },
        Err(err) => {
            log::warn!(
                "event=responses_ws_failover_connect_failed account_id={} status={} err={}",
                current_account_id,
                status_code,
                err
            );
            return;
        }
    };

    crate::gateway::gateway_record_failover_attempt();
    let _ = upstream.stream.close(None).await;
    *upstream = replacement;
}

fn inspect_ws_terminal_event(text: &str) -> Option<WsTerminalEvent> {
    let value = serde_json::from_str::<Value>(text).ok()?;
    let event_type = value
        .get("type")
        .and_then(Value::as_str)?
        .trim()
        .to_ascii_lowercase();
    match event_type.as_str() {
        "response.completed" | "response.done" => Some(WsTerminalEvent {
            status_code: 200,
            usage: parse_ws_usage(&value),
            error: None,
        }),
        "response.failed" => Some(WsTerminalEvent {
            status_code: value
                .get("status")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(502),
            usage: parse_ws_usage(&value),
            error: extract_ws_error_message(&value),
        }),
        "error" => Some(WsTerminalEvent {
            status_code: value
                .get("status")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(502),
            usage: crate::gateway::RequestLogUsage::default(),
            error: extract_ws_error_message(&value),
        }),
        _ => None,
    }
}

fn parse_ws_usage(value: &Value) -> crate::gateway::RequestLogUsage {
    let top_usage = value.get("usage").and_then(Value::as_object);
    let response_usage = value
        .get("response")
        .and_then(|response| response.get("usage"))
        .and_then(Value::as_object);
    let usage = response_usage.or(top_usage);
    crate::gateway::RequestLogUsage {
        input_tokens: usage
            .and_then(|map| map.get("input_tokens"))
            .and_then(Value::as_i64)
            .or_else(|| {
                usage
                    .and_then(|map| map.get("prompt_tokens"))
                    .and_then(Value::as_i64)
            }),
        cached_input_tokens: usage
            .and_then(|map| map.get("input_tokens_details"))
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_i64)
            .or_else(|| {
                usage
                    .and_then(|map| map.get("cached_input_tokens"))
                    .and_then(Value::as_i64)
            }),
        output_tokens: usage
            .and_then(|map| map.get("output_tokens"))
            .and_then(Value::as_i64)
            .or_else(|| {
                usage
                    .and_then(|map| map.get("completion_tokens"))
                    .and_then(Value::as_i64)
            }),
        total_tokens: usage
            .and_then(|map| map.get("total_tokens"))
            .and_then(Value::as_i64),
        reasoning_output_tokens: usage
            .and_then(|map| map.get("output_tokens_details"))
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_i64)
            .or_else(|| {
                usage
                    .and_then(|map| map.get("reasoning_output_tokens"))
                    .and_then(Value::as_i64)
            }),
    }
}

fn extract_ws_error_message(value: &Value) -> Option<String> {
    value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|message| !message.is_empty())
        .map(str::to_string)
        .or_else(|| {
            value
                .get("message")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|message| !message.is_empty())
                .map(str::to_string)
        })
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) -> Result<(), WsSessionError> {
    let header_name = header::HeaderName::from_bytes(name.as_bytes()).map_err(|err| {
        WsSessionError::bad_gateway_bilingual(
            "上游 WebSocket 请求头名称无效",
            format!("invalid upstream websocket header name {name}: {err}"),
        )
    })?;
    let header_value = HeaderValue::from_str(value).map_err(|err| {
        WsSessionError::bad_gateway_bilingual(
            "上游 WebSocket 请求头值无效",
            format!("invalid upstream websocket header {name}: {err}"),
        )
    })?;
    headers.insert(header_name, header_value);
    Ok(())
}

async fn send_ws_error_and_close(
    socket: &mut WebSocket,
    err: WsSessionError,
    prefer_raw_errors: bool,
) {
    let message = crate::gateway::error_message_for_client(prefer_raw_errors, err.message);
    let payload = json!({
        "type": "error",
        "status": err.status,
        "error": {
            "code": err.code,
            "message": message,
        }
    });
    let _ = socket.send(Message::Text(payload.to_string().into())).await;
    let _ = socket.close().await;
}

fn parse_bool_header(value: Option<&HeaderValue>) -> bool {
    value
        .and_then(|header| header.to_str().ok())
        .map(str::trim)
        .is_some_and(|raw| {
            raw.eq_ignore_ascii_case("true")
                || raw.eq_ignore_ascii_case("1")
                || raw.eq_ignore_ascii_case("yes")
                || raw.eq_ignore_ascii_case("on")
        })
}

fn upgrade_required_response(message: impl Into<String>) -> Response<Body> {
    let mut response = text_response(StatusCode::UPGRADE_REQUIRED, message.into());
    response
        .headers_mut()
        .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    response.headers_mut().insert(
        crate::error_codes::ERROR_CODE_HEADER_NAME,
        HeaderValue::from_static("upgrade_required"),
    );
    response
}

impl From<String> for WsSessionError {
    fn from(value: String) -> Self {
        WsSessionError::bad_gateway(value)
    }
}
