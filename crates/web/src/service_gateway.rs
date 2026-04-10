use super::*;

/// 函数 `should_spawn_service`
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
pub(super) fn should_spawn_service() -> bool {
    read_env_trim("CODEXMANAGER_WEB_NO_SPAWN_SERVICE").is_none()
}

/// 函数 `service_rpc_probe`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - service_addr: 参数 service_addr
/// - rpc_token: 参数 rpc_token
///
/// # 返回
/// 返回函数执行结果
async fn service_rpc_probe(service_addr: &str, rpc_token: &str) -> Result<(), String> {
    let trimmed = service_addr.trim();
    if trimmed.is_empty() {
        return Err("service address is empty".to_string());
    }

    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_millis(1200))
        .build()
        .map_err(|err| format!("probe client init failed: {err}"))?
        .post(format!("http://{trimmed}/rpc"))
        .header("content-type", "application/json")
        .header("x-codexmanager-rpc-token", rpc_token)
        .body(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {}
            })
            .to_string(),
        )
        .send()
        .await
        .map_err(|err| format!("probe request failed: {err}"))?;

    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err("rpc_token_mismatch".to_string());
    }
    if !response.status().is_success() {
        return Err(format!("probe http {}", response.status()));
    }

    let payload = response
        .json::<serde_json::Value>()
        .await
        .map_err(|err| format!("probe response parse failed: {err}"))?;
    let result = payload.get("result").and_then(|value| value.as_object());
    let user_agent = result
        .and_then(|value| value.get("userAgent").or_else(|| value.get("user_agent")))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let codex_home = result
        .and_then(|value| value.get("codexHome").or_else(|| value.get("codex_home")))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if !user_agent.contains("codex_cli_rs/") || codex_home.is_empty() {
        return Err("unexpected service on target port".to_string());
    }
    Ok(())
}

/// 函数 `shutdown_existing_service`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - service_addr: 参数 service_addr
///
/// # 返回
/// 返回函数执行结果
async fn shutdown_existing_service(service_addr: &str) -> bool {
    let addr = service_addr.to_string();
    let _ = tokio::task::spawn_blocking(move || {
        codexmanager_service::request_shutdown(&addr);
    })
    .await;

    for _ in 0..30 {
        if !tcp_probe(service_addr).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// 函数 `tcp_probe`
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
pub(super) async fn tcp_probe(addr: &str) -> bool {
    let addr = addr.trim();
    if addr.is_empty() {
        return false;
    }
    let addr = addr.strip_prefix("http://").unwrap_or(addr);
    let addr = addr.strip_prefix("https://").unwrap_or(addr);
    let addr = addr.split('/').next().unwrap_or(addr);
    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .is_ok()
}

/// 函数 `service_bin_path`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - dir: 参数 dir
///
/// # 返回
/// 返回函数执行结果
fn service_bin_path(dir: &Path) -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        return dir.join("codexmanager-service.exe");
    }
    #[cfg(not(target_os = "windows"))]
    {
        return dir.join("codexmanager-service");
    }
}

/// 函数 `spawn_service_detached`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - dir: 参数 dir
/// - service_addr: 参数 service_addr
///
/// # 返回
/// 返回函数执行结果
fn spawn_service_detached(dir: &Path, service_addr: &str) -> std::io::Result<()> {
    let bin = service_bin_path(dir);
    let mut cmd = Command::new(bin);
    let bind_addr = codexmanager_service::listener_bind_addr(service_addr);
    cmd.env("CODEXMANAGER_SERVICE_ADDR", bind_addr);

    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }

    let _child = cmd.spawn()?;
    Ok(())
}

/// 函数 `ensure_service_running`
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
pub(super) async fn ensure_service_running(
    service_addr: &str,
    rpc_token: &str,
    dir: &Path,
    spawned_service: &Arc<Mutex<bool>>,
) -> Option<String> {
    if tcp_probe(service_addr).await {
        match service_rpc_probe(service_addr, rpc_token).await {
            Ok(()) => return None,
            Err(err) if err == "rpc_token_mismatch" && should_spawn_service() => {
                if !shutdown_existing_service(service_addr).await {
                    return Some(format!(
                        "service reachable at {service_addr} but rejected rpc token; old instance is still occupying the port"
                    ));
                }
            }
            Err(err) => {
                return Some(format!(
                    "service reachable at {service_addr} but startup handshake failed: {err}"
                ));
            }
        }
    }
    if !should_spawn_service() {
        return Some(format!(
            "service not reachable at {service_addr} (spawn disabled)"
        ));
    }

    let bin = service_bin_path(dir);
    if !bin.is_file() {
        return Some(format!(
            "service not reachable at {service_addr} (missing {})",
            bin.display()
        ));
    }

    if let Err(err) = spawn_service_detached(dir, service_addr) {
        return Some(format!("failed to spawn service: {err}"));
    }
    *spawned_service.lock().await = true;

    let mut last_probe_error: Option<String> = None;
    for _ in 0..50 {
        if tcp_probe(service_addr).await {
            match service_rpc_probe(service_addr, rpc_token).await {
                Ok(()) => return None,
                Err(err) => {
                    last_probe_error = Some(format!(
                        "service became reachable but startup handshake failed: {err}"
                    ));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some(
        last_probe_error.unwrap_or_else(|| {
            format!("service still not reachable at {service_addr} after spawn")
        }),
    )
}

/// 函数 `rpc_proxy`
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
pub(super) async fn rpc_proxy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !is_json_content_type(&headers) {
        return (StatusCode::UNSUPPORTED_MEDIA_TYPE, "{}").into_response();
    }
    let resp = state
        .client
        .post(&state.service_rpc_url)
        .header("content-type", "application/json")
        .header("x-codexmanager-rpc-token", &state.rpc_token)
        .body(body)
        .send()
        .await;
    let resp = match resp {
        Ok(v) => v,
        Err(err) => {
            let msg = format_upstream_error_message(state.service_addr.as_str(), &err);
            return (StatusCode::BAD_GATEWAY, msg).into_response();
        }
    };

    let status = resp.status();
    let bytes = match resp.bytes().await {
        Ok(v) => v,
        Err(err) => {
            let msg = format!("upstream read error: {err}");
            return (StatusCode::BAD_GATEWAY, msg).into_response();
        }
    };
    let mut out = Response::new(axum::body::Body::from(bytes));
    *out.status_mut() = status;
    out.headers_mut().insert(
        "content-type",
        axum::http::HeaderValue::from_static("application/json"),
    );
    out
}

/// 函数 `format_upstream_error_message`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-10
///
/// # 参数
/// - service_addr: 参数 service_addr
/// - err: 参数 err
///
/// # 返回
/// 返回函数执行结果
fn format_upstream_error_message(
    service_addr: &str,
    err: impl std::fmt::Display,
) -> String {
    let mut message = format!("upstream error: {err}");
    if service_addr.contains("host.docker.internal") {
        message.push_str(
            "; Linux Docker 中 `host.docker.internal` 可能不可解析，建议让 web 与 service 加入同一 Docker 网络，并使用容器地址如 `codexmanager-service:48760`；如果必须走宿主机端口，请增加 `--add-host=host.docker.internal:host-gateway`",
        );
    }
    message
}

/// 函数 `quit`
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
pub(super) async fn quit(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if *state.spawned_service.lock().await {
        let addr = state.service_addr.clone();
        let _ = tokio::task::spawn_blocking(move || {
            codexmanager_service::request_shutdown(&addr);
        })
        .await;
    }
    let _ = state.shutdown_tx.send(true);
    Html("<html><body>OK</body></html>")
}

#[cfg(test)]
mod tests {
    use super::format_upstream_error_message;

    #[test]
    fn format_upstream_error_message_adds_docker_hint_for_host_internal() {
        let err = std::io::Error::other("dns failed");
        let message = format_upstream_error_message("host.docker.internal:9760", &err);
        assert!(message.contains("host.docker.internal"));
        assert!(message.contains("codexmanager-service:48760"));
    }
}
