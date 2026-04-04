use crate::commands::shared::rpc_call_in_background;

/// 函数 `service_requestlog_list`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - addr: 参数 addr
/// - query: 参数 query
/// - status_filter: 参数 status_filter
/// - page: 参数 page
/// - page_size: 参数 page_size
///
/// # 返回
/// 返回函数执行结果
#[tauri::command]
pub async fn service_requestlog_list(
    addr: Option<String>,
    query: Option<String>,
    status_filter: Option<String>,
    page: Option<i64>,
    page_size: Option<i64>,
) -> Result<serde_json::Value, String> {
    let params = serde_json::json!({
        "query": query,
        "statusFilter": status_filter,
        "page": page,
        "pageSize": page_size
    });
    rpc_call_in_background("requestlog/list", addr, Some(params)).await
}

/// 函数 `service_requestlog_error_list`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-04
///
/// # 参数
/// - addr: 参数 addr
/// - limit: 参数 limit
///
/// # 返回
/// 返回函数执行结果
#[tauri::command]
pub async fn service_requestlog_error_list(
    addr: Option<String>,
    limit: Option<i64>,
) -> Result<serde_json::Value, String> {
    let params = serde_json::json!({
        "limit": limit
    });
    rpc_call_in_background("requestlog/error_list", addr, Some(params)).await
}

/// 函数 `service_requestlog_clear`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - addr: 参数 addr
///
/// # 返回
/// 返回函数执行结果
#[tauri::command]
pub async fn service_requestlog_clear(addr: Option<String>) -> Result<serde_json::Value, String> {
    rpc_call_in_background("requestlog/clear", addr, None).await
}

/// 函数 `service_requestlog_error_clear`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-04
///
/// # 参数
/// - addr: 参数 addr
///
/// # 返回
/// 返回函数执行结果
#[tauri::command]
pub async fn service_requestlog_error_clear(
    addr: Option<String>,
) -> Result<serde_json::Value, String> {
    rpc_call_in_background("requestlog/error_clear", addr, None).await
}

/// 函数 `service_requestlog_summary`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - addr: 参数 addr
/// - query: 参数 query
/// - status_filter: 参数 status_filter
///
/// # 返回
/// 返回函数执行结果
#[tauri::command]
pub async fn service_requestlog_summary(
    addr: Option<String>,
    query: Option<String>,
    status_filter: Option<String>,
) -> Result<serde_json::Value, String> {
    let params = serde_json::json!({
        "query": query,
        "statusFilter": status_filter
    });
    rpc_call_in_background("requestlog/summary", addr, Some(params)).await
}

/// 函数 `service_requestlog_today_summary`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - addr: 参数 addr
///
/// # 返回
/// 返回函数执行结果
#[tauri::command]
pub async fn service_requestlog_today_summary(
    addr: Option<String>,
) -> Result<serde_json::Value, String> {
    rpc_call_in_background("requestlog/today_summary", addr, None).await
}
