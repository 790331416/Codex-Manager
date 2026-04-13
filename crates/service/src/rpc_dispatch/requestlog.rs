use codexmanager_core::rpc::types::{
    GatewayErrorLogListParams, JsonRpcRequest, JsonRpcResponse, RequestLogListParams,
};

use crate::{
    requestlog_clear, requestlog_error_list, requestlog_list, requestlog_summary,
    requestlog_today_summary,
};

/// 函数 `try_handle`
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
pub(super) fn try_handle(req: &JsonRpcRequest) -> Option<JsonRpcResponse> {
    let result = match req.method.as_str() {
        "requestlog/list" => {
            let params = req
                .params
                .clone()
                .map(serde_json::from_value::<RequestLogListParams>)
                .transpose()
                .map(|params| params.unwrap_or_default())
                .map(RequestLogListParams::normalized)
                .map_err(|err| format!("invalid requestlog/list params: {err}"));
            super::value_or_error(params.and_then(requestlog_list::read_request_log_page))
        }
        "requestlog/summary" => {
            let query = super::string_param(req, "query");
            let status_filter = super::string_param(req, "statusFilter");
            super::value_or_error(requestlog_summary::read_request_log_filter_summary(
                query,
                status_filter,
            ))
        }
        "requestlog/clear" => super::ok_or_error(requestlog_clear::clear_request_logs()),
        "requestlog/error_clear" => {
            super::ok_or_error(requestlog_clear::clear_gateway_error_logs())
        }
        "requestlog/error_list" => {
            let params = req
                .params
                .clone()
                .map(serde_json::from_value::<GatewayErrorLogListParams>)
                .transpose()
                .map(|params| params.unwrap_or_default())
                .map(GatewayErrorLogListParams::normalized)
                .map_err(|err| format!("invalid requestlog/error_list params: {err}"));
            super::value_or_error(params.and_then(requestlog_error_list::read_gateway_error_logs))
        }
        "requestlog/today_summary" => {
            let day_start_ts = super::i64_param(req, "dayStartTs");
            let day_end_ts = super::i64_param(req, "dayEndTs");
            super::value_or_error(requestlog_today_summary::read_requestlog_today_summary(
                day_start_ts,
                day_end_ts,
            ))
        }
        _ => return None,
    };

    Some(super::response(req, result))
}
