use codexmanager_core::rpc::types::{JsonRpcRequest, JsonRpcResponse};

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
        "appSettings/get" => super::value_or_error(crate::app_settings_get()),
        "appSettings/set" => super::value_or_error(crate::app_settings_set(req.params.as_ref())),
        "authorContent/get" => super::value_or_error(crate::author_content_get()),
        "webAuth/status" => super::value_or_error(crate::app_auth_status_value()),
        "webAuth/password/set" => {
            let password = super::str_param(req, "password").unwrap_or("");
            super::value_or_error(
                crate::set_web_access_password(Some(password))
                    .map(|configured| serde_json::json!({ "passwordConfigured": configured })),
            )
        }
        "webAuth/password/clear" => super::value_or_error(
            crate::set_web_access_password(None)
                .map(|configured| serde_json::json!({ "passwordConfigured": configured })),
        ),
        _ => return None,
    };

    Some(super::response(req, result))
}
