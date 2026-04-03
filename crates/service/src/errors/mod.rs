use serde_json::json;
use serde_json::Value;

pub(crate) const ERROR_CODE_HEADER_NAME: &str = "X-CodexManager-Error-Code";
pub(crate) const TRACE_ID_HEADER_NAME: &str = "X-CodexManager-Trace-Id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorCode {
    UnknownMethod,
    UnknownError,
    InvalidSettingsPayload,
    InvalidRequestPayload,
    ProtocolMappingError,
    RequestBodyTooLarge,
    BackendProxyError,
    BuildResponseFailed,
    UpstreamTimeout,
    UpstreamChallengeBlocked,
    UpstreamRateLimited,
    UpstreamNotFound,
    UpstreamNonSuccess,
    NoAvailableAccount,
    CandidateResolveFailed,
    ResponseWriteFailed,
    StreamInterrupted,
}

impl ErrorCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownMethod => "unknown_method",
            Self::UnknownError => "unknown_error",
            Self::InvalidSettingsPayload => "invalid_settings_payload",
            Self::InvalidRequestPayload => "invalid_request_payload",
            Self::ProtocolMappingError => "protocol_mapping_error",
            Self::RequestBodyTooLarge => "request_body_too_large",
            Self::BackendProxyError => "backend_proxy_error",
            Self::BuildResponseFailed => "build_response_failed",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::UpstreamChallengeBlocked => "upstream_challenge_blocked",
            Self::UpstreamRateLimited => "upstream_rate_limited",
            Self::UpstreamNotFound => "upstream_not_found",
            Self::UpstreamNonSuccess => "upstream_non_success",
            Self::NoAvailableAccount => "no_available_account",
            Self::CandidateResolveFailed => "candidate_resolve_failed",
            Self::ResponseWriteFailed => "response_write_failed",
            Self::StreamInterrupted => "stream_interrupted",
        }
    }
}

/// 函数 `classify_message`
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
pub(crate) fn classify_message(message: &str) -> ErrorCode {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return ErrorCode::UnknownError;
    }

    if normalized == "unknown_method" {
        return ErrorCode::UnknownMethod;
    }
    if normalized.starts_with("invalid app settings payload:") {
        return ErrorCode::InvalidSettingsPayload;
    }
    if normalized.starts_with("request body too large") {
        return ErrorCode::RequestBodyTooLarge;
    }
    if normalized.starts_with("backend proxy error:") {
        return ErrorCode::BackendProxyError;
    }
    if normalized.starts_with("build response failed:") {
        return ErrorCode::BuildResponseFailed;
    }
    if normalized == "upstream total timeout exceeded" || normalized == "upstream request timed out"
    {
        return ErrorCode::UpstreamTimeout;
    }
    if normalized == "上游请求超时" || normalized.contains("连接超时") {
        return ErrorCode::UpstreamTimeout;
    }
    if normalized.starts_with("upstream blocked by cloudflare/waf")
        || normalized == "upstream challenge blocked"
    {
        return ErrorCode::UpstreamChallengeBlocked;
    }
    if normalized.contains("cloudflare/waf")
        || normalized.contains("安全验证拦截")
        || normalized.contains("验证/拦截页面")
    {
        return ErrorCode::UpstreamChallengeBlocked;
    }
    if normalized == "upstream rate-limited" {
        return ErrorCode::UpstreamRateLimited;
    }
    if normalized == "upstream not-found failover" {
        return ErrorCode::UpstreamNotFound;
    }
    if normalized == "upstream non-success" {
        return ErrorCode::UpstreamNonSuccess;
    }
    if normalized == "no available account" {
        return ErrorCode::NoAvailableAccount;
    }
    if normalized.starts_with("candidate resolve failed:") {
        return ErrorCode::CandidateResolveFailed;
    }
    if normalized.starts_with("response write failed:") {
        return ErrorCode::ResponseWriteFailed;
    }
    if normalized == "stream disconnected before completion" || normalized == "网络抖动" {
        return ErrorCode::StreamInterrupted;
    }
    if normalized.starts_with("upstream stream terminated unexpectedly")
        || normalized.starts_with("upstream stream read failed: connection interrupted")
    {
        return ErrorCode::StreamInterrupted;
    }
    if normalized.starts_with("上游流中途中断")
        || normalized.starts_with("上游流读取失败（连接中断）")
        || normalized.contains("上游连接中断")
    {
        return ErrorCode::StreamInterrupted;
    }
    if normalized.starts_with("upstream returned non-api content") {
        return ErrorCode::UpstreamNonSuccess;
    }
    if normalized.starts_with("上游返回的不是正常接口数据")
        || normalized.starts_with("上游返回了网页内容而不是接口数据")
    {
        return ErrorCode::UpstreamNonSuccess;
    }
    if normalized.contains("model_not_found")
        || normalized.contains("model not found")
        || normalized.contains("unsupported model")
        || normalized.contains("not supported")
        || normalized.contains("does not exist")
    {
        return ErrorCode::UpstreamNonSuccess;
    }
    if normalized.starts_with("模型不支持") {
        return ErrorCode::UpstreamNonSuccess;
    }
    if normalized.starts_with("invalid upstream ")
        || (normalized.contains("serialize") && normalized.contains("json"))
        || normalized.contains("sse bytes")
    {
        return ErrorCode::ProtocolMappingError;
    }
    if normalized == "invalid claude request json"
        || normalized == "claude request body must be an object"
    {
        return ErrorCode::InvalidRequestPayload;
    }

    ErrorCode::UnknownError
}

/// 函数 `code_or_dash`
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
pub(crate) fn code_or_dash(message: Option<&str>) -> &'static str {
    message
        .map(classify_message)
        .map(ErrorCode::as_str)
        .unwrap_or("-")
}

/// 函数 `code_for_message`
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
pub(crate) fn code_for_message(message: &str) -> &'static str {
    classify_message(message).as_str()
}

/// 函数 `rpc_error_payload`
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
pub(crate) fn rpc_error_payload(message: String) -> Value {
    let code = classify_message(message.as_str()).as_str();
    json!({
        "error": message,
        "errorCode": code,
        "errorDetail": {
            "code": code,
            "message": message,
        }
    })
}

/// 函数 `rpc_action_error_payload`
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
pub(crate) fn rpc_action_error_payload(message: String) -> Value {
    let code = classify_message(message.as_str()).as_str();
    json!({
        "ok": false,
        "error": message,
        "errorCode": code,
        "errorDetail": {
            "code": code,
            "message": message,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{classify_message, ErrorCode};

    /// 函数 `classify_known_messages`
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
    fn classify_known_messages() {
        assert_eq!(
            classify_message("invalid app settings payload: missing field"),
            ErrorCode::InvalidSettingsPayload
        );
        assert_eq!(
            classify_message("upstream total timeout exceeded"),
            ErrorCode::UpstreamTimeout
        );
        assert_eq!(
            classify_message("invalid upstream json payload"),
            ErrorCode::ProtocolMappingError
        );
        assert_eq!(
            classify_message("backend proxy error: connection refused"),
            ErrorCode::BackendProxyError
        );
        assert_eq!(
            classify_message("claude request body must be an object"),
            ErrorCode::InvalidRequestPayload
        );
        assert_eq!(classify_message("上游请求超时"), ErrorCode::UpstreamTimeout);
        assert_eq!(
            classify_message("upstream request timed out"),
            ErrorCode::UpstreamTimeout
        );
        assert_eq!(
            classify_message("上游被安全验证拦截（Cloudflare/WAF）"),
            ErrorCode::UpstreamChallengeBlocked
        );
        assert_eq!(
            classify_message("stream disconnected before completion"),
            ErrorCode::StreamInterrupted
        );
        assert_eq!(classify_message("网络抖动"), ErrorCode::StreamInterrupted);
        assert_eq!(
            classify_message("code=model_not_found type=invalid_request_error The model 'gpt-5.4' does not exist"),
            ErrorCode::UpstreamNonSuccess
        );
    }
}
