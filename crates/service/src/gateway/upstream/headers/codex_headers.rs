pub(crate) const OPENAI_BETA_HEADER: &str = "OpenAI-Beta";
pub(crate) const RESPONSES_WEBSOCKETS_V2_BETA_HEADER_VALUE: &str =
    "responses_websockets=2026-02-06";
pub(crate) const X_RESPONSESAPI_INCLUDE_TIMING_METRICS_HEADER: &str =
    "x-responsesapi-include-timing-metrics";
pub(crate) const CODEX_CLIENT_VERSION: &str = "0.101.0";

pub(crate) struct CodexUpstreamHeaderInput<'a> {
    pub(crate) auth_token: &'a str,
    pub(crate) chatgpt_account_id: Option<&'a str>,
    pub(crate) incoming_session_id: Option<&'a str>,
    pub(crate) incoming_client_request_id: Option<&'a str>,
    pub(crate) incoming_subagent: Option<&'a str>,
    pub(crate) incoming_beta_features: Option<&'a str>,
    pub(crate) incoming_turn_metadata: Option<&'a str>,
    pub(crate) fallback_session_id: Option<&'a str>,
    pub(crate) incoming_turn_state: Option<&'a str>,
    pub(crate) include_turn_state: bool,
    pub(crate) strip_session_affinity: bool,
    pub(crate) is_stream: bool,
    pub(crate) include_timing_metrics: bool,
    pub(crate) has_body: bool,
}

pub(crate) struct CodexCompactUpstreamHeaderInput<'a> {
    pub(crate) auth_token: &'a str,
    pub(crate) chatgpt_account_id: Option<&'a str>,
    pub(crate) incoming_session_id: Option<&'a str>,
    pub(crate) incoming_subagent: Option<&'a str>,
    pub(crate) fallback_session_id: Option<&'a str>,
    pub(crate) strip_session_affinity: bool,
    pub(crate) is_stream: bool,
    pub(crate) include_timing_metrics: bool,
    pub(crate) has_body: bool,
}

/// 函数 `build_codex_upstream_headers`
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
pub(crate) fn build_codex_upstream_headers(
    input: CodexUpstreamHeaderInput<'_>,
) -> Vec<(String, String)> {
    let mut headers = Vec::with_capacity(10);
    headers.push((
        "Authorization".to_string(),
        format!("Bearer {}", input.auth_token),
    ));
    if let Some(account_id) = input
        .chatgpt_account_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.push(("ChatGPT-Account-ID".to_string(), account_id.to_string()));
    }
    if input.has_body {
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
    }
    headers.push((
        "Accept".to_string(),
        if input.is_stream {
            "text/event-stream"
        } else {
            "application/json"
        }
        .to_string(),
    ));
    headers.push((
        "User-Agent".to_string(),
        crate::gateway::current_codex_user_agent(),
    ));
    if input.is_stream {
        headers.push((
            OPENAI_BETA_HEADER.to_string(),
            RESPONSES_WEBSOCKETS_V2_BETA_HEADER_VALUE.to_string(),
        ));
    }
    headers.push((
        "originator".to_string(),
        crate::gateway::current_wire_originator(),
    ));
    if let Some(residency_requirement) = crate::gateway::current_residency_requirement() {
        headers.push((
            crate::gateway::runtime_config::RESIDENCY_HEADER_NAME.to_string(),
            residency_requirement,
        ));
    }
    if let Some(client_request_id) = resolve_client_request_id(input.incoming_client_request_id) {
        headers.push(("x-client-request-id".to_string(), client_request_id));
    }
    if let Some(subagent) = input
        .incoming_subagent
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.push(("x-openai-subagent".to_string(), subagent.to_string()));
    }
    if let Some(beta_features) = input
        .incoming_beta_features
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.push((
            "x-codex-beta-features".to_string(),
            beta_features.to_string(),
        ));
    }
    if let Some(turn_metadata) = input
        .incoming_turn_metadata
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.push((
            "x-codex-turn-metadata".to_string(),
            turn_metadata.to_string(),
        ));
    }
    if let Some(session_id) = resolve_optional_session_id(
        input.incoming_session_id,
        input.fallback_session_id,
        input.strip_session_affinity,
    ) {
        headers.push(("session_id".to_string(), session_id));
    }

    if input.is_stream && input.include_timing_metrics {
        headers.push((
            X_RESPONSESAPI_INCLUDE_TIMING_METRICS_HEADER.to_string(),
            "true".to_string(),
        ));
    }

    if !input.strip_session_affinity {
        if input.include_turn_state {
            if let Some(turn_state) = input.incoming_turn_state {
                headers.push(("x-codex-turn-state".to_string(), turn_state.to_string()));
            }
        }
    }

    headers
}

/// 函数 `build_codex_compact_upstream_headers`
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
pub(crate) fn build_codex_compact_upstream_headers(
    input: CodexCompactUpstreamHeaderInput<'_>,
) -> Vec<(String, String)> {
    let mut headers = Vec::with_capacity(8);
    headers.push((
        "Authorization".to_string(),
        format!("Bearer {}", input.auth_token),
    ));
    if let Some(account_id) = input
        .chatgpt_account_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.push(("ChatGPT-Account-ID".to_string(), account_id.to_string()));
    }
    if input.has_body {
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
    }
    headers.push(("Accept".to_string(), "application/json".to_string()));
    headers.push((
        "User-Agent".to_string(),
        crate::gateway::current_codex_user_agent(),
    ));
    if input.is_stream {
        headers.push((
            OPENAI_BETA_HEADER.to_string(),
            RESPONSES_WEBSOCKETS_V2_BETA_HEADER_VALUE.to_string(),
        ));
    }
    headers.push((
        "originator".to_string(),
        crate::gateway::current_wire_originator(),
    ));
    if let Some(residency_requirement) = crate::gateway::current_residency_requirement() {
        headers.push((
            crate::gateway::runtime_config::RESIDENCY_HEADER_NAME.to_string(),
            residency_requirement,
        ));
    }
    if let Some(subagent) = input
        .incoming_subagent
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.push(("x-openai-subagent".to_string(), subagent.to_string()));
    }
    if let Some(session_id) = resolve_optional_session_id(
        input.incoming_session_id,
        input.fallback_session_id,
        input.strip_session_affinity,
    ) {
        headers.push(("session_id".to_string(), session_id));
    }
    if input.is_stream && input.include_timing_metrics {
        headers.push((
            X_RESPONSESAPI_INCLUDE_TIMING_METRICS_HEADER.to_string(),
            "true".to_string(),
        ));
    }
    headers
}

/// 函数 `resolve_optional_session_id`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - incoming: 参数 incoming
/// - fallback_session_id: 参数 fallback_session_id
/// - strip_session_affinity: 参数 strip_session_affinity
///
/// # 返回
/// 返回函数执行结果
fn resolve_optional_session_id(
    incoming: Option<&str>,
    fallback_session_id: Option<&str>,
    strip_session_affinity: bool,
) -> Option<String> {
    if strip_session_affinity {
        return fallback_session_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
    }
    if let Some(value) = incoming {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    fallback_session_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// 函数 `resolve_client_request_id`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - incoming_client_request_id: 参数 incoming_client_request_id
///
/// # 返回
/// 返回函数执行结果
fn resolve_client_request_id(incoming_client_request_id: Option<&str>) -> Option<String> {
    if let Some(value) = incoming_client_request_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(value.to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{build_codex_compact_upstream_headers, build_codex_upstream_headers};
    use crate::gateway::{
        set_codex_user_agent_version, set_originator, CodexCompactUpstreamHeaderInput,
        CodexUpstreamHeaderInput,
    };

    /// 函数 `header_value`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - headers: 参数 headers
    /// - name: 参数 name
    ///
    /// # 返回
    /// 返回函数执行结果
    fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// 函数 `build_codex_upstream_headers_keeps_final_affinity_shape`
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
    fn build_codex_upstream_headers_keeps_final_affinity_shape() {
        let _guard = crate::test_env_guard();
        let _ = set_originator("codex_cli_rs_tests").expect("set originator");
        let _ = set_codex_user_agent_version("0.999.0").expect("set ua version");

        let headers = build_codex_upstream_headers(CodexUpstreamHeaderInput {
            auth_token: "token-123",
            chatgpt_account_id: Some("account-123"),
            incoming_session_id: Some("conversation-anchor"),
            incoming_client_request_id: Some("conversation-anchor"),
            incoming_subagent: Some("subagent-a"),
            incoming_beta_features: Some("beta-a"),
            incoming_turn_metadata: Some("meta-a"),
            fallback_session_id: Some("conversation-anchor"),
            incoming_turn_state: Some("turn-state-a"),
            include_turn_state: true,
            strip_session_affinity: false,
            is_stream: true,
            include_timing_metrics: true,
            has_body: true,
        });

        assert_eq!(
            header_value(&headers, "Authorization"),
            Some("Bearer token-123")
        );
        assert_eq!(
            header_value(&headers, "ChatGPT-Account-ID"),
            Some("account-123")
        );
        assert_eq!(
            header_value(&headers, "Content-Type"),
            Some("application/json")
        );
        assert_eq!(header_value(&headers, "Accept"), Some("text/event-stream"));
        assert_eq!(
            header_value(&headers, "OpenAI-Beta"),
            Some("responses_websockets=2026-02-06")
        );
        assert_eq!(
            header_value(&headers, "x-responsesapi-include-timing-metrics"),
            Some("true")
        );
        let expected_user_agent_prefix =
            format!("{}/0.999.0", crate::gateway::current_wire_originator());
        assert_eq!(
            header_value(&headers, "User-Agent")
                .map(|value| value.starts_with(expected_user_agent_prefix.as_str())),
            Some(true)
        );
        assert_eq!(header_value(&headers, "originator"), Some("codex_cli_rs"));
        assert_eq!(
            header_value(&headers, "x-client-request-id"),
            Some("conversation-anchor")
        );
        assert_eq!(
            header_value(&headers, "session_id"),
            Some("conversation-anchor")
        );
        assert_eq!(
            header_value(&headers, "x-codex-turn-state"),
            Some("turn-state-a")
        );
    }

    /// 函数 `build_codex_upstream_headers_clears_turn_state_when_affinity_diverges`
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
    fn build_codex_upstream_headers_clears_turn_state_when_affinity_diverges() {
        let _guard = crate::test_env_guard();
        let _ = set_originator("codex_cli_rs_tests").expect("set originator");
        let _ = set_codex_user_agent_version("0.999.1").expect("set ua version");

        let headers = build_codex_upstream_headers(CodexUpstreamHeaderInput {
            auth_token: "token-456",
            chatgpt_account_id: None,
            incoming_session_id: Some("conversation-anchor"),
            incoming_client_request_id: Some("conversation-anchor"),
            incoming_subagent: None,
            incoming_beta_features: None,
            incoming_turn_metadata: None,
            fallback_session_id: Some("prompt-cache-anchor"),
            incoming_turn_state: None,
            include_turn_state: true,
            strip_session_affinity: false,
            is_stream: false,
            include_timing_metrics: false,
            has_body: false,
        });

        assert_eq!(header_value(&headers, "Accept"), Some("application/json"));
        assert_eq!(
            header_value(&headers, "x-client-request-id"),
            Some("conversation-anchor")
        );
        assert_eq!(
            header_value(&headers, "session_id"),
            Some("conversation-anchor")
        );
        assert_eq!(header_value(&headers, "x-codex-turn-state"), None);
    }

    /// 函数 `build_codex_compact_upstream_headers_use_session_fallback_only`
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
    fn build_codex_compact_upstream_headers_use_session_fallback_only() {
        let _guard = crate::test_env_guard();
        let _ = set_originator("codex_cli_rs_tests").expect("set originator");
        let _ = set_codex_user_agent_version("0.999.2").expect("set ua version");

        let headers = build_codex_compact_upstream_headers(CodexCompactUpstreamHeaderInput {
            auth_token: "token-789",
            chatgpt_account_id: Some("account-compact"),
            incoming_session_id: None,
            incoming_subagent: Some("subagent-b"),
            fallback_session_id: Some("conversation-anchor"),
            strip_session_affinity: true,
            is_stream: true,
            include_timing_metrics: false,
            has_body: true,
        });

        assert_eq!(header_value(&headers, "Accept"), Some("application/json"));
        assert_eq!(
            header_value(&headers, "ChatGPT-Account-ID"),
            Some("account-compact")
        );
        assert_eq!(header_value(&headers, "x-client-request-id"), None);
        assert_eq!(
            header_value(&headers, "session_id"),
            Some("conversation-anchor")
        );
        assert_eq!(header_value(&headers, "x-codex-turn-state"), None);
        assert_eq!(
            header_value(&headers, "OpenAI-Beta"),
            Some("responses_websockets=2026-02-06")
        );
        assert_eq!(
            header_value(&headers, "x-openai-subagent"),
            Some("subagent-b")
        );
    }
}
