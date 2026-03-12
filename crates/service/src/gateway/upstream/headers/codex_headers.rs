use super::sticky_ids::random_session_id;

pub(crate) const CODEX_CLIENT_VERSION: &str = "0.101.0";
pub(super) const CODEX_USER_AGENT: &str =
    "codex_cli_rs/0.101.0 (Mac OS 26.0.1; arm64) Apple_Terminal/464";
const CODEX_OPENAI_BETA: &str = "responses=experimental";
const CODEX_ORIGINATOR: &str = "codex_cli_rs";

pub(crate) struct CodexUpstreamHeaderInput<'a> {
    pub(crate) auth_token: &'a str,
    pub(crate) account_id: Option<&'a str>,
    pub(crate) include_account_id: bool,
    pub(crate) include_openai_beta: bool,
    pub(crate) upstream_cookie: Option<&'a str>,
    pub(crate) incoming_session_id: Option<&'a str>,
    pub(crate) fallback_session_id: Option<&'a str>,
    pub(crate) incoming_turn_state: Option<&'a str>,
    pub(crate) include_turn_state: bool,
    pub(crate) incoming_conversation_id: Option<&'a str>,
    pub(crate) fallback_conversation_id: Option<&'a str>,
    pub(crate) include_conversation_id: bool,
    pub(crate) strip_session_affinity: bool,
    pub(crate) is_stream: bool,
    pub(crate) has_body: bool,
}

pub(crate) fn build_codex_upstream_headers(
    input: CodexUpstreamHeaderInput<'_>,
) -> Vec<(String, String)> {
    let mut headers = Vec::with_capacity(12);
    headers.push((
        "Authorization".to_string(),
        format!("Bearer {}", input.auth_token),
    ));
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
    headers.push(("Connection".to_string(), "Keep-Alive".to_string()));
    headers.push(("Version".to_string(), CODEX_CLIENT_VERSION.to_string()));
    if input.include_openai_beta {
        headers.push(("Openai-Beta".to_string(), CODEX_OPENAI_BETA.to_string()));
    }
    headers.push(("User-Agent".to_string(), CODEX_USER_AGENT.to_string()));
    headers.push(("Originator".to_string(), CODEX_ORIGINATOR.to_string()));
    headers.push((
        "Session_id".to_string(),
        resolve_session_id(
            input.incoming_session_id,
            input.fallback_session_id,
            input.strip_session_affinity,
        ),
    ));

    if !input.strip_session_affinity {
        if input.include_turn_state {
            if let Some(turn_state) = input.incoming_turn_state {
                headers.push(("x-codex-turn-state".to_string(), turn_state.to_string()));
            }
        }
        if input.include_conversation_id {
            if let Some(conversation_id) = input
                .incoming_conversation_id
                .or(input.fallback_conversation_id)
            {
                headers.push(("Conversation_id".to_string(), conversation_id.to_string()));
            }
        }
    }

    if input.include_account_id {
        if let Some(account_id) = input.account_id {
            headers.push(("Chatgpt-Account-Id".to_string(), account_id.to_string()));
        }
    }
    if let Some(cookie) = input
        .upstream_cookie
        .filter(|value| !value.trim().is_empty())
    {
        headers.push(("Cookie".to_string(), cookie.to_string()));
    }
    headers
}

fn resolve_session_id(
    incoming: Option<&str>,
    fallback_session_id: Option<&str>,
    strip_session_affinity: bool,
) -> String {
    if strip_session_affinity {
        return random_session_id();
    }
    if let Some(value) = incoming {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Some(value) = fallback_session_id {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    random_session_id()
}
