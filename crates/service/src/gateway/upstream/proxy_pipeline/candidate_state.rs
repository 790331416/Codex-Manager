use bytes::Bytes;
use codexmanager_core::storage::Account;
use std::collections::HashMap;

use super::super::support::payload_rewrite::strip_encrypted_content_from_body;
use super::request_setup::UpstreamRequestSetup;

#[derive(Default)]
pub(in super::super) struct CandidateExecutionState {
    stripped_body: Option<Bytes>,
    rewritten_bodies: HashMap<String, Bytes>,
    stripped_rewritten_bodies: HashMap<String, Bytes>,
    first_candidate_account_scope: Option<String>,
}

impl CandidateExecutionState {
    fn existing_prompt_cache_key(body: &Bytes) -> Option<String> {
        serde_json::from_slice::<serde_json::Value>(body.as_ref())
            .ok()
            .and_then(|value| {
                value
                    .get("prompt_cache_key")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string())
            })
    }

    /// 函数 `rewrite_cache_key`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - model_override: 参数 model_override
    /// - prompt_cache_key: 参数 prompt_cache_key
    ///
    /// # 返回
    /// 返回函数执行结果
    fn rewrite_cache_key(
        model_override: Option<&str>,
        prompt_cache_key: Option<&str>,
    ) -> Option<String> {
        let normalized_model = model_override
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let normalized_prompt_cache_key = prompt_cache_key
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if normalized_model.is_none() && normalized_prompt_cache_key.is_none() {
            return None;
        }
        Some(format!(
            "model={}|thread={}",
            normalized_model.unwrap_or("-"),
            normalized_prompt_cache_key.unwrap_or("-")
        ))
    }

    /// 函数 `strip_session_affinity`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - in super: 参数 in super
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(in super::super) fn strip_session_affinity(
        &mut self,
        account: &Account,
        idx: usize,
        anthropic_has_prompt_cache_key: bool,
    ) -> bool {
        if !anthropic_has_prompt_cache_key {
            return idx > 0;
        }
        let candidate_scope = account
            .chatgpt_account_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
            .or_else(|| {
                account
                    .workspace_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string())
            });
        if idx == 0 {
            self.first_candidate_account_scope = candidate_scope.clone();
            false
        } else {
            candidate_scope != self.first_candidate_account_scope
        }
    }

    /// 函数 `rewrite_body_for_model`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - path: 参数 path
    /// - body: 参数 body
    /// - setup: 参数 setup
    /// - model_override: 参数 model_override
    /// - prompt_cache_key: 参数 prompt_cache_key
    ///
    /// # 返回
    /// 返回函数执行结果
    fn rewrite_body_for_model(
        &mut self,
        path: &str,
        body: &Bytes,
        setup: &UpstreamRequestSetup,
        model_override: Option<&str>,
        prompt_cache_key: Option<&str>,
    ) -> Bytes {
        let existing_prompt_cache_key = Self::existing_prompt_cache_key(body);
        let effective_prompt_cache_key = existing_prompt_cache_key.as_deref().or(prompt_cache_key);
        let Some(cache_key) = Self::rewrite_cache_key(model_override, effective_prompt_cache_key)
        else {
            return body.clone();
        };

        self.rewritten_bodies
            .entry(cache_key)
            .or_insert_with(|| {
                Bytes::from(
                    super::super::super::apply_request_overrides_with_prompt_cache_key(
                        path,
                        body.to_vec(),
                        model_override,
                        None,
                        Some(setup.upstream_base.as_str()),
                        effective_prompt_cache_key,
                    ),
                )
            })
            .clone()
    }

    /// 函数 `body_for_attempt`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - in super: 参数 in super
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(in super::super) fn body_for_attempt(
        &mut self,
        path: &str,
        body: &Bytes,
        strip_session_affinity: bool,
        setup: &UpstreamRequestSetup,
        model_override: Option<&str>,
        prompt_cache_key: Option<&str>,
    ) -> Bytes {
        let rewritten =
            self.rewrite_body_for_model(path, body, setup, model_override, prompt_cache_key);
        if strip_session_affinity && setup.has_body_encrypted_content {
            if let Some(cache_key) = Self::rewrite_cache_key(model_override, prompt_cache_key) {
                return self
                    .stripped_rewritten_bodies
                    .entry(cache_key)
                    .or_insert_with(|| {
                        strip_encrypted_content_from_body(rewritten.as_ref())
                            .map(Bytes::from)
                            .unwrap_or_else(|| rewritten.clone())
                    })
                    .clone();
            }
            if self.stripped_body.is_none() {
                self.stripped_body = strip_encrypted_content_from_body(rewritten.as_ref())
                    .map(Bytes::from)
                    .or_else(|| Some(rewritten.clone()));
            }
            self.stripped_body
                .as_ref()
                .expect("stripped body should be initialized")
                .clone()
        } else {
            rewritten
        }
    }

    /// 函数 `retry_body`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - in super: 参数 in super
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(in super::super) fn retry_body(
        &mut self,
        path: &str,
        body: &Bytes,
        setup: &UpstreamRequestSetup,
        model_override: Option<&str>,
        prompt_cache_key: Option<&str>,
    ) -> Bytes {
        let rewritten =
            self.rewrite_body_for_model(path, body, setup, model_override, prompt_cache_key);
        if setup.has_body_encrypted_content {
            if let Some(cache_key) = Self::rewrite_cache_key(model_override, prompt_cache_key) {
                return self
                    .stripped_rewritten_bodies
                    .entry(cache_key)
                    .or_insert_with(|| {
                        strip_encrypted_content_from_body(rewritten.as_ref())
                            .map(Bytes::from)
                            .unwrap_or_else(|| rewritten.clone())
                    })
                    .clone();
            }
            if self.stripped_body.is_none() {
                self.stripped_body = strip_encrypted_content_from_body(rewritten.as_ref())
                    .map(Bytes::from)
                    .or_else(|| Some(rewritten.clone()));
            }
            self.stripped_body
                .as_ref()
                .expect("stripped body should be initialized")
                .clone()
        } else {
            rewritten
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CandidateExecutionState;
    use bytes::Bytes;

    /// 函数 `body_for_attempt_rewrites_model_override`
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
    fn body_for_attempt_rewrites_model_override() {
        let mut state = CandidateExecutionState::default();
        let body = Bytes::from_static(br#"{"model":"gpt-5.4","input":"hello"}"#);
        let setup = super::super::request_setup::UpstreamRequestSetup {
            upstream_base: "https://chatgpt.com/backend-api/codex".to_string(),
            upstream_fallback_base: None,
            url: "https://chatgpt.com/backend-api/codex/responses".to_string(),
            url_alt: None,
            candidate_count: 1,
            account_max_inflight: 1,
            anthropic_has_prompt_cache_key: false,
            has_sticky_fallback_session: false,
            has_sticky_fallback_conversation: false,
            has_body_encrypted_content: false,
            conversation_routing: None,
        };

        let actual = state.body_for_attempt(
            "/v1/responses",
            &body,
            false,
            &setup,
            Some("gpt-5.2"),
            Some("thread-2"),
        );
        let value: serde_json::Value =
            serde_json::from_slice(actual.as_ref()).expect("parse rewritten body");

        assert_eq!(
            value.get("model").and_then(serde_json::Value::as_str),
            Some("gpt-5.2")
        );
        assert_eq!(
            value
                .get("prompt_cache_key")
                .and_then(serde_json::Value::as_str),
            Some("thread-2")
        );
    }

    #[test]
    fn body_for_attempt_preserves_existing_prompt_cache_key() {
        let mut state = CandidateExecutionState::default();
        let body = Bytes::from_static(
            br#"{"model":"gpt-5.4","input":"hello","prompt_cache_key":"client-thread"}"#,
        );
        let setup = super::super::request_setup::UpstreamRequestSetup {
            upstream_base: "https://chatgpt.com/backend-api/codex".to_string(),
            upstream_fallback_base: None,
            url: "https://chatgpt.com/backend-api/codex/responses".to_string(),
            url_alt: None,
            candidate_count: 1,
            account_max_inflight: 1,
            anthropic_has_prompt_cache_key: false,
            has_sticky_fallback_session: false,
            has_sticky_fallback_conversation: false,
            has_body_encrypted_content: false,
            conversation_routing: None,
        };

        let actual = state.body_for_attempt(
            "/v1/responses",
            &body,
            false,
            &setup,
            None,
            Some("thread-from-conversation"),
        );
        let value: serde_json::Value =
            serde_json::from_slice(actual.as_ref()).expect("parse rewritten body");

        assert_eq!(
            value
                .get("prompt_cache_key")
                .and_then(serde_json::Value::as_str),
            Some("client-thread")
        );
    }
}
