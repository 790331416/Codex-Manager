use super::super::support::candidates;
use codexmanager_core::storage::Storage;

pub(in super::super) struct GatewayUpstreamExecutionContext<'a> {
    trace_id: &'a str,
    storage: &'a Storage,
    key_id: &'a str,
    original_path: &'a str,
    path: &'a str,
    request_method: &'a str,
    response_adapter: super::super::super::ResponseAdapter,
    protocol_type: &'a str,
    model_for_log: Option<&'a str>,
    reasoning_for_log: Option<&'a str>,
    service_tier_for_log: Option<&'a str>,
    effective_service_tier_for_log: Option<&'a str>,
    candidate_count: usize,
    account_max_inflight: usize,
}

impl<'a> GatewayUpstreamExecutionContext<'a> {
    /// 函数 `new`
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
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn new(
        trace_id: &'a str,
        storage: &'a Storage,
        key_id: &'a str,
        original_path: &'a str,
        path: &'a str,
        request_method: &'a str,
        response_adapter: super::super::super::ResponseAdapter,
        protocol_type: &'a str,
        model_for_log: Option<&'a str>,
        reasoning_for_log: Option<&'a str>,
        service_tier_for_log: Option<&'a str>,
        effective_service_tier_for_log: Option<&'a str>,
        candidate_count: usize,
        account_max_inflight: usize,
    ) -> Self {
        Self {
            trace_id,
            storage,
            key_id,
            original_path,
            path,
            request_method,
            response_adapter,
            protocol_type,
            model_for_log,
            reasoning_for_log,
            service_tier_for_log,
            effective_service_tier_for_log,
            candidate_count,
            account_max_inflight,
        }
    }

    /// 函数 `has_more_candidates`
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
    pub(in super::super) fn has_more_candidates(&self, idx: usize) -> bool {
        idx + 1 < self.candidate_count
    }

    pub(in super::super) fn protocol_type(&self) -> &str {
        self.protocol_type
    }

    /// 函数 `should_skip_candidate`
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
    pub(in super::super) fn should_skip_candidate(
        &self,
        account_id: &str,
        idx: usize,
    ) -> Option<candidates::CandidateSkipReason> {
        candidates::candidate_skip_reason_for_proxy(
            account_id,
            idx,
            self.candidate_count,
            self.account_max_inflight,
        )
    }

    /// 函数 `log_candidate_start`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - in super: 参数 in super
    ///
    /// # 返回
    /// 无
    pub(in super::super) fn log_candidate_start(
        &self,
        account_id: &str,
        idx: usize,
        strip_session_affinity: bool,
    ) {
        super::super::super::trace_log::log_candidate_start(
            self.trace_id,
            idx,
            self.candidate_count,
            account_id,
            strip_session_affinity,
        );
    }

    /// 函数 `log_candidate_skip`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - in super: 参数 in super
    ///
    /// # 返回
    /// 无
    pub(in super::super) fn log_candidate_skip(
        &self,
        account_id: &str,
        idx: usize,
        reason: candidates::CandidateSkipReason,
    ) {
        let reason_text = match reason {
            candidates::CandidateSkipReason::Cooldown => "cooldown",
            candidates::CandidateSkipReason::Inflight => "inflight",
        };
        super::super::super::trace_log::log_candidate_skip(
            self.trace_id,
            idx,
            self.candidate_count,
            account_id,
            reason_text,
        );
    }

    /// 函数 `log_attempt_result`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - super: 参数 super
    ///
    /// # 返回
    /// 无
    pub(super) fn log_attempt_result(
        &self,
        account_id: &str,
        upstream_url: Option<&str>,
        status_code: u16,
        error: Option<&str>,
    ) {
        super::super::super::trace_log::log_attempt_result(
            self.trace_id,
            account_id,
            upstream_url,
            status_code,
            error,
        );
    }

    /// 函数 `mark_account_unavailable_for_gateway_error`
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
    pub(in super::super) fn mark_account_unavailable_for_gateway_error(
        &self,
        account_id: &str,
        err: &str,
    ) -> bool {
        crate::account_status::mark_account_unavailable_for_gateway_error(
            self.storage,
            account_id,
            err,
        )
    }

    pub(in super::super) fn apply_gateway_error_follow_up(
        &self,
        account_id: &str,
        err: &str,
        has_more_candidates: bool,
    ) -> crate::account_status::GatewayErrorFollowUp {
        let follow_up = crate::account_status::analyze_gateway_error(err, has_more_candidates);
        if follow_up.should_mark_default_cooldown {
            super::super::super::mark_account_cooldown(
                account_id,
                super::super::super::CooldownReason::Default,
            );
        }
        if follow_up.should_mark_account_unavailable {
            let _ = self.mark_account_unavailable_for_gateway_error(account_id, err);
        }
        follow_up
    }

    /// 函数 `log_final_result`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - in super: 参数 in super
    ///
    /// # 返回
    /// 无
    pub(in super::super) fn log_final_result(
        &self,
        final_account_id: Option<&str>,
        upstream_url: Option<&str>,
        status_code: u16,
        usage: super::super::super::request_log::RequestLogUsage,
        error: Option<&str>,
        elapsed_ms: u128,
        attempted_account_ids: Option<&[String]>,
    ) {
        self.log_final_result_with_model(
            final_account_id,
            upstream_url,
            self.model_for_log,
            status_code,
            usage,
            error,
            elapsed_ms,
            attempted_account_ids,
        );
    }

    /// 函数 `log_final_result_with_model`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - in super: 参数 in super
    ///
    /// # 返回
    /// 无
    #[allow(clippy::too_many_arguments)]
    pub(in super::super) fn log_final_result_with_model(
        &self,
        final_account_id: Option<&str>,
        upstream_url: Option<&str>,
        model_for_log: Option<&str>,
        status_code: u16,
        usage: super::super::super::request_log::RequestLogUsage,
        error: Option<&str>,
        elapsed_ms: u128,
        attempted_account_ids: Option<&[String]>,
    ) {
        super::super::super::request_log::write_request_log_with_attempts(
            self.storage,
            super::super::super::request_log::RequestLogTraceContext {
                trace_id: Some(self.trace_id),
                original_path: Some(self.original_path),
                adapted_path: Some(self.path),
                response_adapter: Some(self.response_adapter),
                request_type: Some("http"),
                service_tier: self.service_tier_for_log,
                effective_service_tier: self.effective_service_tier_for_log,
                ..Default::default()
            },
            Some(self.key_id),
            final_account_id,
            self.path,
            self.request_method,
            model_for_log,
            self.reasoning_for_log,
            upstream_url,
            Some(status_code),
            usage,
            error,
            Some(elapsed_ms),
            attempted_account_ids,
        );
        super::super::super::trace_log::log_request_final(
            self.trace_id,
            status_code,
            final_account_id,
            upstream_url,
            error,
            elapsed_ms,
        );
        super::super::super::record_gateway_request_outcome(
            self.path,
            status_code,
            Some(self.protocol_type),
        );
    }
}
