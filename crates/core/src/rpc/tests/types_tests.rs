use super::{
    AccountListParams, AccountListResult, AccountSummary, ApiKeyUsageStatSummary,
    RequestLogFilterSummaryResult, RequestLogListParams, RequestLogListResult, RequestLogSummary,
};

/// 函数 `account_summary_serialization_matches_compact_contract`
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
fn account_summary_serialization_matches_compact_contract() {
    let summary = AccountSummary {
        id: "acc-1".to_string(),
        label: "主账号".to_string(),
        group_name: Some("TEAM".to_string()),
        preferred: true,
        sort: 10,
        status: "active".to_string(),
        status_reason: Some("account_deactivated".to_string()),
        plan_type: Some("team".to_string()),
        plan_type_raw: None,
        has_subscription: Some(true),
        subscription_plan: Some("team".to_string()),
        subscription_expires_at: Some(1_745_000_000),
        subscription_renews_at: Some(1_745_000_000),
        note: Some("主账号".to_string()),
        tags: Some("高频,团队A".to_string()),
    };

    let value = serde_json::to_value(summary).expect("serialize account summary");
    let obj = value.as_object().expect("account summary object");

    for key in [
        "id",
        "label",
        "groupName",
        "preferred",
        "sort",
        "status",
        "statusReason",
        "note",
        "tags",
    ] {
        assert!(obj.contains_key(key), "missing key: {key}");
    }

    for key in ["workspaceId", "workspaceName", "updatedAt"] {
        assert!(!obj.contains_key(key), "unexpected key: {key}");
    }
}

/// 函数 `account_list_params_default_to_first_page_with_five_items`
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
fn account_list_params_default_to_first_page_with_five_items() {
    let params: AccountListParams =
        serde_json::from_value(serde_json::json!({})).expect("deserialize params");
    let normalized = params.normalized();

    assert_eq!(normalized.page, 1);
    assert_eq!(normalized.page_size, 5);
}

/// 函数 `account_list_result_serialization_includes_pagination_fields`
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
fn account_list_result_serialization_includes_pagination_fields() {
    let result = AccountListResult {
        items: vec![AccountSummary {
            id: "acc-1".to_string(),
            label: "主账号".to_string(),
            group_name: Some("TEAM".to_string()),
            preferred: true,
            sort: 10,
            status: "active".to_string(),
            status_reason: Some("account_deactivated".to_string()),
            plan_type: Some("team".to_string()),
            plan_type_raw: None,
            has_subscription: Some(true),
            subscription_plan: Some("team".to_string()),
            subscription_expires_at: Some(1_745_000_000),
            subscription_renews_at: Some(1_745_000_000),
            note: Some("主账号".to_string()),
            tags: Some("高频,团队A".to_string()),
        }],
        total: 9,
        page: 2,
        page_size: 3,
    };

    let value = serde_json::to_value(result).expect("serialize account list result");
    let obj = value.as_object().expect("account list result object");
    for key in ["items", "total", "page", "pageSize"] {
        assert!(obj.contains_key(key), "missing key: {key}");
    }
}

/// 函数 `request_log_summary_serialization_includes_trace_route_fields`
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
fn request_log_summary_serialization_includes_trace_route_fields() {
    let summary = RequestLogSummary {
        trace_id: Some("trc_1".to_string()),
        key_id: Some("gk_1".to_string()),
        account_id: Some("acc_1".to_string()),
        initial_account_id: Some("acc_free".to_string()),
        attempted_account_ids: vec!["acc_free".to_string(), "acc_1".to_string()],
        request_path: "/v1/responses".to_string(),
        original_path: Some("/v1/chat/completions".to_string()),
        adapted_path: Some("/v1/responses".to_string()),
        method: "POST".to_string(),
        model: Some("gpt-5.3-codex".to_string()),
        reasoning_effort: Some("high".to_string()),
        effective_service_tier: Some("fast".to_string()),
        response_adapter: Some("OpenAIChatCompletionsJson".to_string()),
        canonical_source: Some("openai_compat".to_string()),
        size_reject_stage: Some("-".to_string()),
        upstream_url: Some("https://api.openai.com/v1".to_string()),
        aggregate_api_supplier_name: Some("方木木提供".to_string()),
        aggregate_api_url: Some("https://api.example.com/v1".to_string()),
        status_code: Some(502),
        duration_ms: Some(1450),
        input_tokens: Some(10),
        cached_input_tokens: Some(0),
        output_tokens: Some(3),
        total_tokens: Some(13),
        reasoning_output_tokens: Some(1),
        estimated_cost_usd: Some(0.12),
        error: Some("internal_error".to_string()),
        created_at: 1,
        ..Default::default()
    };

    let value = serde_json::to_value(summary).expect("serialize request log summary");
    let obj = value.as_object().expect("request log summary object");
    for key in [
        "traceId",
        "initialAccountId",
        "attemptedAccountIds",
        "originalPath",
        "adaptedPath",
        "responseAdapter",
        "canonicalSource",
        "sizeRejectStage",
        "effectiveServiceTier",
        "requestPath",
        "upstreamUrl",
        "durationMs",
        "firstResponseMs",
    ] {
        assert!(obj.contains_key(key), "missing key: {key}");
    }
}

/// 函数 `request_log_list_params_default_to_first_page_with_twenty_items`
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
fn request_log_list_params_default_to_first_page_with_twenty_items() {
    let params: RequestLogListParams =
        serde_json::from_value(serde_json::json!({})).expect("deserialize params");
    let normalized = params.normalized();

    assert_eq!(normalized.page, 1);
    assert_eq!(normalized.page_size, 20);
}

/// 函数 `request_log_list_result_serialization_includes_pagination_fields`
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
fn request_log_list_result_serialization_includes_pagination_fields() {
    let result = RequestLogListResult {
        items: vec![RequestLogSummary {
            trace_id: Some("trc_1".to_string()),
            key_id: Some("gk_1".to_string()),
            account_id: Some("acc_1".to_string()),
            initial_account_id: Some("acc_free".to_string()),
            attempted_account_ids: vec!["acc_free".to_string(), "acc_1".to_string()],
            request_path: "/v1/responses".to_string(),
            original_path: Some("/v1/chat/completions".to_string()),
            adapted_path: Some("/v1/responses".to_string()),
            method: "POST".to_string(),
            model: Some("gpt-5.3-codex".to_string()),
            reasoning_effort: Some("high".to_string()),
            effective_service_tier: Some("fast".to_string()),
            response_adapter: Some("OpenAIChatCompletionsJson".to_string()),
            canonical_source: Some("openai_compat".to_string()),
            size_reject_stage: Some("-".to_string()),
            upstream_url: Some("https://api.openai.com/v1".to_string()),
            aggregate_api_supplier_name: Some("方木木提供".to_string()),
            aggregate_api_url: Some("https://api.example.com/v1".to_string()),
            status_code: Some(200),
            duration_ms: Some(1200),
            input_tokens: Some(10),
            cached_input_tokens: Some(1),
            output_tokens: Some(2),
            total_tokens: Some(12),
            reasoning_output_tokens: Some(1),
            estimated_cost_usd: Some(0.12),
            error: None,
            created_at: 1,
            ..Default::default()
        }],
        total: 88,
        page: 3,
        page_size: 25,
    };

    let value = serde_json::to_value(result).expect("serialize request log list result");
    let obj = value.as_object().expect("request log list result object");
    for key in ["items", "total", "page", "pageSize"] {
        assert!(obj.contains_key(key), "missing key: {key}");
    }
}

/// 函数 `request_log_filter_summary_serialization_uses_camel_case`
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
fn request_log_filter_summary_serialization_uses_camel_case() {
    let result = RequestLogFilterSummaryResult {
        total_count: 120,
        filtered_count: 33,
        success_count: 30,
        error_count: 3,
        total_tokens: 123456,
        total_cost_usd: 12.34,
    };

    let value = serde_json::to_value(result).expect("serialize request log filter summary");
    let obj = value
        .as_object()
        .expect("request log filter summary object");
    for key in [
        "totalCount",
        "filteredCount",
        "successCount",
        "errorCount",
        "totalTokens",
        "totalCostUsd",
    ] {
        assert!(obj.contains_key(key), "missing key: {key}");
    }
}

/// 函数 `api_key_usage_stat_summary_serialization_uses_camel_case`
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
fn api_key_usage_stat_summary_serialization_uses_camel_case() {
    let result = ApiKeyUsageStatSummary {
        key_id: "gk_test".to_string(),
        total_tokens: 123,
        estimated_cost_usd: 4.56,
    };

    let value = serde_json::to_value(result).expect("serialize api key usage stat summary");
    let obj = value
        .as_object()
        .expect("api key usage stat summary object");
    for key in ["keyId", "totalTokens", "estimatedCostUsd"] {
        assert!(obj.contains_key(key), "missing key: {key}");
    }
}
