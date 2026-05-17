use super::*;
use codexmanager_core::storage::RequestTokenStat;

const MISSING_AUTH_JSON_OPENAI_API_KEY_ERROR: &str =
    "配置错误：未配置auth.json的OPENAI_API_KEY(invalid api key)";

/// 函数 `gateway_logs_invalid_api_key_error`
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
fn gateway_logs_invalid_api_key_error() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-logs");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let server = TestServer::start();
    let req_body = r#"{"model":"gpt-5.3-codex","input":"hello"}"#;
    let (status, body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        req_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", "Bearer invalid-platform-key"),
        ],
    );
    assert_eq!(status, 403);
    assert!(
        body.contains("invalid api key"),
        "gateway should return raw upstream message, got {body}"
    );
    assert!(
        !body.contains("未配置auth.json"),
        "gateway response should not expose bilingual log text, got {body}"
    );

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init schema");
    let mut logs = Vec::new();
    for _ in 0..40 {
        logs = storage
            .list_request_logs(None, 100)
            .expect("list request logs");
        if !logs.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let found = logs.iter().any(|item| {
        item.request_path == "/v1/responses"
            && item.status_code == Some(403)
            && item.input_tokens.is_none()
            && item.cached_input_tokens.is_none()
            && item.output_tokens.is_none()
            && item.total_tokens.is_none()
            && item.reasoning_output_tokens.is_none()
            && item.error.as_deref() == Some(MISSING_AUTH_JSON_OPENAI_API_KEY_ERROR)
    });
    assert!(
        found,
        "expected missing auth.json OPENAI_API_KEY request to be logged, got {:?}",
        logs.iter()
            .map(|v| (&v.request_path, v.status_code, v.error.as_deref()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn gateway_rejects_api_key_after_quota_limit() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-key-quota");
    let db_path: PathBuf = dir.join("codexmanager.db");
    let _guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let platform_key = "pk_quota_limit_reached";
    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init schema");
    let now = now_ts();
    storage
        .insert_api_key(&ApiKey {
            id: "gk_quota_limit_reached".to_string(),
            name: Some("quota-limit".to_string()),
            model_slug: None,
            reasoning_effort: None,
            service_tier: None,
            rotation_strategy: "account_rotation".to_string(),
            aggregate_api_id: None,
            account_plan_filter: None,
            aggregate_api_url: None,
            client_type: "codex".to_string(),
            protocol_type: "openai_compat".to_string(),
            auth_scheme: "authorization_bearer".to_string(),
            upstream_base_url: None,
            static_headers_json: None,
            key_hash: hash_platform_key_for_test(platform_key),
            status: "active".to_string(),
            created_at: now,
            last_used_at: None,
        })
        .expect("insert api key");
    storage
        .upsert_api_key_quota_limit("gk_quota_limit_reached", Some(100))
        .expect("upsert quota");
    storage
        .insert_request_token_stat(&RequestTokenStat {
            request_log_id: 1,
            key_id: Some("gk_quota_limit_reached".to_string()),
            total_tokens: Some(100),
            created_at: now,
            ..RequestTokenStat::default()
        })
        .expect("insert token stat");

    let server = TestServer::start();
    let req_body = r#"{"model":"gpt-5.3-codex","input":"hello"}"#;
    let (status, body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        req_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    assert_eq!(status, 429, "response body: {body}");
    assert!(
        body.contains("quota") || body.contains("额度"),
        "gateway should report quota exhaustion, got {body}"
    );
}

#[test]
fn gateway_reports_wallet_quota_exhaustion_in_chinese() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-wallet-quota");
    let db_path: PathBuf = dir.join("codexmanager.db");
    let _guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let platform_key = "pk_wallet_quota_exhausted";
    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init schema");
    let now = now_ts();
    storage
        .set_app_setting("distribution.enabled", "true", now)
        .expect("enable distribution");
    storage
        .insert_app_user(&AppUser {
            id: "usr_wallet_quota_exhausted".to_string(),
            username: "wallet-quota-member".to_string(),
            display_name: None,
            password_hash: "test-password-hash".to_string(),
            role: "member".to_string(),
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
            last_login_at: None,
        })
        .expect("insert app user");
    storage
        .insert_api_key(&ApiKey {
            id: "gk_wallet_quota_exhausted".to_string(),
            name: Some("wallet-quota".to_string()),
            model_slug: None,
            reasoning_effort: None,
            service_tier: None,
            rotation_strategy: "account_rotation".to_string(),
            aggregate_api_id: None,
            account_plan_filter: None,
            aggregate_api_url: None,
            client_type: "codex".to_string(),
            protocol_type: "openai_compat".to_string(),
            auth_scheme: "authorization_bearer".to_string(),
            upstream_base_url: None,
            static_headers_json: None,
            key_hash: hash_platform_key_for_test(platform_key),
            status: "active".to_string(),
            created_at: now,
            last_used_at: None,
        })
        .expect("insert api key");
    storage
        .upsert_api_key_owner(&ApiKeyOwner {
            key_id: "gk_wallet_quota_exhausted".to_string(),
            owner_kind: "user".to_string(),
            owner_user_id: Some("usr_wallet_quota_exhausted".to_string()),
            project_id: None,
            updated_at: now,
        })
        .expect("insert api key owner");
    storage
        .ensure_wallet_for_owner(
            "wlt_wallet_quota_exhausted",
            "user",
            "usr_wallet_quota_exhausted",
        )
        .expect("ensure zero wallet");

    let server = TestServer::start();
    let req_body = r#"{"model":"gpt-5.3-codex","input":"hello"}"#;
    let (status, body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        req_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    assert_eq!(status, 402, "response body: {body}");
    assert!(
        body.contains("额度不足，请联系管理员"),
        "gateway should report wallet quota exhaustion in Chinese, got {body}"
    );
    assert!(
        !body.contains("Too Many Requests"),
        "wallet quota exhaustion must not look like retryable upstream throttling, got {body}"
    );
}

#[test]
fn gateway_reports_platform_model_route_errors() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-model-route-errors");
    let db_path: PathBuf = dir.join("codexmanager.db");
    let _guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let platform_key = "pk_model_route_errors";
    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init schema");
    let now = now_ts();
    storage
        .insert_api_key(&ApiKey {
            id: "gk_model_route_errors".to_string(),
            name: Some("model-route-errors".to_string()),
            model_slug: None,
            reasoning_effort: None,
            service_tier: None,
            rotation_strategy: "account_rotation".to_string(),
            aggregate_api_id: None,
            account_plan_filter: None,
            aggregate_api_url: None,
            client_type: "codex".to_string(),
            protocol_type: "openai_compat".to_string(),
            auth_scheme: "authorization_bearer".to_string(),
            upstream_base_url: None,
            static_headers_json: None,
            key_hash: hash_platform_key_for_test(platform_key),
            status: "active".to_string(),
            created_at: now,
            last_used_at: None,
        })
        .expect("insert api key");

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let (status, body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        r#"{"model":"missing-platform","input":"hello"}"#,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 404, "response body: {body}");
    assert!(
        body.contains("model_not_found"),
        "gateway should report missing platform model, got {body}"
    );

    seed_model_catalog_models(&storage, &["gpt-platform"]);

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let (status, body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        r#"{"model":"gpt-platform","input":"hello"}"#,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 503, "response body: {body}");
    assert!(
        body.contains("model_unavailable"),
        "gateway should report platform model without enabled mappings, got {body}"
    );
}

#[test]
fn gateway_rewrites_account_pool_model_from_enabled_mapping() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-account-model-mapping");
    let db_path: PathBuf = dir.join("codexmanager.db");
    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let response = serde_json::json!({
        "id": "resp_model_mapping",
        "model": "gpt-upstream",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": { "input_tokens": 2, "output_tokens": 1, "total_tokens": 3 }
    });
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_once(&serde_json::to_string(&response).expect("serialize response"));
    let upstream_base = format!("http://{upstream_addr}/v1");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let platform_key = "pk_account_model_mapping";
    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init schema");
    let now = now_ts();
    seed_model_catalog_models(&storage, &["gpt-platform"]);
    storage
        .insert_account(&Account {
            id: "acc_model_mapping".to_string(),
            label: "mapping account".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: None,
            workspace_id: Some("ws_model_mapping".to_string()),
            group_name: None,
            sort: 1,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
        })
        .expect("insert account");
    storage
        .insert_token(&Token {
            account_id: "acc_model_mapping".to_string(),
            id_token: String::new(),
            access_token: "access_token_model_mapping".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_model_mapping".to_string()),
            last_refresh: now,
        })
        .expect("insert token");
    storage
        .insert_api_key(&ApiKey {
            id: "gk_account_model_mapping".to_string(),
            name: Some("account-model-mapping".to_string()),
            model_slug: None,
            reasoning_effort: None,
            service_tier: None,
            rotation_strategy: "account_rotation".to_string(),
            aggregate_api_id: None,
            account_plan_filter: None,
            aggregate_api_url: None,
            client_type: "codex".to_string(),
            protocol_type: "openai_compat".to_string(),
            auth_scheme: "authorization_bearer".to_string(),
            upstream_base_url: None,
            static_headers_json: None,
            key_hash: hash_platform_key_for_test(platform_key),
            status: "active".to_string(),
            created_at: now,
            last_used_at: None,
        })
        .expect("insert api key");
    storage
        .upsert_model_source_model(&ModelSourceModel {
            source_kind: "openai_account".to_string(),
            source_id: "acc_model_mapping".to_string(),
            upstream_model: "gpt-upstream".to_string(),
            display_name: None,
            status: "available".to_string(),
            discovery_kind: "manual".to_string(),
            last_synced_at: Some(now),
            extra_json: "{}".to_string(),
            created_at: now,
            updated_at: now,
        })
        .expect("upsert source model");
    storage
        .upsert_model_source_mapping(&ModelSourceMapping {
            id: "map_account_model_mapping".to_string(),
            platform_model_slug: "gpt-platform".to_string(),
            source_kind: "openai_account".to_string(),
            source_id: "acc_model_mapping".to_string(),
            upstream_model: "gpt-upstream".to_string(),
            enabled: true,
            priority: 0,
            weight: 1,
            billing_model_slug: None,
            created_at: now,
            updated_at: now,
        })
        .expect("upsert mapping");

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let (status, response_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        r#"{"model":"gpt-platform","input":"hello","stream":false}"#,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {response_body}");

    let upstream_request = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    let request_body: serde_json::Value =
        serde_json::from_slice(&decode_upstream_request_body(&upstream_request))
            .expect("parse upstream request body");
    assert_eq!(
        request_body
            .get("model")
            .and_then(serde_json::Value::as_str),
        Some("gpt-upstream")
    );
    let logs = storage
        .list_request_logs(None, 10)
        .expect("list request logs");
    let log = logs.first().expect("request log should be written");
    assert_eq!(log.model.as_deref(), Some("gpt-platform"));
    assert_eq!(log.upstream_model.as_deref(), Some("gpt-upstream"));
    assert_eq!(log.actual_source_kind.as_deref(), Some("openai_account"));
    assert_eq!(log.actual_source_id.as_deref(), Some("acc_model_mapping"));
}

/// 函数 `gateway_tolerates_non_ascii_turn_metadata_header`
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
fn gateway_tolerates_non_ascii_turn_metadata_header() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-logs-nonascii");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let server = TestServer::start();
    let req_body = r#"{"model":"gpt-5.3-codex","input":"hello"}"#;
    let metadata = r#"{"workspaces":{"D:\\MyComputer\\own\\GPTTeam相关\\CodexManager\\CodexManager":{"latest_git_commit_hash":"abc123"}}}"#;
    let (status, body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        req_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", "Bearer invalid-platform-key"),
            ("x-codex-turn-metadata", metadata),
        ],
    );
    assert_eq!(status, 403, "response body: {body}");
}

#[test]
fn gateway_openai_responses_auto_compacts_oversized_context_before_primary_send() {
    fn build_large_input_items(count: usize, chars_per_item: usize) -> Vec<serde_json::Value> {
        (0..count)
            .map(|idx| {
                serde_json::json!({
                    "type": "message",
                    "role": if idx % 2 == 0 { "user" } else { "assistant" },
                    "content": [{
                        "type": "input_text",
                        "text": format!("item-{idx}-{}", "x".repeat(chars_per_item)),
                    }]
                })
            })
            .collect()
    }

    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-auto-compact");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let compact_response = serde_json::json!({
        "output": [
            {
                "type": "compaction",
                "encrypted_content": "REMOTE_COMPACTED_SUMMARY"
            }
        ]
    });
    let final_response = serde_json::json!({
        "id": "resp_openai_auto_compact",
        "model": "gpt-5.4",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "compacted ok" }]
        }],
        "usage": {
            "input_tokens": 120000,
            "output_tokens": 3,
            "total_tokens": 120003
        }
    });
    let compact_body =
        serde_json::to_string(&compact_response).expect("serialize compact response");
    let final_body = serde_json::to_string(&final_response).expect("serialize final response");
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_sequence(vec![(200, compact_body), (200, final_body)]);
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_auto_compact".to_string(),
            label: "openai-auto-compact".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_auto_compact".to_string()),
            workspace_id: None,
            group_name: None,
            sort: 0,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
        })
        .expect("insert account");
    storage
        .insert_token(&Token {
            account_id: "acc_openai_auto_compact".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_auto_compact".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_auto_compact".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_auto_compact";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_auto_compact".to_string(),
            name: Some("openai-auto-compact".to_string()),
            model_slug: Some("gpt-5.4".to_string()),
            reasoning_effort: Some("high".to_string()),
            service_tier: None,
            rotation_strategy: "account_rotation".to_string(),
            aggregate_api_id: None,
            account_plan_filter: None,
            aggregate_api_url: None,
            client_type: "codex".to_string(),
            protocol_type: "openai_compat".to_string(),
            auth_scheme: "authorization_bearer".to_string(),
            upstream_base_url: None,
            static_headers_json: None,
            key_hash: hash_platform_key_for_test(platform_key),
            status: "active".to_string(),
            created_at: now,
            last_used_at: None,
        })
        .expect("insert api key");

    let request_body = serde_json::json!({
        "model": "gpt-5.4",
        "instructions": "stay focused",
        "input": build_large_input_items(12, 60_000),
        "tools": [],
        "parallel_tool_calls": true,
        "reasoning": { "effort": "high" },
        "text": { "verbosity": "low" },
        "stream": false,
        "store": true,
        "service_tier": "priority",
        "include": ["reasoning.encrypted_content"],
        "prompt_cache_key": "thread-auto-compact",
        "client_metadata": { "requestKind": "stress" },
        "tool_choice": "auto"
    });

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        &serde_json::to_string(&request_body).expect("serialize request"),
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {gateway_body}");

    let compact_request = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive compact upstream request");
    let final_request = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive final upstream request");
    upstream_join.join().expect("join upstream");

    assert_eq!(compact_request.path, "/backend-api/codex/responses/compact");
    assert_eq!(final_request.path, "/backend-api/codex/responses");

    let compact_payload: serde_json::Value =
        serde_json::from_slice(&decode_upstream_request_body(&compact_request))
            .expect("parse compact payload");
    let final_payload: serde_json::Value =
        serde_json::from_slice(&decode_upstream_request_body(&final_request))
            .expect("parse final payload");

    assert!(compact_payload.get("stream").is_none());
    assert!(compact_payload.get("store").is_none());
    assert!(compact_payload.get("service_tier").is_none());
    assert!(compact_payload.get("include").is_none());
    assert!(compact_payload.get("prompt_cache_key").is_none());
    assert!(compact_payload.get("client_metadata").is_none());
    assert!(compact_payload.get("tool_choice").is_none());
    assert!(
        compact_payload["input"]
            .as_array()
            .is_some_and(|items| !items.is_empty()),
        "compact payload should carry prefix input: {compact_payload}"
    );

    let final_input = final_payload["input"]
        .as_array()
        .expect("final input array");
    assert!(
        final_input.iter().any(|item| {
            item.get("type").and_then(|value| value.as_str()) == Some("compaction")
                && item
                    .get("encrypted_content")
                    .and_then(|value| value.as_str())
                    == Some("REMOTE_COMPACTED_SUMMARY")
        }),
        "final payload should include remote compacted content: {final_payload}"
    );
    assert!(
        final_input
            .last()
            .and_then(|item| item["content"][0]["text"].as_str())
            .is_some_and(|text| text.contains("item-11-")),
        "latest raw tail should stay in request: {final_payload}"
    );
    assert_eq!(
        final_payload
            .get("service_tier")
            .and_then(|value| value.as_str()),
        Some("priority")
    );
}
