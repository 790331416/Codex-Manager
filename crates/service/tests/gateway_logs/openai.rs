use super::*;

#[test]
fn gateway_openai_stream_logs_cached_and_reasoning_tokens() {
    let _lock = lock_env();
    let dir = new_test_dir("codexmanager-gateway-openai-stream-usage");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_sse = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_stream_usage_1\",\"model\":\"gpt-5.3-codex\",\"usage\":{\"input_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":90},\"output_tokens\":18,\"total_tokens\":138,\"output_tokens_details\":{\"reasoning_tokens\":7}}}}\n\n",
        "data: [DONE]\n\n"
    );
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_once_with_content_type(upstream_sse, "text/event-stream");
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_stream_usage".to_string(),
            label: "openai-stream-usage".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_stream_usage".to_string()),
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
            account_id: "acc_openai_stream_usage".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_stream_usage".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_stream_usage".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_stream_usage";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_stream_usage".to_string(),
            name: Some("openai-stream-usage".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
            reasoning_effort: Some("high".to_string()),
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
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "stream": true
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {gateway_body}");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses");

    let mut matched = None;
    for _ in 0..40 {
        let logs = storage
            .list_request_logs(Some("key:=gk_openai_stream_usage"), 20)
            .expect("list request logs");
        matched = logs
            .into_iter()
            .find(|item| item.request_path == "/v1/responses");
        if matched.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let log = matched.expect("openai stream request log");
    assert_eq!(log.status_code, Some(200));
    assert_eq!(log.input_tokens, Some(120));
    assert_eq!(log.cached_input_tokens, Some(90));
    assert_eq!(log.output_tokens, Some(18));
    assert_eq!(log.total_tokens, Some(138));
    assert_eq!(log.reasoning_output_tokens, Some(7));
}

#[test]
fn gateway_openai_stream_usage_with_plain_content_type() {
    let _lock = lock_env();
    let dir = new_test_dir("codexmanager-gateway-openai-stream-plain-ct");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_sse = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_stream_usage_plain_1\",\"model\":\"gpt-5.3-codex\",\"usage\":{\"input_tokens\":91,\"input_tokens_details\":{\"cached_tokens\":56},\"output_tokens\":14,\"total_tokens\":105,\"output_tokens_details\":{\"reasoning_tokens\":5}}}}\n\n",
        "data: [DONE]\n\n"
    );
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_once_with_content_type(upstream_sse, "text/plain; charset=utf-8");
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_stream_plain_ct".to_string(),
            label: "openai-stream-plain-ct".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_stream_plain_ct".to_string()),
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
            account_id: "acc_openai_stream_plain_ct".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_stream_plain_ct".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_stream_plain_ct".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_stream_plain_ct";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_stream_plain_ct".to_string(),
            name: Some("openai-stream-plain-ct".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
            reasoning_effort: Some("high".to_string()),
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
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "stream": true
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {gateway_body}");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses");

    let mut matched = None;
    for _ in 0..40 {
        let logs = storage
            .list_request_logs(Some("key:=gk_openai_stream_plain_ct"), 20)
            .expect("list request logs");
        matched = logs
            .into_iter()
            .find(|item| item.request_path == "/v1/responses");
        if matched.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let log = matched.expect("openai stream plain content-type request log");
    assert_eq!(log.status_code, Some(200));
    assert_eq!(log.input_tokens, Some(91));
    assert_eq!(log.cached_input_tokens, Some(56));
    assert_eq!(log.output_tokens, Some(14));
    assert_eq!(log.total_tokens, Some(105));
    assert_eq!(log.reasoning_output_tokens, Some(5));
}

#[test]
fn gateway_openai_non_stream_sse_with_plain_content_type_is_collapsed_to_json() {
    let _lock = lock_env();
    let dir = new_test_dir("codexmanager-gateway-openai-non-stream-plain-ct");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_sse = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_non_stream_plain_ct_1\",\"model\":\"gpt-5.3-codex\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}],\"usage\":{\"input_tokens\":9,\"output_tokens\":2,\"total_tokens\":11}}}\n\n",
        "data: [DONE]\n\n"
    );
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_once_with_content_type(upstream_sse, "text/plain; charset=utf-8");
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_non_stream_plain_ct".to_string(),
            label: "openai-non-stream-plain-ct".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_non_stream_plain_ct".to_string()),
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
            account_id: "acc_openai_non_stream_plain_ct".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_non_stream_plain_ct".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_non_stream_plain_ct".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_non_stream_plain_ct";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_non_stream_plain_ct".to_string(),
            name: Some("openai-non-stream-plain-ct".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
            reasoning_effort: Some("high".to_string()),
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
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {gateway_body}");
    let value: serde_json::Value = serde_json::from_str(&gateway_body)
        .unwrap_or_else(|err| panic!("parse response failed: {err}; body={gateway_body}"));
    assert_eq!(value["id"], "resp_non_stream_plain_ct_1");
    assert_eq!(value["output"][0]["content"][0]["text"], "hello");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses");
}

#[test]
fn gateway_openai_non_stream_without_usage_keeps_tokens_null() {
    let _lock = lock_env();
    let dir = new_test_dir("codexmanager-gateway-openai-no-usage");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_no_usage_1",
        "model": "gpt-5.3-codex",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "pong" }]
        }]
    });
    let upstream_response =
        serde_json::to_string(&upstream_response).expect("serialize upstream response");
    let (upstream_addr, upstream_rx, upstream_join) = start_mock_upstream_once(&upstream_response);
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_no_usage".to_string(),
            label: "openai-no-usage".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_no_usage".to_string()),
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
            account_id: "acc_openai_no_usage".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_no_usage".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_no_usage".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_no_usage";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_no_usage".to_string(),
            name: Some("openai-no-usage".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
            reasoning_effort: Some("high".to_string()),
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
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {gateway_body}");
    let value: serde_json::Value = serde_json::from_str(&gateway_body)
        .unwrap_or_else(|err| panic!("parse response failed: {err}; body={gateway_body}"));
    assert_eq!(value["output"][0]["content"][0]["text"], "pong");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses");

    let mut matched = None;
    for _ in 0..40 {
        let logs = storage
            .list_request_logs(Some("key:=gk_openai_no_usage"), 20)
            .expect("list request logs");
        matched = logs
            .into_iter()
            .find(|item| item.request_path == "/v1/responses");
        if matched.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let log = matched.expect("openai no usage request log");
    assert_eq!(log.status_code, Some(200), "log error: {:?}", log.error);
    assert_eq!(log.input_tokens, None);
    assert_eq!(log.cached_input_tokens, None);
    assert_eq!(log.output_tokens, None);
    assert_eq!(log.total_tokens, None);
    assert_eq!(log.reasoning_output_tokens, None);
}

#[test]
fn gateway_models_returns_cached_without_upstream() {
    let _lock = lock_env();
    let dir = new_test_dir("codexmanager-gateway-models-cache");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    let _upstream_guard = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_BASE_URL",
        "http://127.0.0.1:1/backend-api/codex",
    );

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    let platform_key = "pk_models_cache";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_models_cache".to_string(),
            name: Some("models-cache".to_string()),
            model_slug: None,
            reasoning_effort: None,
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

    let cached = vec![ModelOption {
        slug: "gpt-5.3-codex".to_string(),
        display_name: "GPT-5.3 Codex".to_string(),
    }];
    let items_json = serde_json::to_string(&cached).expect("serialize cached model options");
    storage
        .upsert_model_options_cache("default", &items_json, now_ts())
        .expect("upsert model options cache");

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let (status, response_body) = get_http_raw(
        &server.addr,
        "/v1/models",
        &[("Authorization", &format!("Bearer {platform_key}"))],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {response_body}");

    let value: serde_json::Value =
        serde_json::from_str(&response_body).expect("parse models list response");
    let data = value
        .get("data")
        .and_then(|v| v.as_array())
        .expect("models list data array");
    assert!(
        data.iter()
            .any(|item| item.get("id").and_then(|v| v.as_str()) == Some("gpt-5.3-codex")),
        "models response missing cached id: {response_body}"
    );
}

#[test]
fn apikey_models_refresh_includes_client_version_query() {
    let _lock = lock_env();
    let dir = new_test_dir("codexmanager-apikey-models-client-version");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_body = serde_json::json!({
        "models": [
            { "slug": "gpt-5.3-codex", "display_name": "GPT-5.3 Codex" }
        ]
    })
    .to_string();
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_once_with_content_type(&upstream_body, "application/json");
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_models_client_version".to_string(),
            label: "models-client-version".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_models_client_version".to_string()),
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
            account_id: "acc_models_client_version".to_string(),
            id_token: String::new(),
            access_token: "access_token_models_client_version".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_models_client_version".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "apikey/models",
        "params": { "refreshRemote": true }
    })
    .to_string();
    let (status, response_body) = post_http_raw(
        &server.addr,
        "/rpc",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            (
                "X-CodexManager-Rpc-Token",
                codexmanager_service::rpc_auth_token(),
            ),
        ],
    );
    server.join();
    assert_eq!(status, 200, "rpc response: {response_body}");

    let value: serde_json::Value =
        serde_json::from_str(&response_body).expect("parse rpc response body");
    assert!(
        value.get("error").is_none(),
        "rpc returned error: {response_body}"
    );
    let items = value
        .get("result")
        .and_then(|v| v.get("items"))
        .and_then(|v| v.as_array())
        .expect("models items array");
    assert_eq!(items.len(), 1, "unexpected rpc result: {response_body}");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(
        captured.path,
        "/backend-api/codex/models?client_version=0.101.0"
    );
}

#[test]
fn gateway_chatgpt_primary_preserves_turn_state_headers_without_openai_fallback() {
    let _lock = lock_env();
    let dir = new_test_dir("codexmanager-gateway-chatgpt-primary-turn-state");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_primary_ok",
        "model": "gpt-5.3-codex",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": { "input_tokens": 3, "output_tokens": 2, "total_tokens": 5 }
    });
    let ok_body = serde_json::to_string(&upstream_response).expect("serialize upstream response");
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_sequence(vec![(200, ok_body)]);

    let upstream_base = format!("http://{upstream_addr}/chatgpt.com/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_primary".to_string(),
            label: "primary".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: None,
            workspace_id: Some("ws_primary".to_string()),
            group_name: None,
            sort: 1,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
        })
        .expect("insert account");
    storage
        .insert_token(&Token {
            account_id: "acc_primary".to_string(),
            id_token: String::new(),
            access_token: "access_token_primary".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_primary".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_chatgpt_primary_turn_state";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_chatgpt_primary_turn_state".to_string(),
            name: Some("chatgpt-primary-turn-state".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
            reasoning_effort: None,
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
    let req_body = r#"{"model":"gpt-5.3-codex","input":"hello","stream":false}"#;
    let (status, response_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        req_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
            ("x-codex-turn-state", "gAAA_dummy_turn_state_blob"),
            ("Conversation_id", "conv_dummy"),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {response_body}");

    let first = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive primary upstream request");
    upstream_join.join().expect("join mock upstream");

    assert!(
        first.headers.contains_key("x-codex-turn-state"),
        "primary request should forward turn_state for same-account flow"
    );
    assert!(
        first.headers.contains_key("conversation_id"),
        "primary request should preserve conversation_id without fallback rewriting"
    );
    assert!(
        first.headers.contains_key("session_id"),
        "primary request should still send a session_id"
    );
}

#[test]
fn gateway_unauthorized_refreshes_access_token_and_retries_once() {
    let _lock = lock_env();
    let dir = new_test_dir("codexmanager-gateway-openai-unauthorized-refresh");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let first_response = serde_json::json!({
        "error": {
            "message": "expired access token",
            "type": "authentication_error"
        }
    });
    let refresh_response = serde_json::json!({
        "access_token": "access_token_refreshed",
        "refresh_token": "refresh_token_refreshed"
    });
    let second_response = serde_json::json!({
        "id": "resp_after_refresh",
        "model": "gpt-5.3-codex",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok after refresh" }]
        }],
        "usage": { "input_tokens": 4, "output_tokens": 3, "total_tokens": 7 }
    });
    let body_401 = serde_json::to_string(&first_response).expect("serialize first response");
    let body_refresh = serde_json::to_string(&refresh_response).expect("serialize refresh response");
    let body_200 = serde_json::to_string(&second_response).expect("serialize second response");
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_sequence(vec![(401, body_401), (200, body_refresh), (200, body_200)]);

    let upstream_base = format!("http://{upstream_addr}/chatgpt.com/backend-api/codex");
    let issuer = format!("http://{upstream_addr}");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);
    let _issuer_guard = EnvGuard::set("CODEXMANAGER_ISSUER", &issuer);
    let _client_id_guard = EnvGuard::set("CODEXMANAGER_CLIENT_ID", "client-test-refresh");

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_refresh".to_string(),
            label: "refresh".to_string(),
            issuer: issuer.clone(),
            chatgpt_account_id: Some("chatgpt_refresh".to_string()),
            workspace_id: None,
            group_name: None,
            sort: 1,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
        })
        .expect("insert account");
    storage
        .insert_token(&Token {
            account_id: "acc_refresh".to_string(),
            id_token: String::new(),
            access_token: "access_token_old".to_string(),
            refresh_token: "refresh_token_old".to_string(),
            api_key_access_token: None,
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_unauthorized_refresh";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_unauthorized_refresh".to_string(),
            name: Some("openai-unauthorized-refresh".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
            reasoning_effort: None,
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
    let req_body = r#"{"model":"gpt-5.3-codex","input":"hello","stream":false}"#;
    let (status, response_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        req_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {response_body}");

    let first = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive first upstream request");
    let second = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive refresh request");
    let third = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive retried upstream request");
    upstream_join.join().expect("join mock upstream");

    assert_eq!(first.path, "/chatgpt.com/backend-api/codex/responses");
    assert_eq!(
        first.headers.get("authorization").map(String::as_str),
        Some("Bearer access_token_old")
    );
    assert_eq!(second.path, "/oauth/token");
    let refresh_body = String::from_utf8(second.body.clone()).expect("refresh body utf8");
    assert!(
        refresh_body.contains("grant_type=refresh_token"),
        "unexpected refresh body: {refresh_body}"
    );
    assert!(
        refresh_body.contains("refresh_token=refresh_token_old"),
        "unexpected refresh body: {refresh_body}"
    );
    assert_eq!(third.path, "/chatgpt.com/backend-api/codex/responses");
    assert_eq!(
        third.headers.get("authorization").map(String::as_str),
        Some("Bearer access_token_refreshed")
    );
}
