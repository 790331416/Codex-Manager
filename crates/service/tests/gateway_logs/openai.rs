use super::*;

/// 函数 `gateway_openai_chat_completions_stabilizes_prompt_cache_key_without_conversation_id`
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
fn gateway_openai_chat_completions_stabilizes_prompt_cache_key_without_conversation_id() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-chat-sticky-thread-anchor");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_openai_chat_sticky_anchor",
        "model": "gpt-5.4-mini",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": {
            "input_tokens": 18,
            "output_tokens": 3,
            "total_tokens": 21
        }
    });
    let ok_body = serde_json::to_string(&upstream_response).expect("serialize upstream response");
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_sequence(vec![(200, ok_body.clone()), (200, ok_body)]);
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_chat_sticky_anchor".to_string(),
            label: "openai-chat-sticky-anchor".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_chat_sticky_anchor".to_string()),
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
            account_id: "acc_openai_chat_sticky_anchor".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_chat_sticky_anchor".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_chat_sticky_anchor".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_chat_sticky_anchor";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_chat_sticky_anchor".to_string(),
            name: Some("openai-chat-sticky-anchor".to_string()),
            model_slug: Some("gpt-5.4-mini".to_string()),
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

    for user in ["alice", "bob"] {
        let server = codexmanager_service::start_one_shot_server().expect("start server");
        let request_body = serde_json::json!({
            "model": "gpt-5.4-mini",
            "messages": [{ "role": "user", "content": "hello" }],
            "user": user,
            "stream": false
        });
        let request_body = serde_json::to_string(&request_body).expect("serialize request");
        let (status, gateway_body) = post_http_raw(
            &server.addr,
            "/v1/chat/completions",
            &request_body,
            &[
                ("Content-Type", "application/json"),
                ("Authorization", &format!("Bearer {platform_key}")),
            ],
        );
        assert_eq!(status, 200, "gateway response: {gateway_body}");
        server.join();
    }

    let first = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive first upstream request");
    let second = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive second upstream request");
    upstream_join.join().expect("join upstream");

    let first_body = decode_upstream_request_body(&first);
    let second_body = decode_upstream_request_body(&second);
    let first_payload: serde_json::Value =
        serde_json::from_slice(&first_body).expect("parse first upstream payload");
    let second_payload: serde_json::Value =
        serde_json::from_slice(&second_body).expect("parse second upstream payload");

    let first_anchor = first_payload
        .get("prompt_cache_key")
        .and_then(serde_json::Value::as_str)
        .expect("first prompt_cache_key");
    let second_anchor = second_payload
        .get("prompt_cache_key")
        .and_then(serde_json::Value::as_str)
        .expect("second prompt_cache_key");

    assert_eq!(first.path, "/backend-api/codex/responses");
    assert_eq!(second.path, "/backend-api/codex/responses");
    assert_eq!(first_anchor, second_anchor);
    assert_eq!(
        first.headers.get("x-client-request-id").map(String::as_str),
        Some(first_anchor)
    );
    assert_eq!(
        second
            .headers
            .get("x-client-request-id")
            .map(String::as_str),
        Some(second_anchor)
    );
    assert_eq!(
        first.headers.get("session_id").map(String::as_str),
        Some(first_anchor)
    );
    assert_eq!(
        second.headers.get("session_id").map(String::as_str),
        Some(second_anchor)
    );
}

#[test]
fn gateway_openai_chat_completions_keeps_session_layer_prompt_cache_anchor() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-chat-transparent-sticky-thread-anchor");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_openai_chat_transparent_sticky_anchor",
        "model": "gpt-5.4-mini",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": {
            "input_tokens": 18,
            "output_tokens": 3,
            "total_tokens": 21
        }
    });
    let ok_body = serde_json::to_string(&upstream_response).expect("serialize upstream response");
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_sequence(vec![(200, ok_body.clone()), (200, ok_body)]);
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_chat_transparent_sticky_anchor".to_string(),
            label: "openai-chat-transparent-sticky-anchor".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_chat_transparent_sticky_anchor".to_string()),
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
            account_id: "acc_openai_chat_transparent_sticky_anchor".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_chat_transparent_sticky_anchor".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some(
                "api_access_token_openai_chat_transparent_sticky_anchor".to_string(),
            ),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_chat_transparent_sticky_anchor";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_chat_transparent_sticky_anchor".to_string(),
            name: Some("openai-chat-transparent-sticky-anchor".to_string()),
            model_slug: Some("gpt-5.4-mini".to_string()),
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

    for user in ["alice", "bob"] {
        let server = codexmanager_service::start_one_shot_server().expect("start server");
        let request_body = serde_json::json!({
            "model": "gpt-5.4-mini",
            "messages": [{ "role": "user", "content": "hello" }],
            "user": user,
            "stream": false
        });
        let request_body = serde_json::to_string(&request_body).expect("serialize request");
        let (status, gateway_body) = post_http_raw(
            &server.addr,
            "/v1/chat/completions",
            &request_body,
            &[
                ("Content-Type", "application/json"),
                ("Authorization", &format!("Bearer {platform_key}")),
            ],
        );
        assert_eq!(status, 200, "gateway response: {gateway_body}");
        server.join();
    }

    let first = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive first upstream request");
    let second = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive second upstream request");
    upstream_join.join().expect("join upstream");

    let first_body = decode_upstream_request_body(&first);
    let second_body = decode_upstream_request_body(&second);
    let first_payload: serde_json::Value =
        serde_json::from_slice(&first_body).expect("parse first upstream payload");
    let second_payload: serde_json::Value =
        serde_json::from_slice(&second_body).expect("parse second upstream payload");

    let first_anchor = first_payload
        .get("prompt_cache_key")
        .and_then(serde_json::Value::as_str)
        .expect("first prompt_cache_key");
    let second_anchor = second_payload
        .get("prompt_cache_key")
        .and_then(serde_json::Value::as_str)
        .expect("second prompt_cache_key");

    assert_eq!(first.path, "/backend-api/codex/responses");
    assert_eq!(second.path, "/backend-api/codex/responses");
    assert_eq!(first_anchor, second_anchor);
}

#[test]
fn gateway_openai_responses_does_not_invent_prompt_cache_key_without_anchor() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-responses-transparent-no-anchor");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_openai_transparent_no_anchor",
        "model": "gpt-5.4-mini",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": {
            "input_tokens": 12,
            "output_tokens": 2,
            "total_tokens": 14
        }
    });
    let upstream_body = serde_json::to_string(&upstream_response).expect("serialize upstream");
    let (upstream_addr, upstream_rx, upstream_join) = start_mock_upstream_once(&upstream_body);
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_transparent_no_anchor".to_string(),
            label: "openai-transparent-no-anchor".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_transparent_no_anchor".to_string()),
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
            account_id: "acc_openai_transparent_no_anchor".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_transparent_no_anchor".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_transparent_no_anchor".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_transparent_no_anchor";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_transparent_no_anchor".to_string(),
            name: Some("openai-transparent-no-anchor".to_string()),
            model_slug: Some("gpt-5.4-mini".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.4-mini",
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
    assert_eq!(status, 200, "gateway response: {gateway_body}");
    server.join();

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");

    let upstream_body = decode_upstream_request_body(&captured);
    let upstream_payload: serde_json::Value =
        serde_json::from_slice(&upstream_body).expect("parse upstream payload");

    assert_eq!(captured.path, "/backend-api/codex/responses");
    assert!(upstream_payload.get("prompt_cache_key").is_none());
}

#[test]
fn gateway_openai_chat_completions_keeps_conversation_anchor_over_conflicting_prompt_cache_key() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-chat-explicit-prompt-cache-key");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_openai_explicit_prompt_cache_key",
        "model": "gpt-5.4-mini",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": {
            "input_tokens": 18,
            "output_tokens": 3,
            "total_tokens": 21
        }
    });
    let upstream_body = serde_json::to_string(&upstream_response).expect("serialize upstream");
    let (upstream_addr, upstream_rx, upstream_join) = start_mock_upstream_once(&upstream_body);
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_explicit_prompt_cache_key".to_string(),
            label: "openai-explicit-prompt-cache-key".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_explicit_prompt_cache_key".to_string()),
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
            account_id: "acc_openai_explicit_prompt_cache_key".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_explicit_prompt_cache_key".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some(
                "api_access_token_openai_explicit_prompt_cache_key".to_string(),
            ),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_explicit_prompt_cache_key";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_explicit_prompt_cache_key".to_string(),
            name: Some("openai-explicit-prompt-cache-key".to_string()),
            model_slug: Some("gpt-5.4-mini".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.4-mini",
        "messages": [{ "role": "user", "content": "hello" }],
        "prompt_cache_key": "client_thread_123",
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/chat/completions",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
            ("Conversation_id", "conv_should_not_override"),
            ("x-codex-turn-state", "turn_state_should_survive"),
        ],
    );
    assert_eq!(status, 200, "gateway response: {gateway_body}");
    server.join();

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");

    let upstream_body = decode_upstream_request_body(&captured);
    let upstream_payload: serde_json::Value =
        serde_json::from_slice(&upstream_body).expect("parse upstream payload");

    assert_eq!(captured.path, "/backend-api/codex/responses");
    assert_eq!(
        captured
            .headers
            .get("x-codex-turn-state")
            .map(String::as_str),
        Some("turn_state_should_survive")
    );
    assert_eq!(
        upstream_payload
            .get("prompt_cache_key")
            .and_then(serde_json::Value::as_str),
        Some("conv_should_not_override")
    );
}

/// 函数 `gateway_openai_chat_completions_logs_anthropic_style_cached_tokens`
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
fn gateway_openai_chat_completions_logs_anthropic_style_cached_tokens() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-chat-cache-usage");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_chat_cache_1",
        "model": "gpt-5.3-codex",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "pong" }]
        }],
        "usage": {
            "input_tokens": 24,
            "cache_read_input_tokens": 19,
            "output_tokens": 5,
            "total_tokens": 29
        }
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
            id: "acc_openai_chat_cache".to_string(),
            label: "openai-chat-cache".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_chat_cache".to_string()),
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
            account_id: "acc_openai_chat_cache".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_chat_cache".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_chat_cache".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_chat_cache";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_chat_cache".to_string(),
            name: Some("openai-chat-cache".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "messages": [{ "role": "user", "content": "hello" }],
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/chat/completions",
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
    assert_eq!(value["object"], "chat.completion");
    assert_eq!(value["choices"][0]["message"]["content"], "pong");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses");
    assert_eq!(
        captured
            .headers
            .get("chatgpt-account-id")
            .map(String::as_str),
        Some("chatgpt_openai_chat_cache")
    );

    let mut matched = None;
    for _ in 0..40 {
        let logs = storage
            .list_request_logs(Some("key:=gk_openai_chat_cache"), 20)
            .expect("list request logs");
        matched = logs
            .into_iter()
            .find(|item| item.original_path.as_deref() == Some("/v1/chat/completions"));
        if matched.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let log = matched.expect("openai chat cache request log");
    assert_eq!(log.request_path, "/v1/responses");
    assert_eq!(log.original_path.as_deref(), Some("/v1/chat/completions"));
    assert_eq!(log.adapted_path.as_deref(), Some("/v1/responses"));
    assert_eq!(
        log.response_adapter.as_deref(),
        Some("OpenAIChatCompletionsJson")
    );
    assert_eq!(log.status_code, Some(200));
    assert_eq!(log.input_tokens, Some(24));
    assert_eq!(log.cached_input_tokens, Some(19));
    assert_eq!(log.output_tokens, Some(5));
    assert_eq!(log.total_tokens, Some(29));
}

/// 函数 `gateway_openai_stream_logs_cached_and_reasoning_tokens`
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
fn gateway_openai_stream_logs_cached_and_reasoning_tokens() {
    let _lock = test_env_guard();
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

/// 函数 `gateway_openai_api_base_suppresses_cookie_and_account_headers`
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
fn gateway_openai_api_base_suppresses_cookie_and_account_headers() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-api-base-no-cookie");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_openai_api_base",
        "model": "gpt-5.3-codex",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": { "input_tokens": 5, "output_tokens": 2, "total_tokens": 7 }
    });
    let upstream_response =
        serde_json::to_string(&upstream_response).expect("serialize upstream response");
    let (upstream_addr, upstream_rx, upstream_join) = start_mock_upstream_once(&upstream_response);
    let upstream_base = format!("http://{upstream_addr}/api.openai.com/v1");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_api_base".to_string(),
            label: "openai-api-base".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_api_base".to_string()),
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
            account_id: "acc_openai_api_base".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_api_base".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_api_base".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_api_base";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_api_base".to_string(),
            name: Some("openai-api-base".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");

    assert_eq!(captured.path, "/api.openai.com/v1/responses");
    assert!(!captured.headers.contains_key("cookie"));
    assert!(!captured.headers.contains_key("chatgpt-account-id"));
    assert_eq!(
        captured.headers.get("authorization").map(String::as_str),
        Some("Bearer api_access_token_openai_api_base")
    );
}

/// 函数 `gateway_openai_stream_usage_with_plain_content_type`
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
fn gateway_openai_stream_usage_with_plain_content_type() {
    let _lock = test_env_guard();
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

/// 函数 `gateway_openai_non_stream_sse_with_plain_content_type_is_collapsed_to_json`
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
fn gateway_openai_non_stream_sse_with_plain_content_type_is_collapsed_to_json() {
    let _lock = test_env_guard();
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

/// 函数 `gateway_openai_non_stream_without_usage_keeps_tokens_null`
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
fn gateway_openai_non_stream_without_usage_keeps_tokens_null() {
    let _lock = test_env_guard();
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

/// 函数 `gateway_openai_compact_route_aligns_with_codex_remote_compact_request`
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
fn gateway_openai_compact_route_aligns_with_codex_remote_compact_request() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-compact-route");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "output": [
            {
                "type": "message",
                "role": "user",
                "content": [{ "type": "input_text", "text": "compact me" }]
            },
            {
                "type": "compaction",
                "encrypted_content": "REMOTE_COMPACTED_SUMMARY"
            }
        ]
    });
    let upstream_response =
        serde_json::to_string(&upstream_response).expect("serialize compact response");
    let (upstream_addr, upstream_rx, upstream_join) = start_mock_upstream_once(&upstream_response);
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_compact".to_string(),
            label: "openai-compact".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_compact".to_string()),
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
            account_id: "acc_openai_compact".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_compact".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_compact".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_compact";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_compact".to_string(),
            name: Some("openai-compact".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "compact me",
        "stream": false,
        "store": true,
        "service_tier": "priority"
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/responses/compact",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
            ("session_id", "sess_compact_cli"),
            ("x-codex-window-id", "sess_compact_cli:7"),
            ("x-openai-subagent", "compact"),
            ("x-codex-parent-thread-id", "thread_parent_compact_cli"),
            ("x-codex-other-limit-name", "promo_header_http"),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {gateway_body}");

    let value: serde_json::Value = serde_json::from_str(&gateway_body)
        .unwrap_or_else(|err| panic!("parse response failed: {err}; body={gateway_body}"));
    assert_eq!(value["output"][0]["content"][0]["text"], "compact me");
    assert_eq!(
        value["output"][1]["encrypted_content"],
        "REMOTE_COMPACTED_SUMMARY"
    );

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses/compact");
    assert_eq!(
        captured.headers.get("accept").map(String::as_str),
        Some("application/json")
    );
    assert_eq!(
        captured.headers.get("session_id").map(String::as_str),
        Some("sess_compact_cli")
    );
    assert_eq!(
        captured
            .headers
            .get("x-codex-window-id")
            .map(String::as_str),
        Some("sess_compact_cli:7")
    );
    assert_eq!(
        captured
            .headers
            .get("x-openai-subagent")
            .map(String::as_str),
        Some("compact")
    );
    assert_eq!(
        captured
            .headers
            .get("x-codex-parent-thread-id")
            .map(String::as_str),
        Some("thread_parent_compact_cli")
    );
    assert_eq!(
        captured
            .headers
            .get("x-codex-other-limit-name")
            .map(String::as_str),
        Some("promo_header_http")
    );
    assert!(
        captured.headers.contains_key("user-agent"),
        "compact should carry codex user-agent defaults"
    );
    assert_eq!(
        captured.headers.get("originator").map(String::as_str),
        Some("codex_cli_rs")
    );
    assert!(
        !captured.headers.contains_key("cookie"),
        "compact should not forward upstream cookie"
    );
    assert!(
        !captured.headers.contains_key("conversation_id"),
        "compact should not forward conversation affinity"
    );
    assert!(
        !captured.headers.contains_key("x-codex-turn-state"),
        "compact should not forward turn-state affinity"
    );
    assert!(
        !captured.headers.contains_key("openai-beta"),
        "compact should not force responses streaming beta header"
    );

    let upstream_body = String::from_utf8(captured.body).expect("upstream body utf8");
    assert!(
        !upstream_body.contains("\"stream\":"),
        "unexpected upstream body: {upstream_body}"
    );
    assert!(
        !upstream_body.contains("\"store\":"),
        "unexpected upstream body: {upstream_body}"
    );
    assert!(
        !upstream_body.contains("\"service_tier\":"),
        "unexpected upstream body: {upstream_body}"
    );
    assert!(
        upstream_body.contains("\"reasoning\":{\"effort\":\"high\"}"),
        "unexpected upstream body: {upstream_body}"
    );
}

/// 函数 `gateway_openai_compact_invalid_success_body_is_mapped_to_502`
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
fn gateway_openai_compact_invalid_success_body_is_mapped_to_502() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-compact-invalid-success");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_body =
        "<!doctype html><html><title>Just a moment...</title><body>challenge</body></html>";
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_once_with_content_type(upstream_body, "text/html; charset=utf-8");
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_compact_bad".to_string(),
            label: "openai-compact-bad".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_compact_bad".to_string()),
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
            account_id: "acc_openai_compact_bad".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_compact_bad".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_compact_bad".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_compact_bad";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_compact_bad".to_string(),
            name: Some("openai-compact-bad".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "compact me",
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/responses/compact",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
            ("session_id", "sess_compact_invalid"),
        ],
    );
    server.join();
    assert_eq!(status, 502, "gateway response: {gateway_body}");
    assert!(
        gateway_body.contains("invalid upstream compact response:"),
        "unexpected gateway body: {gateway_body}"
    );
    assert!(
        gateway_body.contains("Just a moment")
            || gateway_body.contains("Cloudflare")
            || gateway_body.contains("HTML 错误页"),
        "unexpected gateway body: {gateway_body}"
    );
    assert!(
        gateway_body.contains("kind=invalid_success_body")
            || gateway_body.contains("kind=cloudflare_challenge")
            || gateway_body.contains("kind=html"),
        "unexpected gateway body: {gateway_body}"
    );

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses/compact");

    let mut matched = None;
    for _ in 0..40 {
        let logs = storage
            .list_request_logs(Some("key:=gk_openai_compact_bad"), 20)
            .expect("list request logs");
        matched = logs
            .into_iter()
            .find(|item| item.request_path == "/v1/responses/compact");
        if matched.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let log = matched.expect("compact invalid success request log");
    assert_eq!(log.status_code, Some(502), "log error: {:?}", log.error);
    assert!(
        log.error
            .as_deref()
            .is_some_and(|err| err.contains("invalid upstream compact response:")),
        "unexpected log error: {:?}",
        log.error
    );
}

/// 函数 `gateway_openai_compact_uses_conversation_id_as_session_anchor`
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
fn gateway_openai_compact_uses_conversation_id_as_session_anchor() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-compact-conversation-anchor");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_body = serde_json::json!({
        "output": [
            {
                "type": "message",
                "id": "msg_compact_anchor",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": "compacted"
                    }
                ]
            }
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
            id: "acc_openai_compact_conversation_anchor".to_string(),
            label: "openai-compact-conversation-anchor".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_compact_conversation_anchor".to_string()),
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
            account_id: "acc_openai_compact_conversation_anchor".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_compact_conversation_anchor".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some(
                "api_access_token_openai_compact_conversation_anchor".to_string(),
            ),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_compact_conversation_anchor";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_compact_conversation_anchor".to_string(),
            name: Some("openai-compact-conversation-anchor".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "compact me",
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/responses/compact",
        &request_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
            ("Conversation_id", "conv_compact_anchor"),
            ("session_id", "legacy_session_should_not_win"),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {gateway_body}");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses/compact");
    assert_eq!(
        captured.headers.get("session_id").map(String::as_str),
        Some("conv_compact_anchor")
    );
    assert_eq!(
        captured
            .headers
            .get("x-openai-subagent")
            .map(String::as_str),
        None
    );
}

/// 函数 `gateway_openai_compact_html_non_success_is_mapped_to_structured_403`
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
fn gateway_openai_compact_html_non_success_is_mapped_to_structured_403() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-compact-html-non-success");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_body =
        "<!doctype html><html><title>Just a moment...</title><body>challenge</body></html>";
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_once_with_status_content_type_and_headers(
            403,
            upstream_body,
            "text/html; charset=utf-8",
            &[
                ("x-oai-request-id", "req-compact-html"),
                ("cf-ray", "ray-compact-html"),
                ("x-openai-authorization-error", "expired_session"),
                (
                    "x-error-json",
                    "eyJlcnJvciI6eyJjb2RlIjoidG9rZW5fZXhwaXJlZCJ9fQ==",
                ),
            ],
        );
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_compact_html".to_string(),
            label: "openai-compact-html".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_compact_html".to_string()),
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
            account_id: "acc_openai_compact_html".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_compact_html".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_compact_html".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_compact_html";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_compact_html".to_string(),
            name: Some("openai-compact-html".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "compact me",
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let gateway_url = format!("http://{}/v1/responses/compact", server.addr);
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client")
        .post(&gateway_url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {platform_key}"))
        .header("session_id", "sess_compact_html_non_success")
        .body(request_body)
        .send()
        .expect("send compact request");
    let status = response.status().as_u16();
    let gateway_body = response.text().expect("read gateway body");
    server.join();
    assert_eq!(status, 502, "gateway response: {gateway_body}");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses/compact");

    let mut matched = None;
    for _ in 0..40 {
        let logs = storage
            .list_request_logs(Some("key:=gk_openai_compact_html"), 20)
            .expect("list request logs");
        matched = logs
            .into_iter()
            .find(|item| item.request_path == "/v1/responses/compact");
        if matched.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let log = matched.expect("compact html non-success request log");
    assert_eq!(log.status_code, Some(502), "log error: {:?}", log.error);
    assert!(
        log.error.as_deref().is_some_and(|err| {
            err.contains("upstream server error")
                || err.contains("invalid upstream compact response:")
        }),
        "unexpected log error: {:?}",
        log.error
    );
}

/// 函数 `gateway_openai_html_non_success_logs_debug_ids_for_responses`
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
fn gateway_openai_html_non_success_logs_debug_ids_for_responses() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-html-non-success-debug-ids");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_body =
        "<!doctype html><html><title>Just a moment...</title><body>challenge</body></html>";
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_once_with_status_content_type_and_headers(
            403,
            upstream_body,
            "text/html; charset=utf-8",
            &[
                ("x-oai-request-id", "req-responses-html"),
                ("cf-ray", "ray-responses-html"),
                ("x-openai-authorization-error", "expired_session"),
            ],
        );
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_html_non_success".to_string(),
            label: "openai-html-non-success".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_html_non_success".to_string()),
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
            account_id: "acc_openai_html_non_success".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_html_non_success".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_html_non_success".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_html_non_success";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_html_non_success".to_string(),
            name: Some("openai-html-non-success".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.3-codex",
        "input": "hello",
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let gateway_url = format!("http://{}/v1/responses", server.addr);
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client")
        .post(&gateway_url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {platform_key}"))
        .body(request_body)
        .send()
        .expect("send responses request");
    let status = response.status().as_u16();
    let gateway_body = response.text().expect("read gateway body");
    server.join();
    assert_eq!(status, 502, "gateway response: {gateway_body}");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(captured.path, "/backend-api/codex/responses");

    let mut matched = None;
    for _ in 0..40 {
        let logs = storage
            .list_request_logs(Some("key:=gk_openai_html_non_success"), 20)
            .expect("list request logs");
        matched = logs
            .into_iter()
            .find(|item| item.request_path == "/v1/responses");
        if matched.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let log = matched.expect("responses html non-success request log");
    assert_eq!(log.status_code, Some(502), "log error: {:?}", log.error);
    assert!(
        log.error
            .as_deref()
            .is_some_and(|err| err.contains("upstream server error")),
        "unexpected log error: {:?}",
        log.error
    );
}

/// 函数 `gateway_models_returns_cached_without_upstream`
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
fn gateway_models_returns_cached_without_upstream() {
    let _lock = test_env_guard();
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

    let cached = ModelsResponse {
        models: vec![ModelInfo {
            slug: "gpt-5.3-codex".to_string(),
            display_name: "GPT-5.3 Codex".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    seed_model_catalog_response(&storage, &cached);

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
        .get("models")
        .and_then(|v| v.as_array())
        .expect("models list data array");
    assert!(
        data.iter()
            .any(|item| item.get("slug").and_then(|v| v.as_str()) == Some("gpt-5.3-codex")),
        "models response missing cached id: {response_body}"
    );
}

/// 函数 `gateway_models_hides_descriptions_for_codex_cli_only`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-12
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn gateway_models_hides_descriptions_for_codex_cli_only() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-models-codex-cli");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());
    let _upstream_guard = EnvGuard::set(
        "CODEXMANAGER_UPSTREAM_BASE_URL",
        "http://127.0.0.1:1/backend-api/codex",
    );

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    let platform_key = "pk_models_cli";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_models_cli".to_string(),
            name: Some("models-cli".to_string()),
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

    let cached = ModelsResponse {
        models: vec![ModelInfo {
            slug: "gpt-5.3-codex".to_string(),
            display_name: "GPT-5.3 Codex".to_string(),
            description: Some("Latest frontier agentic coding model.".to_string()),
            supported_in_api: true,
            ..Default::default()
        }],
        ..Default::default()
    };
    seed_model_catalog_response(&storage, &cached);

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let headers = &[
        ("Authorization", &format!("Bearer {platform_key}")[..]),
        (
            "User-Agent",
            "codex_cli_rs/0.101.0 (Windows 11; x86_64) terminal",
        ),
    ];
    let (status, response_body) = get_http_raw(&server.addr, "/v1/models", headers);
    server.join();
    assert_eq!(status, 200, "gateway response: {response_body}");

    let value: serde_json::Value =
        serde_json::from_str(&response_body).expect("parse models list response");
    let data = value
        .get("models")
        .and_then(|v| v.as_array())
        .expect("models list data array");
    assert!(
        data[0].get("description").is_none(),
        "codex cli response should hide description: {response_body}"
    );

    assert_eq!(
        storage
            .list_model_catalog_models("default")
            .expect("read model rows")[0]
            .description
            .as_deref(),
        Some("Latest frontier agentic coding model.")
    );
}

/// 函数 `apikey_models_refresh_includes_client_version_query`
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
fn apikey_models_refresh_includes_client_version_query() {
    let _lock = test_env_guard();
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
        .and_then(|v| v.get("models"))
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

/// 函数 `apikey_models_refresh_merges_cached_catalog_without_removal`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-12
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn apikey_models_refresh_merges_cached_catalog_without_removal() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-apikey-models-merge");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_body = serde_json::json!({
        "models": [
            {
                "slug": "gpt-5.4",
                "display_name": "GPT-5.4 New",
                "supported_in_api": true,
                "supported_reasoning_levels": [
                    { "effort": "high", "description": "deeper" }
                ]
            }
        ]
    })
    .to_string();
    let (upstream_addr, _upstream_rx, upstream_join) =
        start_mock_upstream_once_with_content_type(&upstream_body, "application/json");
    let upstream_base = format!("http://{upstream_addr}/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();
    let cached = ModelsResponse {
        models: vec![
            ModelInfo {
                slug: "gpt-5.4".to_string(),
                display_name: "GPT-5.4".to_string(),
                description: Some("cached description".to_string()),
                supported_in_api: true,
                input_modalities: vec!["text".to_string(), "image".to_string()],
                ..Default::default()
            },
            ModelInfo {
                slug: "gpt-legacy".to_string(),
                display_name: "GPT Legacy".to_string(),
                supported_in_api: true,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    seed_model_catalog_response(&storage, &cached);

    storage
        .insert_account(&Account {
            id: "acc_models_merge".to_string(),
            label: "models-merge".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_models_merge".to_string()),
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
            account_id: "acc_models_merge".to_string(),
            id_token: String::new(),
            access_token: "access_token_models_merge".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_models_merge".to_string()),
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
    upstream_join.join().expect("join upstream");
    assert_eq!(status, 200, "rpc response: {response_body}");

    let value: serde_json::Value =
        serde_json::from_str(&response_body).expect("parse rpc response body");
    let models = value
        .get("result")
        .and_then(|v| v.get("models"))
        .and_then(|v| v.as_array())
        .expect("models array");
    assert_eq!(models.len(), 2, "unexpected rpc result: {response_body}");
    assert_eq!(
        models[0].get("slug").and_then(|v| v.as_str()),
        Some("gpt-5.4")
    );
    assert_eq!(
        models[1].get("slug").and_then(|v| v.as_str()),
        Some("gpt-legacy")
    );
    assert_eq!(
        models[0].get("display_name").and_then(|v| v.as_str()),
        Some("GPT-5.4 New")
    );
    assert_eq!(
        models[0].get("description").and_then(|v| v.as_str()),
        Some("cached description")
    );
    assert_eq!(
        models[0]
            .get("input_modalities")
            .and_then(|v| v.as_array())
            .map(|items| items.len()),
        Some(2)
    );
    assert_eq!(
        models[0]
            .get("supported_reasoning_levels")
            .and_then(|v| v.as_array())
            .map(|items| items.len()),
        Some(1)
    );

    let row_models = storage
        .list_model_catalog_models("default")
        .expect("read model catalog rows");
    assert_eq!(row_models.len(), 2);
    assert_eq!(row_models[0].slug, "gpt-5.4");
    assert_eq!(row_models[1].slug, "gpt-legacy");
    assert_eq!(
        row_models[0].description.as_deref(),
        Some("cached description")
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&row_models[0].extra_json)
            .expect("parse row extra json"),
        serde_json::json!({})
    );
    let reasoning_levels = storage
        .list_model_catalog_reasoning_levels("default")
        .expect("read reasoning rows");
    assert_eq!(reasoning_levels.len(), 1);
    assert_eq!(reasoning_levels[0].slug, "gpt-5.4");
    assert_eq!(reasoning_levels[0].effort, "high");
    let input_modalities = storage
        .list_model_catalog_input_modalities("default")
        .expect("read modality rows");
    assert_eq!(
        input_modalities
            .iter()
            .filter(|item| item.slug == "gpt-5.4")
            .count(),
        2
    );
    let scope = storage
        .get_model_catalog_scope("default")
        .expect("read scope row")
        .expect("scope row exists");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&scope.extra_json).expect("parse scope extra"),
        serde_json::json!({})
    );
}

/// 函数 `gateway_models_request_stays_on_chatgpt_codex_base`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-04
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn gateway_models_request_stays_on_chatgpt_codex_base() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-models-chatgpt-base");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_body = serde_json::json!({
        "object": "list",
        "data": [{
            "id": "gpt-5.4",
            "object": "model",
            "owned_by": "openai"
        }]
    })
    .to_string();
    let (upstream_addr, upstream_rx, upstream_join) = start_mock_upstream_once(&upstream_body);
    let upstream_base = format!("http://{upstream_addr}/chatgpt.com/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();
    storage
        .insert_account(&Account {
            id: "acc_models_chatgpt_base".to_string(),
            label: "models-chatgpt-base".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_models_chatgpt_base".to_string()),
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
            account_id: "acc_models_chatgpt_base".to_string(),
            id_token: String::new(),
            access_token: "access_token_models_chatgpt_base".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_models_chatgpt_base".to_string()),
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

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join upstream");
    assert_eq!(
        captured.path,
        "/chatgpt.com/backend-api/codex/models?client_version=0.101.0"
    );
}

/// 函数 `gateway_chatgpt_primary_preserves_turn_state_headers_without_openai_fallback`
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
fn gateway_chatgpt_primary_preserves_turn_state_headers_without_openai_fallback() {
    let _lock = test_env_guard();
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
            ("x-client-request-id", "req_dummy"),
            ("x-openai-subagent", "review"),
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
        !first.headers.contains_key("conversation_id"),
        "primary request should not forward conversation_id on codex HTTP responses"
    );
    assert_eq!(
        first.headers.get("x-client-request-id").map(String::as_str),
        Some("conv_dummy")
    );
    assert_eq!(
        first.headers.get("x-openai-subagent").map(String::as_str),
        Some("review")
    );
    assert!(
        first.headers.contains_key("session_id"),
        "primary request should still send a session_id"
    );
}

/// 函数 `gateway_openai_chat_completions_stay_on_chatgpt_codex_base`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-04
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn gateway_openai_chat_completions_stay_on_chatgpt_codex_base() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-openai-chat-chatgpt-base");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_openai_chat_chatgpt_base",
        "model": "gpt-5.4-mini",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": {
            "input_tokens": 12,
            "output_tokens": 4,
            "total_tokens": 16
        }
    });
    let ok_body = serde_json::to_string(&upstream_response).expect("serialize upstream response");
    let (upstream_addr, upstream_rx, upstream_join) = start_mock_upstream_once(&ok_body);
    let upstream_base = format!("http://{upstream_addr}/chatgpt.com/backend-api/codex");
    let _upstream_guard = EnvGuard::set("CODEXMANAGER_UPSTREAM_BASE_URL", &upstream_base);

    let storage = Storage::open(&db_path).expect("open db");
    storage.init().expect("init db");
    let now = now_ts();

    storage
        .insert_account(&Account {
            id: "acc_openai_chat_chatgpt_base".to_string(),
            label: "openai-chat-chatgpt-base".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: Some("chatgpt_openai_chat_chatgpt_base".to_string()),
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
            account_id: "acc_openai_chat_chatgpt_base".to_string(),
            id_token: String::new(),
            access_token: "access_token_openai_chat_chatgpt_base".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_openai_chat_chatgpt_base".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_openai_chat_chatgpt_base";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_chat_chatgpt_base".to_string(),
            name: Some("openai-chat-chatgpt-base".to_string()),
            model_slug: Some("gpt-5.4-mini".to_string()),
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

    let server = codexmanager_service::start_one_shot_server().expect("start server");
    let request_body = serde_json::json!({
        "model": "gpt-5.4-mini",
        "messages": [{ "role": "user", "content": "hello" }],
        "stream": false
    });
    let request_body = serde_json::to_string(&request_body).expect("serialize request");
    let (status, gateway_body) = post_http_raw(
        &server.addr,
        "/v1/chat/completions",
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
    assert_eq!(captured.path, "/chatgpt.com/backend-api/codex/responses");
}

/// 函数 `gateway_chatgpt_primary_drops_turn_state_without_thread_anchor`
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
fn gateway_chatgpt_primary_drops_turn_state_without_thread_anchor() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-chatgpt-primary-turn-state-no-anchor");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_primary_turn_state_no_anchor",
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
            id: "acc_primary_turn_state_no_anchor".to_string(),
            label: "primary-turn-state-no-anchor".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: None,
            workspace_id: Some("ws_primary_turn_state_no_anchor".to_string()),
            group_name: None,
            sort: 1,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
        })
        .expect("insert account");
    storage
        .insert_token(&Token {
            account_id: "acc_primary_turn_state_no_anchor".to_string(),
            id_token: String::new(),
            access_token: "access_token_primary_turn_state_no_anchor".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_primary_turn_state_no_anchor".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_chatgpt_primary_turn_state_no_anchor";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_chatgpt_primary_turn_state_no_anchor".to_string(),
            name: Some("chatgpt-primary-turn-state-no-anchor".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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
    let req_body = r#"{"model":"gpt-5.3-codex","input":"hello","stream":false}"#;
    let (status, response_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        req_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
            ("x-codex-turn-state", "gAAA_orphan_turn_state_blob"),
            ("x-openai-subagent", "review"),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {response_body}");

    let first = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive primary upstream request");
    upstream_join.join().expect("join mock upstream");

    assert!(
        !first.headers.contains_key("x-codex-turn-state"),
        "request without stable thread anchor should not forward turn_state"
    );
}

/// 函数 `gateway_chatgpt_primary_uses_prompt_cache_anchor_for_session_without_inventing_request_id`
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
fn gateway_chatgpt_primary_uses_prompt_cache_anchor_for_session_without_inventing_request_id() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-chatgpt-primary-prompt-cache-anchor");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let upstream_response = serde_json::json!({
        "id": "resp_primary_prompt_cache_anchor",
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
            id: "acc_primary_prompt_cache_anchor".to_string(),
            label: "primary-prompt-cache-anchor".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: None,
            workspace_id: Some("ws_primary_prompt_cache_anchor".to_string()),
            group_name: None,
            sort: 1,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
        })
        .expect("insert account");
    storage
        .insert_token(&Token {
            account_id: "acc_primary_prompt_cache_anchor".to_string(),
            id_token: String::new(),
            access_token: "access_token_primary_prompt_cache_anchor".to_string(),
            refresh_token: String::new(),
            api_key_access_token: Some("api_access_token_primary_prompt_cache_anchor".to_string()),
            last_refresh: now,
        })
        .expect("insert token");

    let platform_key = "pk_chatgpt_primary_prompt_cache_anchor";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_chatgpt_primary_prompt_cache_anchor".to_string(),
            name: Some("chatgpt-primary-prompt-cache-anchor".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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
    let req_body = r#"{"model":"gpt-5.3-codex","input":"hello","stream":false}"#;
    let (status, response_body) = post_http_raw(
        &server.addr,
        "/v1/responses",
        req_body,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {platform_key}")),
            ("Conversation_id", "conv_anchor_primary"),
            ("Session_id", "legacy_session_should_not_win"),
            ("x-codex-turn-state", "legacy_turn_state_should_not_win"),
        ],
    );
    server.join();
    assert_eq!(status, 200, "gateway response: {response_body}");

    let captured = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive upstream request");
    upstream_join.join().expect("join mock upstream");

    assert_eq!(
        captured.headers.get("session_id").map(String::as_str),
        Some("conv_anchor_primary")
    );
    assert_eq!(
        captured
            .headers
            .get("x-client-request-id")
            .map(String::as_str),
        Some("conv_anchor_primary")
    );
    assert_eq!(
        captured
            .headers
            .get("x-codex-turn-state")
            .map(String::as_str),
        Some("legacy_turn_state_should_not_win")
    );
    assert!(!captured.headers.contains_key("conversation_id"));

    let upstream_body = decode_upstream_request_body(&captured);
    let upstream_payload: serde_json::Value =
        serde_json::from_slice(&upstream_body).expect("parse upstream payload");
    assert_eq!(upstream_payload["prompt_cache_key"], "conv_anchor_primary");
}

/// 函数 `gateway_unauthorized_refreshes_access_token_and_retries_once`
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
fn gateway_unauthorized_refreshes_access_token_and_retries_once() {
    let _lock = test_env_guard();
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
    let body_refresh =
        serde_json::to_string(&refresh_response).expect("serialize refresh response");
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

/// 函数 `gateway_invalid_refresh_token_marks_first_account_unavailable_and_fails_over`
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
fn gateway_invalid_refresh_token_marks_first_account_unavailable_and_fails_over() {
    let _lock = test_env_guard();
    let dir = new_test_dir("codexmanager-gateway-invalid-refresh-failover");
    let db_path: PathBuf = dir.join("codexmanager.db");

    let _db_guard = EnvGuard::set("CODEXMANAGER_DB_PATH", db_path.to_string_lossy().as_ref());

    let first_response = serde_json::json!({
        "error": {
            "message": "expired access token",
            "type": "authentication_error"
        }
    });
    let refresh_response = serde_json::json!({
        "error": "invalid_grant"
    });
    let second_response = serde_json::json!({
        "id": "resp_after_failover",
        "model": "gpt-5.3-codex",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "output_text", "text": "ok after failover" }]
        }],
        "usage": { "input_tokens": 5, "output_tokens": 4, "total_tokens": 9 }
    });
    let body_401 = serde_json::to_string(&first_response).expect("serialize first response");
    let body_refresh =
        serde_json::to_string(&refresh_response).expect("serialize refresh response");
    let body_200 = serde_json::to_string(&second_response).expect("serialize second response");
    let (upstream_addr, upstream_rx, upstream_join) =
        start_mock_upstream_sequence(vec![(401, body_401), (401, body_refresh), (200, body_200)]);

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
            id: "acc_refresh_bad".to_string(),
            label: "refresh-bad".to_string(),
            issuer: issuer.clone(),
            chatgpt_account_id: Some("chatgpt_refresh_bad".to_string()),
            workspace_id: None,
            group_name: None,
            sort: 0,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
        })
        .expect("insert first account");
    storage
        .insert_token(&Token {
            account_id: "acc_refresh_bad".to_string(),
            id_token: String::new(),
            access_token: "access_token_old_bad".to_string(),
            refresh_token: "refresh_token_bad".to_string(),
            api_key_access_token: None,
            last_refresh: now,
        })
        .expect("insert first token");

    storage
        .insert_account(&Account {
            id: "acc_refresh_good".to_string(),
            label: "refresh-good".to_string(),
            issuer: issuer.clone(),
            chatgpt_account_id: Some("chatgpt_refresh_good".to_string()),
            workspace_id: None,
            group_name: None,
            sort: 1,
            status: "active".to_string(),
            created_at: now + 1,
            updated_at: now + 1,
        })
        .expect("insert second account");
    storage
        .insert_token(&Token {
            account_id: "acc_refresh_good".to_string(),
            id_token: String::new(),
            access_token: "access_token_good".to_string(),
            refresh_token: String::new(),
            api_key_access_token: None,
            last_refresh: now + 1,
        })
        .expect("insert second token");

    let platform_key = "pk_openai_invalid_refresh_failover";
    storage
        .insert_api_key(&ApiKey {
            id: "gk_openai_invalid_refresh_failover".to_string(),
            name: Some("openai-invalid-refresh-failover".to_string()),
            model_slug: Some("gpt-5.3-codex".to_string()),
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
    let req_body =
        r#"{"model":"gpt-5.3-codex","input":"hello","stream":false,"service_tier":"priority"}"#;
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
    assert_eq!(status, 401, "gateway response: {response_body}");
    assert!(
        response_body.contains("expired access token"),
        "unexpected gateway response: {response_body}"
    );

    let first = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive first upstream request");
    let second = upstream_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive refresh request");
    upstream_join.join().expect("join mock upstream");

    assert_eq!(first.path, "/chatgpt.com/backend-api/codex/responses");
    assert_eq!(
        first.headers.get("authorization").map(String::as_str),
        Some("Bearer access_token_old_bad")
    );
    let first_body =
        String::from_utf8(decode_upstream_request_body(&first)).expect("first body utf8");
    assert!(
        first_body.contains("\"service_tier\":\"priority\""),
        "unexpected first upstream body: {first_body}"
    );
    assert_eq!(second.path, "/oauth/token");
    assert!(
        upstream_rx
            .recv_timeout(Duration::from_millis(500))
            .is_err(),
        "unexpected second-account failover request observed"
    );

    let bad_account = storage
        .find_account_by_id("acc_refresh_bad")
        .expect("find first account")
        .expect("first account exists");
    assert_eq!(bad_account.status, "unavailable");
}
