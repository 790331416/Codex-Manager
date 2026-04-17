use super::Storage;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

/// 函数 `temp_db_path`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - name: 参数 name
///
/// # 返回
/// 返回函数执行结果
fn temp_db_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("codexmanager-{name}-{}-{nanos}.db", process::id()))
}

/// 函数 `init_tracks_schema_migrations_and_is_idempotent`
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
fn init_tracks_schema_migrations_and_is_idempotent() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage.init().expect("first init");
    storage.init().expect("second init");

    let applied_001: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '001_init'",
            [],
            |row| row.get(0),
        )
        .expect("count 001 migration");
    assert_eq!(applied_001, 1);

    let applied_005: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '005_request_logs'",
            [],
            |row| row.get(0),
        )
        .expect("count 005 migration");
    assert_eq!(applied_005, 1);

    let applied_012: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '012_request_logs_search_indexes'",
            [],
            |row| row.get(0),
        )
        .expect("count 012 migration");
    assert_eq!(applied_012, 1);

    let applied_013: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '013_drop_accounts_note_tags'",
            [],
            |row| row.get(0),
        )
        .expect("count 013 migration");
    assert_eq!(applied_013, 1);
    let applied_014: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '014_drop_accounts_workspace_name'",
            [],
            |row| row.get(0),
        )
        .expect("count 014 migration");
    assert_eq!(applied_014, 1);
    let applied_015: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '015_api_key_profiles'",
            [],
            |row| row.get(0),
        )
        .expect("count 015 migration");
    assert_eq!(applied_015, 1);
    let applied_016: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '016_api_keys_key_hash_index'",
            [],
            |row| row.get(0),
        )
        .expect("count 016 migration");
    assert_eq!(applied_016, 1);
    let applied_017: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '017_usage_snapshots_captured_id_index'",
            [],
            |row| row.get(0),
        )
        .expect("count 017 migration");
    assert_eq!(applied_017, 1);
    let applied_018: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '018_accounts_sort_updated_at_index'",
            [],
            |row| row.get(0),
        )
        .expect("count 018 migration");
    assert_eq!(applied_018, 1);
    let applied_022: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '022_request_token_stats'",
            [],
            |row| row.get(0),
        )
        .expect("count 022 migration");
    assert_eq!(applied_022, 1);
    let applied_023: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '023_request_token_stats_total_tokens'",
            [],
            |row| row.get(0),
        )
        .expect("count 023 migration");
    assert_eq!(applied_023, 1);
    let applied_025: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '025_tokens_refresh_schedule'",
            [],
            |row| row.get(0),
        )
        .expect("count 025 migration");
    assert_eq!(applied_025, 1);
    let applied_027: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '027_request_logs_trace_context'",
            [],
            |row| row.get(0),
        )
        .expect("count 027 migration");
    assert_eq!(applied_027, 1);
    let applied_028: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '028_request_logs_drop_legacy_usage_columns'",
            [],
            |row| row.get(0),
        )
        .expect("count 028 migration");
    assert_eq!(applied_028, 1);
    let applied_029: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '029_app_settings'",
            [],
            |row| row.get(0),
        )
        .expect("count 029 migration");
    assert_eq!(applied_029, 1);
    let applied_031: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '031_request_logs_duration_ms'",
            [],
            |row| row.get(0),
        )
        .expect("count 031 migration");
    assert_eq!(applied_031, 1);
    let applied_032: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '032_request_logs_attempt_chain'",
            [],
            |row| row.get(0),
        )
        .expect("count 032 migration");
    assert_eq!(applied_032, 1);
    let applied_033: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '033_login_sessions_workspace_id'",
            [],
            |row| row.get(0),
        )
        .expect("count 033 migration");
    assert_eq!(applied_033, 1);
    let applied_034: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '034_conversation_bindings'",
            [],
            |row| row.get(0),
        )
        .expect("count 034 migration");
    assert_eq!(applied_034, 1);
    let applied_035: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '035_api_key_profiles_service_tier'",
            [],
            |row| row.get(0),
        )
        .expect("count 035 migration");
    assert_eq!(applied_035, 1);
    let applied_043: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '043_request_logs_effective_service_tier'",
            [],
            |row| row.get(0),
        )
        .expect("count 043 migration");
    assert_eq!(applied_043, 1);
    let applied_050: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '050_api_key_profiles_drop_azure_protocol'",
            [],
            |row| row.get(0),
        )
        .expect("count 050 migration");
    assert_eq!(applied_050, 1);
    let applied_052: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '052_account_subscriptions'",
            [],
            |row| row.get(0),
        )
        .expect("count 052 migration");
    assert_eq!(applied_052, 1);

    assert!(!storage
        .has_column("accounts", "note")
        .expect("check accounts.note"));
    assert!(!storage
        .has_column("accounts", "tags")
        .expect("check accounts.tags"));
    assert!(!storage
        .has_column("accounts", "workspace_name")
        .expect("check accounts.workspace_name"));
    assert!(storage
        .has_column("request_token_stats", "total_tokens")
        .expect("check request_token_stats.total_tokens"));
    assert!(storage
        .has_column("tokens", "next_refresh_at")
        .expect("check tokens.next_refresh_at"));
    assert!(storage
        .has_column("request_logs", "trace_id")
        .expect("check request_logs.trace_id"));
    assert!(storage
        .has_column("request_logs", "original_path")
        .expect("check request_logs.original_path"));
    assert!(storage
        .has_column("request_logs", "adapted_path")
        .expect("check request_logs.adapted_path"));
    assert!(storage
        .has_column("request_logs", "response_adapter")
        .expect("check request_logs.response_adapter"));
    assert!(storage
        .has_column("request_logs", "duration_ms")
        .expect("check request_logs.duration_ms"));
    assert!(storage
        .has_column("request_logs", "first_response_ms")
        .expect("check request_logs.first_response_ms"));
    assert!(storage
        .has_column("request_logs", "initial_account_id")
        .expect("check request_logs.initial_account_id"));
    assert!(storage
        .has_column("request_logs", "attempted_account_ids_json")
        .expect("check request_logs.attempted_account_ids_json"));
    assert!(storage
        .has_column("request_logs", "initial_aggregate_api_id")
        .expect("check request_logs.initial_aggregate_api_id"));
    assert!(storage
        .has_column("request_logs", "attempted_aggregate_api_ids_json")
        .expect("check request_logs.attempted_aggregate_api_ids_json"));
    assert!(storage
        .has_column("request_logs", "effective_service_tier")
        .expect("check request_logs.effective_service_tier"));
    assert!(storage
        .has_column("app_settings", "value")
        .expect("check app_settings.value"));
    assert!(storage
        .has_column("login_sessions", "workspace_id")
        .expect("check login_sessions.workspace_id"));
    assert!(storage
        .has_column("conversation_bindings", "thread_anchor")
        .expect("check conversation_bindings.thread_anchor"));
    assert!(storage
        .has_column("conversation_bindings", "last_switch_reason")
        .expect("check conversation_bindings.last_switch_reason"));
    assert!(storage
        .has_table("account_subscriptions")
        .expect("check account_subscriptions table"));
    assert!(storage
        .has_column("account_subscriptions", "has_subscription")
        .expect("check account_subscriptions.has_subscription"));
    assert!(storage
        .has_column("account_subscriptions", "plan_type")
        .expect("check account_subscriptions.plan_type"));
    assert!(storage
        .has_column("account_subscriptions", "expires_at")
        .expect("check account_subscriptions.expires_at"));
    assert!(storage
        .has_column("account_subscriptions", "renews_at")
        .expect("check account_subscriptions.renews_at"));
    assert!(storage
        .has_column("api_key_profiles", "service_tier")
        .expect("check api_key_profiles.service_tier"));
    assert!(!storage
        .has_column("request_logs", "input_tokens")
        .expect("check request_logs.input_tokens"));
    assert!(!storage
        .has_column("request_logs", "output_tokens")
        .expect("check request_logs.output_tokens"));
    assert!(!storage
        .has_column("request_logs", "estimated_cost_usd")
        .expect("check request_logs.estimated_cost_usd"));
    assert!(!storage
        .has_column("request_logs", "cached_input_tokens")
        .expect("check request_logs.cached_input_tokens"));
    assert!(!storage
        .has_column("request_logs", "reasoning_output_tokens")
        .expect("check request_logs.reasoning_output_tokens"));
}

/// 函数 `file_open_enables_wal_and_normal_synchronous`
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
fn file_open_enables_wal_and_normal_synchronous() {
    let path = temp_db_path("sqlite-pragmas");
    let storage = Storage::open(&path).expect("open file storage");

    let journal_mode: String = storage
        .conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read journal mode");
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");

    let synchronous: i64 = storage
        .conn
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .expect("read synchronous mode");
    assert_eq!(synchronous, 1);

    drop(storage);
    let _ = fs::remove_file(path);
}

/// 函数 `account_meta_sql_migration_coexists_with_legacy_compat_marker`
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
fn account_meta_sql_migration_coexists_with_legacy_compat_marker() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage
        .conn
        .execute_batch(
            "CREATE TABLE accounts (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                issuer TEXT NOT NULL,
                chatgpt_account_id TEXT,
                workspace_id TEXT,
                workspace_name TEXT,
                note TEXT,
                tags TEXT,
                group_name TEXT,
                sort INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE login_sessions (
                login_id TEXT PRIMARY KEY,
                code_verifier TEXT NOT NULL,
                state TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                note TEXT,
                tags TEXT,
                group_name TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );",
        )
        .expect("create tables with account meta columns");
    storage
        .ensure_migrations_table()
        .expect("ensure migration tracker");
    storage
        .conn
        .execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES ('compat_account_meta_columns', 1)",
            [],
        )
        .expect("insert legacy compat marker");

    storage
        .apply_sql_or_compat_migration(
            "011_account_meta_columns",
            include_str!("../../migrations/011_account_meta_columns.sql"),
            |s| s.ensure_account_meta_columns(),
        )
        .expect("apply 011 migration with fallback");

    let applied_011: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '011_account_meta_columns'",
            [],
            |row| row.get(0),
        )
        .expect("count 011 migration");
    assert_eq!(applied_011, 1);

    let legacy_compat_marker: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = 'compat_account_meta_columns'",
            [],
            |row| row.get(0),
        )
        .expect("count compat marker");
    assert_eq!(legacy_compat_marker, 1);
}

/// 函数 `sql_migration_can_fallback_to_compat_when_schema_already_exists`
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
fn sql_migration_can_fallback_to_compat_when_schema_already_exists() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage
        .conn
        .execute_batch(
            "CREATE TABLE api_keys (
                id TEXT PRIMARY KEY,
                name TEXT,
                model_slug TEXT,
                key_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER
            )",
        )
        .expect("create api_keys with model_slug");
    storage
        .ensure_migrations_table()
        .expect("ensure migration tracker");

    storage
        .apply_sql_or_compat_migration(
            "004_api_key_model",
            include_str!("../../migrations/004_api_key_model.sql"),
            |s| s.ensure_api_key_model_column(),
        )
        .expect("apply 004 migration with fallback");

    let applied_004: i64 = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM schema_migrations WHERE version = '004_api_key_model'",
            [],
            |row| row.get(0),
        )
        .expect("count 004 migration");
    assert_eq!(applied_004, 1);
}

/// 函数 `api_key_profile_migration_backfills_existing_keys`
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
fn api_key_profile_migration_backfills_existing_keys() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage
        .conn
        .execute_batch(
            "CREATE TABLE api_keys (
                id TEXT PRIMARY KEY,
                name TEXT,
                model_slug TEXT,
                reasoning_effort TEXT,
                key_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER
            );
            INSERT INTO api_keys (id, name, model_slug, reasoning_effort, key_hash, status, created_at, last_used_at)
            VALUES ('key-1', 'k1', 'gpt-5', 'low', 'hash-1', 'active', 100, NULL);",
        )
        .expect("prepare api_keys");
    storage
        .ensure_migrations_table()
        .expect("ensure migration tracker");

    storage
        .apply_sql_or_compat_migration(
            "015_api_key_profiles",
            include_str!("../../migrations/015_api_key_profiles.sql"),
            |s| s.ensure_api_key_profiles_table(),
        )
        .expect("apply 015 migration with fallback");
    storage
        .apply_sql_or_compat_migration(
            "035_api_key_profiles_service_tier",
            include_str!("../../migrations/035_api_key_profiles_service_tier.sql"),
            |s| s.ensure_api_key_service_tier_column(),
        )
        .expect("apply 035 migration with fallback");

    let profile_row: (
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = storage
        .conn
        .query_row(
            "SELECT client_type, protocol_type, auth_scheme, default_model, reasoning_effort, upstream_base_url, service_tier
             FROM api_key_profiles
             WHERE key_id = 'key-1'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .expect("load backfilled profile");

    assert_eq!(profile_row.0, "codex");
    assert_eq!(profile_row.1, "openai_compat");
    assert_eq!(profile_row.2, "authorization_bearer");
    assert_eq!(profile_row.3, "gpt-5");
    assert_eq!(profile_row.4.as_deref(), Some("low"));
    assert_eq!(profile_row.5, None);
    assert_eq!(profile_row.6, None);
}

#[test]
fn api_key_profile_drop_azure_protocol_migration_normalizes_legacy_rows() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage
        .conn
        .execute_batch(
            "CREATE TABLE api_keys (
                id TEXT PRIMARY KEY,
                name TEXT,
                model_slug TEXT,
                reasoning_effort TEXT,
                key_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER
            );
            CREATE TABLE api_key_profiles (
                key_id TEXT PRIMARY KEY REFERENCES api_keys(id) ON DELETE CASCADE,
                client_type TEXT NOT NULL CHECK (client_type IN ('codex', 'claude_code')),
                protocol_type TEXT NOT NULL CHECK (protocol_type IN ('openai_compat', 'anthropic_native', 'azure_openai')),
                auth_scheme TEXT NOT NULL CHECK (auth_scheme IN ('authorization_bearer', 'x_api_key', 'api_key')),
                upstream_base_url TEXT,
                static_headers_json TEXT,
                default_model TEXT,
                reasoning_effort TEXT,
                service_tier TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            INSERT INTO api_keys (id, name, model_slug, reasoning_effort, key_hash, status, created_at, last_used_at)
            VALUES ('key-azure', 'azure', NULL, NULL, 'hash-azure', 'active', 100, NULL);
            INSERT INTO api_key_profiles (
                key_id,
                client_type,
                protocol_type,
                auth_scheme,
                upstream_base_url,
                static_headers_json,
                default_model,
                reasoning_effort,
                service_tier,
                created_at,
                updated_at
            )
            VALUES (
                'key-azure',
                'codex',
                'azure_openai',
                'api_key',
                'https://legacy-resource.openai.azure.com',
                '{\"api-key\":\"legacy\"}',
                'gpt-4o',
                'medium',
                'fast',
                100,
                100
            );",
        )
        .expect("prepare legacy azure profile");
    storage
        .ensure_migrations_table()
        .expect("ensure migration tracker");

    storage
        .apply_sql_migration(
            "050_api_key_profiles_drop_azure_protocol",
            include_str!("../../migrations/050_api_key_profiles_drop_azure_protocol.sql"),
        )
        .expect("apply 050 migration");

    let status: String = storage
        .conn
        .query_row(
            "SELECT status FROM api_keys WHERE id = 'key-azure'",
            [],
            |row| row.get(0),
        )
        .expect("load migrated key status");
    assert_eq!(status, "disabled");

    let profile_row: (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = storage
        .conn
        .query_row(
            "SELECT protocol_type, auth_scheme, upstream_base_url, static_headers_json, default_model, reasoning_effort, service_tier
             FROM api_key_profiles
             WHERE key_id = 'key-azure'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .expect("load migrated profile");

    assert_eq!(profile_row.0, "openai_compat");
    assert_eq!(profile_row.1, "authorization_bearer");
    assert_eq!(profile_row.2, None);
    assert_eq!(profile_row.3, None);
    assert_eq!(profile_row.4.as_deref(), Some("gpt-4o"));
    assert_eq!(profile_row.5.as_deref(), Some("medium"));
    assert_eq!(profile_row.6.as_deref(), Some("fast"));
}

/// 函数 `key_hash_index_migration_adds_api_keys_index`
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
fn key_hash_index_migration_adds_api_keys_index() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage.init().expect("init schema");

    let index_sql: String = storage
        .conn
        .query_row(
            "SELECT sql
             FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_api_keys_key_hash'",
            [],
            |row| row.get(0),
        )
        .expect("load index definition");
    assert!(index_sql.contains("api_keys"));
    assert!(index_sql.contains("key_hash"));
}

/// 函数 `usage_snapshot_latest_index_migration_adds_captured_id_index`
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
fn usage_snapshot_latest_index_migration_adds_captured_id_index() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage.init().expect("init schema");

    let index_sql: String = storage
        .conn
        .query_row(
            "SELECT sql
             FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_usage_snapshots_captured_id'",
            [],
            |row| row.get(0),
        )
        .expect("load index definition");
    assert!(index_sql.contains("usage_snapshots"));
    assert!(index_sql.contains("captured_at DESC"));
    assert!(index_sql.contains("id DESC"));
}

/// 函数 `accounts_sort_index_migration_adds_sort_updated_at_index`
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
fn accounts_sort_index_migration_adds_sort_updated_at_index() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage.init().expect("init schema");

    let index_sql: String = storage
        .conn
        .query_row(
            "SELECT sql
             FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_accounts_sort_updated_at'",
            [],
            |row| row.get(0),
        )
        .expect("load index definition");
    assert!(index_sql.contains("accounts"));
    assert!(index_sql.contains("sort ASC"));
    assert!(index_sql.contains("updated_at DESC"));
}

/// 函数 `conversation_bindings_migration_adds_indexes`
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
fn conversation_bindings_migration_adds_indexes() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage.init().expect("init schema");

    let account_index_sql: String = storage
        .conn
        .query_row(
            "SELECT sql
             FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_conversation_bindings_account_id'",
            [],
            |row| row.get(0),
        )
        .expect("load account index definition");
    assert!(account_index_sql.contains("conversation_bindings"));
    assert!(account_index_sql.contains("account_id"));

    let last_used_index_sql: String = storage
        .conn
        .query_row(
            "SELECT sql
             FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_conversation_bindings_last_used_at'",
            [],
            |row| row.get(0),
        )
        .expect("load last_used index definition");
    assert!(last_used_index_sql.contains("conversation_bindings"));
    assert!(last_used_index_sql.contains("last_used_at DESC"));
}

/// 函数 `request_logs_compact_migration_drops_legacy_usage_columns_and_preserves_rows`
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
fn request_logs_compact_migration_drops_legacy_usage_columns_and_preserves_rows() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage
        .conn
        .execute_batch(
            "CREATE TABLE request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trace_id TEXT,
                key_id TEXT,
                account_id TEXT,
                request_path TEXT NOT NULL,
                original_path TEXT,
                adapted_path TEXT,
                method TEXT NOT NULL,
                model TEXT,
                reasoning_effort TEXT,
                response_adapter TEXT,
                upstream_url TEXT,
                status_code INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                estimated_cost_usd REAL,
                cached_input_tokens INTEGER,
                reasoning_output_tokens INTEGER,
                error TEXT,
                created_at INTEGER NOT NULL
            );
            INSERT INTO request_logs (
                id, trace_id, key_id, account_id, request_path, original_path, adapted_path,
                method, model, reasoning_effort, response_adapter, upstream_url, status_code,
                input_tokens, output_tokens, estimated_cost_usd, cached_input_tokens,
                reasoning_output_tokens, error, created_at
            ) VALUES (
                7, 'trc-legacy', 'gk_legacy', 'acc-legacy', '/v1/responses', '/v1/chat/completions',
                '/v1/responses', 'POST', 'gpt-5.3-codex', 'high', 'OpenAIChatCompletionsJson',
                'https://chatgpt.com/backend-api/codex/v1/responses', 200,
                12, 5, 0.25, 3, 2, NULL, 1700000000
            );",
        )
        .expect("create legacy request_logs");
    storage
        .ensure_migrations_table()
        .expect("ensure migration tracker");

    storage.init().expect("run init on legacy request_logs");

    assert!(!storage
        .has_column("request_logs", "input_tokens")
        .expect("check compact input_tokens"));
    assert!(!storage
        .has_column("request_logs", "output_tokens")
        .expect("check compact output_tokens"));
    assert!(!storage
        .has_column("request_logs", "estimated_cost_usd")
        .expect("check compact estimated_cost_usd"));
    assert!(!storage
        .has_column("request_logs", "cached_input_tokens")
        .expect("check compact cached_input_tokens"));
    assert!(!storage
        .has_column("request_logs", "reasoning_output_tokens")
        .expect("check compact reasoning_output_tokens"));
    assert!(storage
        .has_column("request_logs", "duration_ms")
        .expect("check compact duration_ms"));
    assert!(storage
        .has_column("request_logs", "effective_service_tier")
        .expect("check compact effective_service_tier"));

    let request_log_row: (i64, String, Option<String>, Option<String>, Option<i64>) = storage
        .conn
        .query_row(
            "SELECT id, request_path, response_adapter, effective_service_tier, duration_ms FROM request_logs WHERE id = 7",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .expect("load compacted request log row");
    assert_eq!(request_log_row.0, 7);
    assert_eq!(request_log_row.1, "/v1/responses");
    assert_eq!(
        request_log_row.2.as_deref(),
        Some("OpenAIChatCompletionsJson")
    );
    assert_eq!(request_log_row.3, None);
    assert_eq!(request_log_row.4, None);

    let token_row: (Option<i64>, Option<i64>, Option<f64>, Option<i64>, Option<i64>) = storage
        .conn
        .query_row(
            "SELECT input_tokens, output_tokens, estimated_cost_usd, cached_input_tokens, reasoning_output_tokens
             FROM request_token_stats
             WHERE request_log_id = 7",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .expect("load migrated token stats");
    assert_eq!(token_row.0, Some(12));
    assert_eq!(token_row.1, Some(5));
    assert_eq!(token_row.2, Some(0.25));
    assert_eq!(token_row.3, Some(3));
    assert_eq!(token_row.4, Some(2));
}

#[test]
fn init_upgrades_legacy_model_catalog_table_to_structured_schema() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage
        .conn
        .execute_batch(
            "CREATE TABLE model_catalog_models (
                scope TEXT NOT NULL,
                slug TEXT NOT NULL,
                display_name TEXT NOT NULL,
                model_json TEXT NOT NULL,
                sort_index INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (scope, slug)
            );",
        )
        .expect("create legacy model catalog table");
    storage
        .conn
        .execute(
            "CREATE TABLE model_options_cache (
                scope TEXT PRIMARY KEY,
                items_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )
        .expect("create legacy model options cache");
    storage
        .ensure_migrations_table()
        .expect("ensure migration tracker");

    storage.init().expect("run init on legacy model catalog");

    assert!(storage
        .has_column("model_catalog_models", "description")
        .expect("description column"));
    assert!(storage
        .has_column("model_catalog_models", "supported_in_api")
        .expect("supported_in_api column"));
    assert!(storage
        .has_column("model_catalog_models", "source_kind")
        .expect("source_kind column"));
    assert!(storage
        .has_column("model_catalog_models", "user_edited")
        .expect("user_edited column"));
    assert!(storage
        .has_column("model_catalog_models", "minimal_client_version_json")
        .expect("minimal client version column"));
    assert!(storage
        .has_column("model_catalog_models", "extra_json")
        .expect("extra_json column"));
    assert!(!storage
        .has_column("model_catalog_models", "model_json")
        .expect("model_json column removed"));
    assert!(!storage
        .has_table("model_options_cache")
        .expect("model_options_cache removed"));

    let scope_table_exists = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = 'model_catalog_scopes'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("check scope table");
    assert_eq!(scope_table_exists, 1);

    let reasoning_table_exists = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = 'model_catalog_reasoning_levels'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("check reasoning table");
    assert_eq!(reasoning_table_exists, 1);

    let string_items_table_exists = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = 'model_catalog_string_items'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("check string items table");
    assert_eq!(string_items_table_exists, 1);

    let legacy_plans_table_exists = storage
        .conn
        .query_row(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = 'model_catalog_available_in_plans'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("check legacy plans table");
    assert_eq!(legacy_plans_table_exists, 0);
}
