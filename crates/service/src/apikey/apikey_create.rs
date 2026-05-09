use codexmanager_core::rpc::types::ApiKeyCreateResult;
use codexmanager_core::storage::{now_ts, ApiKey};

use crate::apikey::service_tier::normalize_service_tier_owned;
use crate::apikey_profile::{
    normalize_protocol_type, normalize_rotation_strategy, normalize_static_headers_json,
    normalize_upstream_base_url, profile_from_protocol,
};
use crate::reasoning_effort::normalize_reasoning_effort_owned;
use crate::storage_helpers::{
    generate_key_id, generate_platform_key, hash_platform_key, open_storage,
};

/// 函数 `create_api_key`
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
pub(crate) fn create_api_key(
    name: Option<String>,
    model_slug: Option<String>,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    protocol_type: Option<String>,
    upstream_base_url: Option<String>,
    static_headers_json: Option<String>,
    rotation_strategy: Option<String>,
    aggregate_api_id: Option<String>,
    account_plan_filter: Option<String>,
) -> Result<ApiKeyCreateResult, String> {
    // 创建平台 Key 并写入存储
    let storage = open_storage().ok_or_else(|| "storage unavailable".to_string())?;
    let key = generate_platform_key();
    let key_hash = hash_platform_key(&key);
    let key_id = generate_key_id();
    let protocol_type = normalize_protocol_type(protocol_type)?;
    let (client_type, protocol_type, auth_scheme) = profile_from_protocol(&protocol_type)?;
    let upstream_base_url = normalize_upstream_base_url(upstream_base_url)?;
    let static_headers_json = normalize_static_headers_json(static_headers_json)?;
    let rotation_strategy = normalize_rotation_strategy(rotation_strategy)?;
    let aggregate_api_id = if rotation_strategy == crate::apikey_profile::ROTATION_AGGREGATE_API {
        aggregate_api_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    } else {
        None
    };
    let account_plan_filter = if rotation_strategy == crate::apikey_profile::ROTATION_ACCOUNT
        || rotation_strategy == crate::apikey_profile::ROTATION_HYBRID
    {
        crate::account_plan::normalize_account_plan_filter(account_plan_filter)?
    } else {
        None
    };
    let record = ApiKey {
        id: key_id.clone(),
        name,
        model_slug,
        reasoning_effort: normalize_reasoning_effort_owned(reasoning_effort),
        service_tier: normalize_service_tier_owned(service_tier)?,
        rotation_strategy,
        aggregate_api_id,
        account_plan_filter,
        aggregate_api_url: None,
        client_type,
        protocol_type,
        auth_scheme,
        upstream_base_url,
        static_headers_json,
        key_hash,
        status: "active".to_string(),
        created_at: now_ts(),
        last_used_at: None,
    };
    storage.insert_api_key(&record).map_err(|e| e.to_string())?;
    if let Err(err) = storage.upsert_api_key_secret(&key_id, &key) {
        let _ = storage.delete_api_key(&key_id);
        return Err(format!("persist api key secret failed: {err}"));
    }
    Ok(ApiKeyCreateResult { id: key_id, key })
}
