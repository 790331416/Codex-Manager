use codexmanager_core::rpc::types::ApiKeySummary;
use std::collections::HashSet;

use crate::storage_helpers::open_storage;
use crate::RpcActor;

/// 函数 `read_api_keys`
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
pub(crate) fn read_api_keys() -> Result<Vec<ApiKeySummary>, String> {
    // 读取平台 Key 列表
    let storage = open_storage().ok_or_else(|| "open storage failed".to_string())?;
    let keys = storage
        .list_api_keys()
        .map_err(|err| format!("list api keys failed: {err}"))?;
    let quota_limits = storage
        .list_api_key_quota_limits()
        .map_err(|err| format!("list api key quota limits failed: {err}"))?;
    Ok(keys
        .into_iter()
        .map(|key| ApiKeySummary {
            quota_limit_tokens: quota_limits.get(&key.id).copied(),
            id: key.id,
            name: key.name,
            model_slug: key.model_slug,
            reasoning_effort: key.reasoning_effort,
            service_tier: key.service_tier,
            rotation_strategy: key.rotation_strategy,
            aggregate_api_id: key.aggregate_api_id,
            account_plan_filter: key.account_plan_filter,
            aggregate_api_url: key.aggregate_api_url,
            client_type: key.client_type,
            protocol_type: key.protocol_type,
            auth_scheme: key.auth_scheme,
            upstream_base_url: key.upstream_base_url,
            static_headers_json: key.static_headers_json,
            status: key.status,
            created_at: key.created_at,
            last_used_at: key.last_used_at,
        })
        .collect())
}

pub(crate) fn read_api_keys_for_actor(actor: &RpcActor) -> Result<Vec<ApiKeySummary>, String> {
    let items = read_api_keys()?;
    if actor.is_admin() {
        return Ok(items);
    }
    let user_id = actor
        .user_id
        .as_deref()
        .ok_or_else(|| "permission_denied: apikey requires user session".to_string())?;
    let owned_key_ids = crate::list_api_key_ids_for_user(user_id)?
        .into_iter()
        .collect::<HashSet<_>>();
    Ok(items
        .into_iter()
        .filter(|item| owned_key_ids.contains(&item.id))
        .collect())
}
