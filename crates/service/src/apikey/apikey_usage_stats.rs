use codexmanager_core::rpc::types::ApiKeyUsageStatSummary;
use std::collections::HashSet;

use crate::storage_helpers::open_storage;
use crate::RpcActor;

/// 函数 `read_api_key_usage_stats`
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
pub(crate) fn read_api_key_usage_stats() -> Result<Vec<ApiKeyUsageStatSummary>, String> {
    let storage = open_storage().ok_or_else(|| "open storage failed".to_string())?;
    let items = storage
        .summarize_request_token_stats_by_key()
        .map_err(|err| format!("summarize api key token stats failed: {err}"))?;

    Ok(items
        .into_iter()
        .map(|item| ApiKeyUsageStatSummary {
            key_id: item.key_id,
            total_tokens: item.total_tokens.max(0),
            estimated_cost_usd: item.estimated_cost_usd.max(0.0),
        })
        .collect())
}

pub(crate) fn read_api_key_usage_stats_for_actor(
    actor: &RpcActor,
) -> Result<Vec<ApiKeyUsageStatSummary>, String> {
    let items = read_api_key_usage_stats()?;
    if actor.is_admin() {
        return Ok(items);
    }
    let user_id = actor
        .user_id
        .as_deref()
        .ok_or_else(|| "permission_denied: apikey usage requires user session".to_string())?;
    let owned_key_ids = crate::list_api_key_ids_for_user(user_id)?
        .into_iter()
        .collect::<HashSet<_>>();
    Ok(items
        .into_iter()
        .filter(|item| owned_key_ids.contains(&item.key_id))
        .collect())
}
