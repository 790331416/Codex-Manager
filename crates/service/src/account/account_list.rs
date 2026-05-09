use codexmanager_core::{
    rpc::types::{AccountListParams, AccountListResult, AccountSummary},
    storage::{Account, AccountMetadata, AccountSubscription, Token, UsageSnapshotRecord},
};
use std::collections::HashMap;

use crate::account_plan::resolve_account_plan;
use crate::storage_helpers::open_storage;

const DEFAULT_ACCOUNT_PAGE_SIZE: i64 = 5;
const MAX_ACCOUNT_PAGE_SIZE: i64 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountFilter {
    All,
    Active,
    Low,
}

/// 函数 `read_accounts`
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
pub(crate) fn read_accounts(
    params: AccountListParams,
    pagination_requested: bool,
) -> Result<AccountListResult, String> {
    // 中文注释：账号页需要后端分页，但仪表盘/日志等全局功能仍依赖全量账号列表；
    // 因此这里兼容“无分页参数时返回全量，有分页参数时返回当前页”两种模式。
    let params = params.normalized();
    let storage = open_storage().ok_or_else(|| "open storage failed".to_string())?;
    let query = normalize_optional_text(params.query);
    let group_filter = normalize_optional_text(params.group_filter);
    let filter = normalize_filter(params.filter);

    if filter == AccountFilter::All {
        if pagination_requested {
            let page_size = normalize_page_size(params.page_size);
            let total = storage
                .account_count_filtered(query.as_deref(), group_filter.as_deref())
                .map_err(|err| format!("count accounts failed: {err}"))?;
            let page = clamp_page(params.page, total, page_size);
            let offset = (page - 1) * page_size;
            let accounts = storage
                .list_accounts_paginated(
                    query.as_deref(),
                    group_filter.as_deref(),
                    offset,
                    page_size,
                )
                .map_err(|err| format!("list accounts failed: {err}"))?;
            let items = to_account_summaries(&storage, accounts)?;
            return Ok(AccountListResult {
                items,
                total,
                page,
                page_size,
            });
        }

        let accounts = storage
            .list_accounts_filtered(query.as_deref(), group_filter.as_deref())
            .map_err(|err| format!("list accounts failed: {err}"))?;
        let total = accounts.len() as i64;
        let items = to_account_summaries(&storage, accounts)?;
        return Ok(AccountListResult {
            items,
            total,
            page: 1,
            page_size: if total > 0 {
                total
            } else {
                DEFAULT_ACCOUNT_PAGE_SIZE
            },
        });
    }

    if pagination_requested {
        let total =
            filtered_account_count(&storage, filter, query.as_deref(), group_filter.as_deref())?;
        let page_size = normalize_page_size(params.page_size);
        let page = clamp_page(params.page, total, page_size);
        let offset = (page - 1) * page_size;
        let paged = filtered_accounts(
            &storage,
            filter,
            query.as_deref(),
            group_filter.as_deref(),
            Some((offset, page_size)),
        )?;
        let items = to_account_summaries(&storage, paged)?;
        return Ok(AccountListResult {
            items,
            total,
            page,
            page_size,
        });
    }

    let accounts = filtered_accounts(
        &storage,
        filter,
        query.as_deref(),
        group_filter.as_deref(),
        None,
    )?;
    let total = accounts.len() as i64;
    let items = to_account_summaries(&storage, accounts)?;

    Ok(AccountListResult {
        items,
        total,
        page: 1,
        page_size: if total > 0 {
            total
        } else {
            DEFAULT_ACCOUNT_PAGE_SIZE
        },
    })
}

/// 函数 `normalize_optional_text`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - value: 参数 value
///
/// # 返回
/// 返回函数执行结果
fn normalize_optional_text(value: Option<String>) -> Option<String> {
    let trimmed = value.unwrap_or_default().trim().to_string();
    if trimmed.is_empty() || trimmed == "all" {
        return None;
    }
    Some(trimmed)
}

/// 函数 `normalize_filter`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - value: 参数 value
///
/// # 返回
/// 返回函数执行结果
fn normalize_filter(value: Option<String>) -> AccountFilter {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "active" => AccountFilter::Active,
        "low" => AccountFilter::Low,
        _ => AccountFilter::All,
    }
}

/// 函数 `normalize_page_size`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - value: 参数 value
///
/// # 返回
/// 返回函数执行结果
fn normalize_page_size(value: i64) -> i64 {
    value.clamp(1, MAX_ACCOUNT_PAGE_SIZE)
}

/// 函数 `clamp_page`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - page: 参数 page
/// - total: 参数 total
/// - page_size: 参数 page_size
///
/// # 返回
/// 返回函数执行结果
fn clamp_page(page: i64, total: i64, page_size: i64) -> i64 {
    let normalized_page = page.max(1);
    let total_pages = if total <= 0 {
        1
    } else {
        ((total + page_size - 1) / page_size).max(1)
    };
    normalized_page.min(total_pages)
}

/// 函数 `filtered_account_count`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - storage: 参数 storage
/// - filter: 参数 filter
/// - query: 参数 query
/// - group_filter: 参数 group_filter
///
/// # 返回
/// 返回函数执行结果
fn filtered_account_count(
    storage: &codexmanager_core::storage::Storage,
    filter: AccountFilter,
    query: Option<&str>,
    group_filter: Option<&str>,
) -> Result<i64, String> {
    match filter {
        AccountFilter::All => storage
            .account_count_filtered(query, group_filter)
            .map_err(|err| format!("count accounts failed: {err}")),
        AccountFilter::Active => storage
            .account_count_active_available(query, group_filter)
            .map_err(|err| format!("count active accounts failed: {err}")),
        AccountFilter::Low => storage
            .account_count_low_quota(query, group_filter)
            .map_err(|err| format!("count low quota accounts failed: {err}")),
    }
}

/// 函数 `filtered_accounts`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - storage: 参数 storage
/// - filter: 参数 filter
/// - query: 参数 query
/// - group_filter: 参数 group_filter
/// - pagination: 参数 pagination
///
/// # 返回
/// 返回函数执行结果
fn filtered_accounts(
    storage: &codexmanager_core::storage::Storage,
    filter: AccountFilter,
    query: Option<&str>,
    group_filter: Option<&str>,
    pagination: Option<(i64, i64)>,
) -> Result<Vec<Account>, String> {
    match filter {
        AccountFilter::All => match pagination {
            Some((offset, limit)) => storage
                .list_accounts_paginated(query, group_filter, offset, limit)
                .map_err(|err| format!("list accounts failed: {err}")),
            None => storage
                .list_accounts_filtered(query, group_filter)
                .map_err(|err| format!("list accounts failed: {err}")),
        },
        AccountFilter::Active => storage
            .list_accounts_active_available(query, group_filter, pagination)
            .map_err(|err| format!("list active accounts failed: {err}")),
        AccountFilter::Low => storage
            .list_accounts_low_quota(query, group_filter, pagination)
            .map_err(|err| format!("list low quota accounts failed: {err}")),
    }
}

/// 函数 `to_account_summary_with_reason`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - acc: 参数 acc
/// - status_reason: 参数 status_reason
/// - plan_type: 参数 plan_type
/// - plan_type_raw: 参数 plan_type_raw
/// - note: 参数 note
/// - tags: 参数 tags
///
/// # 返回
/// 返回函数执行结果
fn to_account_summary_with_reason(
    acc: Account,
    preferred: bool,
    status_reason: Option<String>,
    plan_type: Option<String>,
    plan_type_raw: Option<String>,
    has_subscription: Option<bool>,
    subscription_plan: Option<String>,
    subscription_expires_at: Option<i64>,
    subscription_renews_at: Option<i64>,
    note: Option<String>,
    tags: Option<String>,
) -> AccountSummary {
    AccountSummary {
        id: acc.id,
        label: acc.label,
        group_name: acc.group_name,
        preferred,
        sort: acc.sort,
        status: acc.status,
        status_reason,
        plan_type,
        plan_type_raw,
        has_subscription,
        subscription_plan,
        subscription_expires_at,
        subscription_renews_at,
        note,
        tags,
    }
}

/// 函数 `to_account_summaries`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - storage: 参数 storage
/// - accounts: 参数 accounts
///
/// # 返回
/// 返回函数执行结果
fn to_account_summaries(
    storage: &codexmanager_core::storage::Storage,
    accounts: Vec<Account>,
) -> Result<Vec<AccountSummary>, String> {
    let account_ids = accounts
        .iter()
        .map(|account| account.id.clone())
        .collect::<Vec<_>>();
    let preferred_account_id = storage
        .preferred_account_id()
        .map_err(|err| format!("load preferred account failed: {err}"))?;
    let status_reasons = storage
        .latest_account_status_reasons(&account_ids)
        .map_err(|err| format!("load account status reasons failed: {err}"))?;
    let tokens = storage
        .list_tokens()
        .map_err(|err| format!("load account tokens failed: {err}"))?
        .into_iter()
        .map(|token| (token.account_id.clone(), token))
        .collect::<HashMap<String, Token>>();
    let usages = storage
        .latest_usage_snapshots_by_account()
        .map_err(|err| format!("load account usage snapshots failed: {err}"))?
        .into_iter()
        .map(|snapshot| (snapshot.account_id.clone(), snapshot))
        .collect::<HashMap<String, UsageSnapshotRecord>>();
    let metadata = storage
        .list_account_metadata()
        .map_err(|err| format!("load account metadata failed: {err}"))?
        .into_iter()
        .map(|item| (item.account_id.clone(), item))
        .collect::<HashMap<String, AccountMetadata>>();
    let subscriptions = storage
        .list_account_subscriptions()
        .map_err(|err| format!("load account subscriptions failed: {err}"))?
        .into_iter()
        .map(|item| (item.account_id.clone(), item))
        .collect::<HashMap<String, AccountSubscription>>();
    Ok(accounts
        .into_iter()
        .map(|account| {
            map_account_summary(
                account,
                preferred_account_id.as_deref(),
                &status_reasons,
                &tokens,
                &usages,
                &metadata,
                &subscriptions,
            )
        })
        .collect())
}

/// 函数 `map_account_summary`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - account: 参数 account
/// - status_reasons: 参数 status_reasons
/// - tokens: 参数 tokens
/// - usages: 参数 usages
/// - metadata: 参数 metadata
///
/// # 返回
/// 返回函数执行结果
fn map_account_summary(
    account: Account,
    preferred_account_id: Option<&str>,
    status_reasons: &HashMap<String, String>,
    tokens: &HashMap<String, Token>,
    usages: &HashMap<String, UsageSnapshotRecord>,
    metadata: &HashMap<String, AccountMetadata>,
    subscriptions: &HashMap<String, AccountSubscription>,
) -> AccountSummary {
    let account_id = account.id.clone();
    let status_reason = status_reasons.get(&account_id).cloned();
    let preferred = preferred_account_id.is_some_and(|id| id == account_id);
    let plan = resolve_account_plan(tokens.get(&account_id), usages.get(&account_id));
    let account_metadata = metadata.get(&account_id);
    let subscription = subscriptions.get(&account_id);
    let (fallback_plan_type, plan_type_raw) = match plan {
        Some(value) => (Some(value.normalized), value.raw),
        None => (None, None),
    };
    let subscription_plan = subscription.and_then(|value| value.plan_type.clone());
    let subscription_plan_type = subscription.and_then(resolve_subscription_plan_type);
    let plan_type = subscription_plan_type.or(fallback_plan_type);
    to_account_summary_with_reason(
        account,
        preferred,
        status_reason,
        plan_type,
        plan_type_raw,
        subscription.map(|value| value.has_subscription),
        subscription_plan,
        subscription.and_then(|value| value.expires_at),
        subscription.and_then(|value| value.renews_at),
        account_metadata.and_then(|value| value.note.clone()),
        account_metadata.and_then(|value| value.tags.clone()),
    )
}

fn resolve_subscription_plan_type(subscription: &AccountSubscription) -> Option<String> {
    if let Some(plan_type) = subscription.plan_type.clone() {
        return Some(plan_type);
    }
    if !subscription.has_subscription {
        return Some("free".to_string());
    }
    None
}
