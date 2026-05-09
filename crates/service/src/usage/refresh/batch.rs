use crate::account_status::is_banned_status_reason;
use codexmanager_core::storage::{Account, Storage, Token};
use crossbeam_channel::unbounded;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

use super::{
    build_workspace_map_from_accounts, notify_usage_refresh_completed, open_storage,
    record_usage_refresh_failure, record_usage_refresh_metrics, refresh_usage_for_token,
    DEFAULT_USAGE_POLL_BATCH_LIMIT, DEFAULT_USAGE_POLL_CYCLE_BUDGET_SECS,
    ENV_USAGE_POLL_BATCH_LIMIT, ENV_USAGE_POLL_CYCLE_BUDGET_SECS, USAGE_POLL_CURSOR,
    USAGE_REFRESH_WORKERS,
};

/// 函数 `refresh_usage_for_all_accounts`
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
pub(crate) fn refresh_usage_for_all_accounts() -> Result<(), String> {
    let storage = open_storage().ok_or_else(|| "storage unavailable".to_string())?;
    let accounts = storage.list_accounts().map_err(|e| e.to_string())?;
    let tasks = build_usage_refresh_tasks(
        storage.list_tokens().map_err(|e| e.to_string())?,
        &accounts,
        &load_banned_account_ids(&storage, &accounts)?,
    );
    if tasks.is_empty() {
        return Ok(());
    }
    let total = tasks.len();
    let processed = run_usage_refresh_tasks(tasks)?;
    notify_usage_refresh_completed("manual_all", processed, total);
    Ok(())
}

/// 函数 `refresh_usage_for_polling_batch`
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
pub(crate) fn refresh_usage_for_polling_batch() -> Result<(), String> {
    let storage = open_storage().ok_or_else(|| "storage unavailable".to_string())?;
    let accounts = storage.list_accounts().map_err(|e| e.to_string())?;
    let all_tasks = build_usage_refresh_tasks(
        storage.list_tokens().map_err(|e| e.to_string())?,
        &accounts,
        &load_banned_account_ids(&storage, &accounts)?,
    );
    if all_tasks.is_empty() {
        return Ok(());
    }

    let total = all_tasks.len();
    let start_cursor = USAGE_POLL_CURSOR.load(Ordering::Relaxed) % total;
    let batch_limit = usage_poll_batch_limit(total);
    let cycle_budget = usage_poll_cycle_budget();
    let cycle_started_at = Instant::now();
    let indices = usage_poll_batch_indices(total, start_cursor, batch_limit);
    let selected_tasks = indices
        .into_iter()
        .map(|index| all_tasks[index].clone())
        .collect::<Vec<_>>();
    let processed = run_usage_refresh_tasks(selected_tasks)?;

    if processed > 0 {
        USAGE_POLL_CURSOR.store(
            next_usage_poll_cursor(total, start_cursor, processed),
            Ordering::Relaxed,
        );
    }
    if cycle_budget.is_some_and(|budget| cycle_started_at.elapsed() >= budget) {
        log::info!(
            "usage polling batch exceeded budget: processed={} total={} workers={} elapsed_ms={} budget_secs={}",
            processed,
            total,
            usage_refresh_worker_count().min(processed.max(1)),
            cycle_started_at.elapsed().as_millis(),
            cycle_budget.map(|budget| budget.as_secs()).unwrap_or(0)
        );
    }
    if processed < total {
        log::info!(
            "usage polling batch truncated: processed={} total={} batch_limit={} workers={} budget_secs={}",
            processed,
            total,
            batch_limit,
            usage_refresh_worker_count().min(processed.max(1)),
            cycle_budget.map(|budget| budget.as_secs()).unwrap_or(0)
        );
    }
    notify_usage_refresh_completed("polling", processed, total);
    Ok(())
}

#[derive(Clone)]
struct UsageRefreshBatchTask {
    account_id: String,
    token: Token,
    workspace_id: Option<String>,
}

/// 函数 `build_usage_refresh_tasks`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - tokens: 参数 tokens
/// - accounts: 参数 accounts
/// - banned_ids: 参数 banned_ids
///
/// # 返回
/// 返回函数执行结果
fn build_usage_refresh_tasks(
    tokens: Vec<Token>,
    accounts: &[Account],
    banned_ids: &HashSet<String>,
) -> Vec<UsageRefreshBatchTask> {
    let mut skipped_ids = accounts
        .iter()
        .filter(|account| is_account_refresh_skipped(account))
        .map(|account| account.id.clone())
        .collect::<HashSet<_>>();
    skipped_ids.extend(banned_ids.iter().cloned());
    let workspace_map = build_workspace_map_from_accounts(accounts);

    tokens
        .into_iter()
        .filter(|token| !skipped_ids.contains(&token.account_id))
        .map(|token| {
            let account_id = token.account_id.clone();
            UsageRefreshBatchTask {
                workspace_id: workspace_map.get(&account_id).cloned().unwrap_or(None),
                token,
                account_id,
            }
        })
        .collect()
}

/// 函数 `run_usage_refresh_tasks`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - tasks: 参数 tasks
///
/// # 返回
/// 返回函数执行结果
fn run_usage_refresh_tasks(tasks: Vec<UsageRefreshBatchTask>) -> Result<usize, String> {
    let total = tasks.len();
    if total == 0 {
        return Ok(0);
    }

    let worker_count = usage_refresh_worker_count().min(total);
    if worker_count <= 1 {
        let storage = open_storage().ok_or_else(|| "storage unavailable".to_string())?;
        for task in tasks {
            run_usage_refresh_task(&storage, task);
        }
        return Ok(total);
    }

    let (sender, receiver) = unbounded::<UsageRefreshBatchTask>();
    for task in tasks {
        sender
            .send(task)
            .map_err(|_| "enqueue usage refresh task failed".to_string())?;
    }
    drop(sender);

    thread::scope(|scope| -> Result<(), String> {
        let mut handles = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let receiver = receiver.clone();
            handles.push(scope.spawn(move || {
                let storage = open_storage().ok_or_else(|| {
                    format!("usage refresh worker {worker_index} storage unavailable")
                })?;
                while let Ok(task) = receiver.recv() {
                    run_usage_refresh_task(&storage, task);
                }
                Ok::<(), String>(())
            }));
        }

        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(_) => return Err("usage refresh worker panicked".to_string()),
            }
        }
        Ok(())
    })?;

    Ok(total)
}

/// 函数 `run_usage_refresh_task`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - storage: 参数 storage
/// - task: 参数 task
///
/// # 返回
/// 无
fn run_usage_refresh_task(storage: &Storage, task: UsageRefreshBatchTask) {
    let started_at = Instant::now();
    match refresh_usage_for_token(storage, &task.token, task.workspace_id.as_deref(), None) {
        Ok(_) => record_usage_refresh_metrics(true, started_at),
        Err(err) => {
            record_usage_refresh_metrics(false, started_at);
            record_usage_refresh_failure(storage, &task.account_id, &err);
        }
    }
}

/// 函数 `load_banned_account_ids`
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
fn load_banned_account_ids(
    storage: &Storage,
    accounts: &[Account],
) -> Result<HashSet<String>, String> {
    let account_ids = accounts
        .iter()
        .map(|account| account.id.clone())
        .collect::<Vec<_>>();
    let reasons = storage
        .latest_account_status_reasons(&account_ids)
        .map_err(|err| err.to_string())?;
    Ok(reasons
        .into_iter()
        .filter(|(_, reason)| is_banned_status_reason(reason))
        .map(|(account_id, _)| account_id)
        .collect())
}

/// 函数 `usage_refresh_worker_count`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 返回函数执行结果
fn usage_refresh_worker_count() -> usize {
    USAGE_REFRESH_WORKERS.load(Ordering::Relaxed).max(1)
}

/// 函数 `is_account_refresh_skipped`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - account: 参数 account
///
/// # 返回
/// 返回函数执行结果
fn is_account_refresh_skipped(account: &Account) -> bool {
    let normalized = account.status.trim().to_ascii_lowercase();
    normalized == "disabled" || normalized == "banned"
}

/// 函数 `usage_poll_batch_limit`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - total: 参数 total
///
/// # 返回
/// 返回函数执行结果
fn usage_poll_batch_limit(total: usize) -> usize {
    if total == 0 {
        return 0;
    }
    let configured = std::env::var(ENV_USAGE_POLL_BATCH_LIMIT)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_USAGE_POLL_BATCH_LIMIT);
    if configured == 0 {
        total
    } else {
        configured.max(1).min(total)
    }
}

/// 函数 `usage_poll_cycle_budget`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 返回函数执行结果
fn usage_poll_cycle_budget() -> Option<Duration> {
    let configured = std::env::var(ENV_USAGE_POLL_CYCLE_BUDGET_SECS)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_USAGE_POLL_CYCLE_BUDGET_SECS);
    if configured == 0 {
        None
    } else {
        Some(Duration::from_secs(configured.max(1)))
    }
}

/// 函数 `usage_poll_batch_indices`
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
#[cfg(test)]
pub(crate) fn usage_poll_batch_indices(
    total: usize,
    cursor: usize,
    batch_limit: usize,
) -> Vec<usize> {
    if total == 0 || batch_limit == 0 {
        return Vec::new();
    }
    let start = cursor % total;
    (0..batch_limit.min(total))
        .map(|offset| (start + offset) % total)
        .collect()
}

/// 函数 `next_usage_poll_cursor`
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
#[cfg(test)]
pub(crate) fn next_usage_poll_cursor(total: usize, cursor: usize, processed: usize) -> usize {
    if total == 0 {
        return 0;
    }
    (cursor % total + processed.min(total)) % total
}

/// 函数 `usage_poll_batch_indices`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - total: 参数 total
/// - cursor: 参数 cursor
/// - batch_limit: 参数 batch_limit
///
/// # 返回
/// 返回函数执行结果
#[cfg(not(test))]
fn usage_poll_batch_indices(total: usize, cursor: usize, batch_limit: usize) -> Vec<usize> {
    if total == 0 || batch_limit == 0 {
        return Vec::new();
    }
    let start = cursor % total;
    (0..batch_limit.min(total))
        .map(|offset| (start + offset) % total)
        .collect()
}

/// 函数 `next_usage_poll_cursor`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - total: 参数 total
/// - cursor: 参数 cursor
/// - processed: 参数 processed
///
/// # 返回
/// 返回函数执行结果
#[cfg(not(test))]
fn next_usage_poll_cursor(total: usize, cursor: usize, processed: usize) -> usize {
    if total == 0 {
        return 0;
    }
    (cursor % total + processed.min(total)) % total
}

#[cfg(test)]
mod tests {
    use super::build_usage_refresh_tasks;
    use codexmanager_core::storage::{now_ts, Account, Token};
    use std::collections::HashSet;

    /// 函数 `account`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - id: 参数 id
    /// - status: 参数 status
    /// - workspace_id: 参数 workspace_id
    ///
    /// # 返回
    /// 返回函数执行结果
    fn account(id: &str, status: &str, workspace_id: Option<&str>) -> Account {
        Account {
            id: id.to_string(),
            label: id.to_string(),
            issuer: "issuer".to_string(),
            chatgpt_account_id: None,
            workspace_id: workspace_id.map(|value| value.to_string()),
            group_name: None,
            sort: 0,
            status: status.to_string(),
            created_at: now_ts(),
            updated_at: now_ts(),
        }
    }

    /// 函数 `token`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - account_id: 参数 account_id
    ///
    /// # 返回
    /// 返回函数执行结果
    fn token(account_id: &str) -> Token {
        Token {
            account_id: account_id.to_string(),
            id_token: "id-token".to_string(),
            access_token: "access-token".to_string(),
            refresh_token: "refresh-token".to_string(),
            api_key_access_token: None,
            last_refresh: now_ts(),
        }
    }

    /// 函数 `build_usage_refresh_tasks_skips_disabled_and_banned_accounts`
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
    fn build_usage_refresh_tasks_skips_disabled_and_banned_accounts() {
        let tasks = build_usage_refresh_tasks(
            vec![
                token("acc-active"),
                token("acc-disabled"),
                token("acc-banned"),
                token("acc-inactive"),
                token("acc-unavailable"),
                token("acc-missing"),
            ],
            &[
                account("acc-active", "active", Some("ws-active")),
                account("acc-disabled", "disabled", Some("ws-disabled")),
                account("acc-banned", "banned", Some("ws-banned")),
                account("acc-inactive", "inactive", Some("ws-inactive")),
                account("acc-unavailable", "unavailable", Some("ws-unavailable")),
            ],
            &HashSet::from([String::from("acc-banned")]),
        );

        let account_ids = tasks
            .iter()
            .map(|task| task.account_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            account_ids,
            vec![
                "acc-active",
                "acc-inactive",
                "acc-unavailable",
                "acc-missing"
            ]
        );
        assert_eq!(tasks[0].workspace_id.as_deref(), Some("ws-active"));
        assert_eq!(tasks[1].workspace_id.as_deref(), Some("ws-inactive"));
        assert_eq!(tasks[2].workspace_id.as_deref(), Some("ws-unavailable"));
        assert_eq!(tasks[3].workspace_id, None);
    }
}
