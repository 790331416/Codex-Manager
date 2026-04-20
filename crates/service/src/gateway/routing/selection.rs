use codexmanager_core::storage::{now_ts, Account, Storage, Token, UsageSnapshotRecord};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use crate::usage_account_meta::{derive_account_meta, patch_account_meta_in_place};

static CANDIDATE_SNAPSHOT_CACHE: OnceLock<Mutex<Option<CandidateSnapshotCache>>> = OnceLock::new();
static SELECTION_CONFIG_LOADED: OnceLock<()> = OnceLock::new();
static CANDIDATE_CACHE_TTL_MS: AtomicU64 = AtomicU64::new(DEFAULT_CANDIDATE_CACHE_TTL_MS);
static CURRENT_DB_PATH: OnceLock<RwLock<String>> = OnceLock::new();
const DEFAULT_CANDIDATE_CACHE_TTL_MS: u64 = 500;
const CANDIDATE_CACHE_TTL_ENV: &str = "CODEXMANAGER_CANDIDATE_CACHE_TTL_MS";
// OpenAI 在 used_percent 未到 100 时就会触发 usage limit（常见于 ChatGPT Plus OAuth
// 账号的 5 小时窗口）。将快要耗尽的账号降权到候选列表尾部，避免网关反复挑到它。
const LOW_QUOTA_THRESHOLD_ENV: &str = "CODEXMANAGER_LOW_QUOTA_THRESHOLD_PERCENT";
const DEFAULT_LOW_QUOTA_THRESHOLD_PERCENT: f64 = 95.0;

#[derive(Clone)]
struct CandidateSnapshotCache {
    db_path: String,
    expires_at: Instant,
    candidates: Vec<(Account, Token)>,
}

/// 函数 `collect_gateway_candidates`
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
pub(crate) fn collect_gateway_candidates(
    storage: &Storage,
) -> Result<Vec<(Account, Token)>, String> {
    if let Some(cached) = read_candidate_cache() {
        return Ok(cached);
    }

    let candidates = collect_gateway_candidates_uncached(storage)?;
    write_candidate_cache(candidates.clone());
    Ok(candidates)
}

/// 函数 `collect_gateway_candidates_uncached`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - storage: 参数 storage
///
/// # 返回
/// 返回函数执行结果
fn collect_gateway_candidates_uncached(storage: &Storage) -> Result<Vec<(Account, Token)>, String> {
    // 选择可用账号作为网关上游候选
    let candidates = storage
        .list_gateway_candidates()
        .map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(candidates.len());
    for (account, token) in candidates {
        let mut candidate_account = account.clone();
        let (chatgpt_account_id, workspace_id) = derive_account_meta(&token);
        if patch_account_meta_in_place(&mut candidate_account, chatgpt_account_id, workspace_id) {
            candidate_account.updated_at = now_ts();
            let _ = storage.insert_account(&candidate_account);
        }
        out.push((candidate_account, token));
    }
    demote_low_quota_candidates(storage, &mut out);
    if out.is_empty() {
        log_no_candidates(storage);
    }
    Ok(out)
}

/// 将快要耗尽的账号（primary 或 secondary used_percent 超过阈值）稳定地排到列表尾部。
/// 不从候选中剔除，保证在全部账号都被降权的极端场景下仍有号可用。
fn demote_low_quota_candidates(storage: &Storage, candidates: &mut Vec<(Account, Token)>) {
    if candidates.len() < 2 {
        return;
    }
    let snapshots = load_usage_snapshots(storage);
    if snapshots.is_empty() {
        return;
    }
    let threshold = low_quota_threshold_percent();
    candidates.sort_by_key(|(account, _)| {
        if is_low_quota_account(&account.id, &snapshots, threshold) {
            1u8
        } else {
            0u8
        }
    });
}

fn load_usage_snapshots(storage: &Storage) -> HashMap<String, UsageSnapshotRecord> {
    storage
        .latest_usage_snapshots_by_account()
        .unwrap_or_default()
        .into_iter()
        .map(|snap| (snap.account_id.clone(), snap))
        .collect()
}

fn is_low_quota_account(
    account_id: &str,
    snapshots: &HashMap<String, UsageSnapshotRecord>,
    threshold: f64,
) -> bool {
    let Some(snap) = snapshots.get(account_id) else {
        return false;
    };
    let primary_low = snap.used_percent.is_some_and(|pct| pct >= threshold);
    let secondary_low = snap
        .secondary_used_percent
        .is_some_and(|pct| pct >= threshold);
    primary_low || secondary_low
}

fn low_quota_threshold_percent() -> f64 {
    std::env::var(LOW_QUOTA_THRESHOLD_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .filter(|pct| pct.is_finite() && *pct > 0.0 && *pct <= 100.0)
        .unwrap_or(DEFAULT_LOW_QUOTA_THRESHOLD_PERCENT)
}

/// 函数 `read_candidate_cache`
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
fn read_candidate_cache() -> Option<Vec<(Account, Token)>> {
    let ttl = candidate_cache_ttl();
    if ttl.is_zero() {
        return None;
    }
    let db_path = cache_identity()?;
    let now = Instant::now();
    let mutex = CANDIDATE_SNAPSHOT_CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!("candidate snapshot cache lock poisoned; dropping cache and continuing");
            let mut guard = poisoned.into_inner();
            *guard = None;
            guard
        }
    };
    let cached = guard.as_ref()?;
    if cached.db_path != db_path || cached.expires_at <= now {
        *guard = None;
        return None;
    }
    Some(cached.candidates.clone())
}

/// 函数 `write_candidate_cache`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - candidates: 参数 candidates
///
/// # 返回
/// 无
fn write_candidate_cache(candidates: Vec<(Account, Token)>) {
    let ttl = candidate_cache_ttl();
    if ttl.is_zero() {
        return;
    }
    let Some(db_path) = cache_identity() else {
        return;
    };
    let expires_at = Instant::now() + ttl;
    let mutex = CANDIDATE_SNAPSHOT_CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::warn!("candidate snapshot cache lock poisoned; recovering");
            poisoned.into_inner()
        }
    };
    *guard = Some(CandidateSnapshotCache {
        db_path,
        expires_at,
        candidates,
    });
}

/// 函数 `cache_identity`
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
fn cache_identity() -> Option<String> {
    let db_path = current_db_path();
    if db_path.trim().is_empty() || db_path == "<unset>" {
        return None;
    }
    Some(db_path)
}

/// 函数 `candidate_cache_ttl`
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
fn candidate_cache_ttl() -> Duration {
    ensure_selection_config_loaded();
    let ttl_ms = CANDIDATE_CACHE_TTL_MS.load(Ordering::Relaxed);
    Duration::from_millis(ttl_ms)
}

/// 函数 `current_db_path`
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
fn current_db_path() -> String {
    ensure_selection_config_loaded();
    crate::lock_utils::read_recover(current_db_path_cell(), "current_db_path").clone()
}

/// 函数 `log_no_candidates`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - storage: 参数 storage
///
/// # 返回
/// 无
fn log_no_candidates(storage: &Storage) {
    let accounts = storage.list_accounts().unwrap_or_default();
    let tokens = storage.list_tokens().unwrap_or_default();
    let snaps = storage
        .latest_usage_snapshots_by_account()
        .unwrap_or_default();
    let token_map = tokens
        .into_iter()
        .map(|token| (token.account_id.clone(), token))
        .collect::<std::collections::HashMap<_, _>>();
    let snap_map = snaps
        .into_iter()
        .map(|snap| (snap.account_id.clone(), snap))
        .collect::<std::collections::HashMap<_, _>>();
    let db_path = current_db_path();
    log::warn!(
        "gateway no candidates: db_path={}, accounts={}, tokens={}, snapshots={}",
        db_path,
        accounts.len(),
        token_map.len(),
        snap_map.len()
    );
    for account in accounts {
        let usage = snap_map.get(&account.id);
        log::warn!(
            "gateway account: id={}, status={}, has_token={}, primary=({:?}/{:?}) secondary=({:?}/{:?})",
            account.id,
            account.status,
            token_map.contains_key(&account.id),
            usage.and_then(|u| u.used_percent),
            usage.and_then(|u| u.window_minutes),
            usage.and_then(|u| u.secondary_used_percent),
            usage.and_then(|u| u.secondary_window_minutes),
        );
    }
}

/// 函数 `reload_from_env`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 无
pub(super) fn reload_from_env() {
    let ttl_ms = std::env::var(CANDIDATE_CACHE_TTL_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_CANDIDATE_CACHE_TTL_MS);
    CANDIDATE_CACHE_TTL_MS.store(ttl_ms, Ordering::Relaxed);

    let db_path = std::env::var("CODEXMANAGER_DB_PATH").unwrap_or_else(|_| "<unset>".to_string());
    let mut cached = crate::lock_utils::write_recover(current_db_path_cell(), "current_db_path");
    *cached = db_path;
    clear_candidate_cache();
}

pub(crate) fn invalidate_candidate_cache() {
    clear_candidate_cache();
}

/// 函数 `ensure_selection_config_loaded`
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
fn ensure_selection_config_loaded() {
    let _ = SELECTION_CONFIG_LOADED.get_or_init(|| reload_from_env());
}

/// 函数 `current_db_path_cell`
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
fn current_db_path_cell() -> &'static RwLock<String> {
    CURRENT_DB_PATH.get_or_init(|| RwLock::new("<unset>".to_string()))
}

/// 函数 `clear_candidate_cache`
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
fn clear_candidate_cache() {
    if let Some(mutex) = CANDIDATE_SNAPSHOT_CACHE.get() {
        let mut guard = match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::warn!("candidate snapshot cache lock poisoned; recovering for tests");
                poisoned.into_inner()
            }
        };
        *guard = None;
    }
}

/// 函数 `clear_candidate_cache_for_tests`
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
#[cfg(test)]
fn clear_candidate_cache_for_tests() {
    clear_candidate_cache();
}

#[cfg(test)]
#[path = "tests/selection_tests.rs"]
mod tests;
