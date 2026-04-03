use super::{
    clear_pending_usage_refresh_tasks_for_tests, enqueue_usage_refresh_with_worker,
    next_usage_poll_cursor, reset_usage_poll_cursor_for_tests, run_token_refresh_task,
    token_refresh_schedule, usage_poll_batch_indices,
};
use codexmanager_core::storage::{now_ts, Storage, Token};
use std::collections::HashSet;
use std::sync::mpsc;
use std::time::Duration;

/// 函数 `enqueue_usage_refresh_for_same_account_is_deduplicated_until_finish`
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
fn enqueue_usage_refresh_for_same_account_is_deduplicated_until_finish() {
    let _guard = crate::test_env_guard();
    clear_pending_usage_refresh_tasks_for_tests();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = enqueue_usage_refresh_with_worker("acc-dedup", move |_| {
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    });
    assert!(first);
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("worker started");

    let second = enqueue_usage_refresh_with_worker("acc-dedup", |_| {});
    assert!(!second);

    let _ = release_tx.send(());
    std::thread::sleep(Duration::from_millis(20));

    let third = enqueue_usage_refresh_with_worker("acc-dedup", |_| {});
    assert!(third);
    std::thread::sleep(Duration::from_millis(20));
    clear_pending_usage_refresh_tasks_for_tests();
}

/// 函数 `enqueue_usage_refresh_for_different_accounts_keeps_queue_progress`
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
fn enqueue_usage_refresh_for_different_accounts_keeps_queue_progress() {
    let _guard = crate::test_env_guard();
    clear_pending_usage_refresh_tasks_for_tests();
    let (started_tx, started_rx) = mpsc::channel::<String>();
    let (release_tx, release_rx) = mpsc::channel();
    let started_tx_first = started_tx.clone();

    let first = enqueue_usage_refresh_with_worker("acc-a", move |_| {
        let _ = started_tx_first.send("acc-a".to_string());
        let _ = release_rx.recv_timeout(Duration::from_secs(1));
    });
    assert!(first);

    let started_tx = started_tx.clone();
    let second = enqueue_usage_refresh_with_worker("acc-b", move |_| {
        let _ = started_tx.send("acc-b".to_string());
    });
    assert!(second);

    let first_started = started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first task should start");
    let _ = release_tx.send(());
    let second_started = started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second task should start");

    let seen: HashSet<String> = [first_started, second_started].into_iter().collect();
    assert_eq!(seen.len(), 2);
    assert!(seen.contains("acc-a"));
    assert!(seen.contains("acc-b"));

    std::thread::sleep(Duration::from_millis(20));
    clear_pending_usage_refresh_tasks_for_tests();
}

/// 函数 `schedule_prefers_exp_minus_ahead`
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
fn schedule_prefers_exp_minus_ahead() {
    let now = now_ts();
    let token = Token {
        account_id: "acc-1".to_string(),
        id_token: "id".to_string(),
        access_token: "a.eyJleHAiOjQxMDI0NDQ4MDB9.s".to_string(),
        refresh_token: "refresh".to_string(),
        api_key_access_token: None,
        last_refresh: now - 10,
    };
    let (exp, scheduled_at) = token_refresh_schedule(&token, now, 600, 2700);
    assert_eq!(exp, Some(4_102_444_800));
    assert_eq!(scheduled_at, 4_102_444_200);
}

/// 函数 `schedule_falls_back_to_last_refresh_when_exp_missing`
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
fn schedule_falls_back_to_last_refresh_when_exp_missing() {
    let now = now_ts();
    let token = Token {
        account_id: "acc-2".to_string(),
        id_token: "id".to_string(),
        access_token: "no-jwt".to_string(),
        refresh_token: "refresh".to_string(),
        api_key_access_token: None,
        last_refresh: now - 5000,
    };
    let (exp, scheduled_at) = token_refresh_schedule(&token, now, 300, 2700);
    assert_eq!(exp, None);
    assert_eq!(scheduled_at, now);
}

/// 函数 `schedule_skips_when_refresh_token_is_empty`
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
fn schedule_skips_when_refresh_token_is_empty() {
    let now = now_ts();
    let token = Token {
        account_id: "acc-empty-refresh".to_string(),
        id_token: "id".to_string(),
        access_token: "a.eyJleHAiOjQxMDI0NDQ4MDB9.s".to_string(),
        refresh_token: String::new(),
        api_key_access_token: None,
        last_refresh: now - 10,
    };
    let (exp, scheduled_at) = token_refresh_schedule(&token, now, 600, 2700);
    assert_eq!(exp, None);
    assert_eq!(scheduled_at, i64::MAX);
}

/// 函数 `run_token_refresh_task_skips_empty_refresh_token`
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
fn run_token_refresh_task_skips_empty_refresh_token() {
    let storage = Storage::open_in_memory().expect("open in memory");
    storage.init().expect("init");
    let now = now_ts();
    let mut token = Token {
        account_id: "acc-empty-refresh".to_string(),
        id_token: "id".to_string(),
        access_token: "access".to_string(),
        refresh_token: String::new(),
        api_key_access_token: None,
        last_refresh: now,
    };

    let refreshed =
        run_token_refresh_task(&storage, &mut token, "https://auth.openai.com", "codex-cli");
    assert!(!refreshed);
}

/// 函数 `usage_poll_batch_indices_rotate_from_cursor`
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
fn usage_poll_batch_indices_rotate_from_cursor() {
    reset_usage_poll_cursor_for_tests();
    assert_eq!(usage_poll_batch_indices(5, 4, 3), vec![4, 0, 1]);
    assert_eq!(usage_poll_batch_indices(3, 1, 10), vec![1, 2, 0]);
}

/// 函数 `usage_poll_cursor_advances_by_processed_count`
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
fn usage_poll_cursor_advances_by_processed_count() {
    reset_usage_poll_cursor_for_tests();
    assert_eq!(next_usage_poll_cursor(5, 4, 2), 1);
    assert_eq!(next_usage_poll_cursor(5, 1, 5), 1);
    assert_eq!(next_usage_poll_cursor(0, 7, 3), 0);
}
