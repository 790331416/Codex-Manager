use codexmanager_core::storage::{now_ts, Account, Token};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::storage_helpers::open_storage;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccountExportResult {
    output_dir: String,
    total_accounts: usize,
    exported: usize,
    skipped_missing_token: usize,
    files: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccountExportDataResult {
    total_accounts: usize,
    exported: usize,
    skipped_missing_token: usize,
    files: Vec<ExportAccountFile>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportAccountFile {
    file_name: String,
    content: String,
}

#[derive(Debug, Clone, Serialize)]
struct ExportAccountPayload {
    tokens: ExportTokensPayload,
    meta: ExportMetaPayload,
}

#[derive(Debug, Clone, Serialize)]
struct ExportTokensPayload {
    access_token: String,
    id_token: String,
    refresh_token: String,
    account_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportMetaPayload {
    label: String,
    issuer: String,
    note: Option<String>,
    tags: Option<String>,
    status: String,
    workspace_id: Option<String>,
    chatgpt_account_id: Option<String>,
    exported_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountExportMode {
    SingleJson,
    MultipleJson,
}

impl AccountExportMode {
    fn parse(value: Option<&str>) -> Self {
        let normalized = value.unwrap_or("multiple").trim().to_ascii_lowercase();
        if normalized == "single" {
            Self::SingleJson
        } else {
            Self::MultipleJson
        }
    }
}

/// 函数 `export_accounts_to_directory`
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
pub(crate) fn export_accounts_to_directory(
    output_dir: &str,
    selected_account_ids: &[String],
    export_mode: Option<&str>,
) -> Result<AccountExportResult, String> {
    let normalized_output_dir = output_dir.trim();
    if normalized_output_dir.is_empty() {
        return Err("missing outputDir".to_string());
    }

    let output_path = PathBuf::from(normalized_output_dir);
    std::fs::create_dir_all(&output_path).map_err(|err| {
        format!(
            "create output directory failed ({}): {err}",
            output_path.display()
        )
    })?;

    let storage = open_storage().ok_or_else(|| "storage unavailable".to_string())?;
    let accounts = select_accounts_for_export(
        storage.list_accounts().map_err(|err| err.to_string())?,
        selected_account_ids,
    );
    let metadata = storage
        .list_account_metadata()
        .map_err(|err| err.to_string())?
        .into_iter()
        .map(|item| (item.account_id.clone(), item))
        .collect::<HashMap<_, _>>();
    let total_accounts = accounts.len();
    let mut exported = 0usize;
    let mut skipped_missing_token = 0usize;
    let mut files = Vec::new();
    let mut file_name_counter: HashMap<String, usize> = HashMap::new();
    let export_mode = AccountExportMode::parse(export_mode);

    match export_mode {
        AccountExportMode::MultipleJson => {
            for account in accounts {
                let token = storage
                    .find_token_by_account_id(&account.id)
                    .map_err(|err| err.to_string())?;
                let Some(token) = token else {
                    skipped_missing_token += 1;
                    continue;
                };

                let file_path =
                    build_account_export_file_path(&output_path, &account, &mut file_name_counter);
                let json = build_account_export_json(&account, &token, metadata.get(&account.id))?;
                std::fs::write(&file_path, json).map_err(|err| {
                    format!("write export file failed ({}): {err}", file_path.display())
                })?;

                exported += 1;
                files.push(file_path.display().to_string());
            }
        }
        AccountExportMode::SingleJson => {
            let bundle = build_single_export_bundle_json(&storage, &accounts, &metadata)?;
            exported = bundle.exported;
            skipped_missing_token = bundle.skipped_missing_token;
            if let Some(content) = bundle.content {
                let file_path = output_path.join("accounts.json");
                std::fs::write(&file_path, content).map_err(|err| {
                    format!("write export file failed ({}): {err}", file_path.display())
                })?;
                files.push(file_path.display().to_string());
            }
        }
    }

    Ok(AccountExportResult {
        output_dir: output_path.display().to_string(),
        total_accounts,
        exported,
        skipped_missing_token,
        files,
    })
}

/// 函数 `export_accounts_data`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - selected_account_ids: 参数 selected_account_ids
/// - export_mode: 参数 export_mode
///
/// # 返回
/// 返回函数执行结果
pub(crate) fn export_accounts_data(
    selected_account_ids: &[String],
    export_mode: Option<&str>,
) -> Result<AccountExportDataResult, String> {
    let storage = open_storage().ok_or_else(|| "storage unavailable".to_string())?;
    let accounts = select_accounts_for_export(
        storage.list_accounts().map_err(|err| err.to_string())?,
        selected_account_ids,
    );
    let metadata = storage
        .list_account_metadata()
        .map_err(|err| err.to_string())?
        .into_iter()
        .map(|item| (item.account_id.clone(), item))
        .collect::<HashMap<_, _>>();
    let total_accounts = accounts.len();
    let mut exported = 0usize;
    let mut skipped_missing_token = 0usize;
    let mut files = Vec::new();
    let mut file_name_counter: HashMap<String, usize> = HashMap::new();

    match AccountExportMode::parse(export_mode) {
        AccountExportMode::MultipleJson => {
            for account in accounts {
                let token = storage
                    .find_token_by_account_id(&account.id)
                    .map_err(|err| err.to_string())?;
                let Some(token) = token else {
                    skipped_missing_token += 1;
                    continue;
                };

                let file_path =
                    build_account_export_file_path(Path::new(""), &account, &mut file_name_counter);
                let file_name = file_path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .map(str::to_string)
                    .ok_or_else(|| "build export file name failed".to_string())?;
                let json = build_account_export_json(&account, &token, metadata.get(&account.id))?;
                let content = String::from_utf8(json)
                    .map_err(|err| format!("encode export utf8 failed: {err}"))?;
                files.push(ExportAccountFile { file_name, content });
                exported += 1;
            }
        }
        AccountExportMode::SingleJson => {
            let bundle = build_single_export_bundle_json(&storage, &accounts, &metadata)?;
            exported = bundle.exported;
            skipped_missing_token = bundle.skipped_missing_token;
            if let Some(content) = bundle.content {
                files.push(ExportAccountFile {
                    file_name: "accounts.json".to_string(),
                    content: String::from_utf8(content)
                        .map_err(|err| format!("encode export utf8 failed: {err}"))?,
                });
            }
        }
    }

    Ok(AccountExportDataResult {
        total_accounts,
        exported,
        skipped_missing_token,
        files,
    })
}

struct SingleExportBundleResult {
    content: Option<Vec<u8>>,
    exported: usize,
    skipped_missing_token: usize,
}

fn build_single_export_bundle_json(
    storage: &codexmanager_core::storage::Storage,
    accounts: &[Account],
    metadata: &HashMap<String, codexmanager_core::storage::AccountMetadata>,
) -> Result<SingleExportBundleResult, String> {
    let mut exported = 0usize;
    let mut skipped_missing_token = 0usize;
    let mut payloads = Vec::new();

    for account in accounts {
        let token = storage
            .find_token_by_account_id(&account.id)
            .map_err(|err| err.to_string())?;
        let Some(token) = token else {
            skipped_missing_token += 1;
            continue;
        };

        payloads.push(build_account_export_payload(
            account,
            &token,
            metadata.get(&account.id),
        ));
        exported += 1;
    }

    let content = if payloads.is_empty() {
        None
    } else {
        Some(
            serde_json::to_vec_pretty(&payloads)
                .map_err(|err| format!("encode export json failed: {err}"))?,
        )
    };

    Ok(SingleExportBundleResult {
        content,
        exported,
        skipped_missing_token,
    })
}

fn select_accounts_for_export(
    accounts: Vec<Account>,
    selected_account_ids: &[String],
) -> Vec<Account> {
    if selected_account_ids.is_empty() {
        return accounts;
    }
    let selected = selected_account_ids
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<HashSet<_>>();
    if selected.is_empty() {
        return accounts;
    }
    accounts
        .into_iter()
        .filter(|account| selected.contains(account.id.as_str()))
        .collect()
}

/// 函数 `build_account_export_file_path`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - output_dir: 参数 output_dir
/// - account: 参数 account
/// - file_name_counter: 参数 file_name_counter
///
/// # 返回
/// 返回函数执行结果
fn build_account_export_file_path(
    output_dir: &Path,
    account: &Account,
    file_name_counter: &mut HashMap<String, usize>,
) -> PathBuf {
    let label_part = sanitize_file_stem(&account.label);
    let id_part = sanitize_file_stem(&account.id);
    let mut stem = if label_part.is_empty() {
        id_part.clone()
    } else if id_part.is_empty() {
        label_part.clone()
    } else {
        format!("{label_part}_{id_part}")
    };
    if stem.is_empty() {
        stem = "account".to_string();
    }

    let sequence = file_name_counter.entry(stem.clone()).or_insert(0);
    let file_stem = if *sequence == 0 {
        stem
    } else {
        format!("{stem}_{}", *sequence)
    };
    *sequence += 1;

    output_dir.join(format!("{file_stem}.json"))
}

/// 函数 `build_account_export_json`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - account: 参数 account
/// - token: 参数 token
/// - metadata: 参数 metadata
///
/// # 返回
/// 返回函数执行结果
fn build_account_export_json(
    account: &Account,
    token: &Token,
    metadata: Option<&codexmanager_core::storage::AccountMetadata>,
) -> Result<Vec<u8>, String> {
    serde_json::to_vec_pretty(&build_account_export_payload(account, token, metadata))
        .map_err(|err| format!("encode export json failed: {err}"))
}

fn build_account_export_payload(
    account: &Account,
    token: &Token,
    metadata: Option<&codexmanager_core::storage::AccountMetadata>,
) -> ExportAccountPayload {
    ExportAccountPayload {
        tokens: ExportTokensPayload {
            access_token: token.access_token.clone(),
            id_token: token.id_token.clone(),
            refresh_token: token.refresh_token.clone(),
            account_id: account.id.clone(),
        },
        meta: ExportMetaPayload {
            label: account.label.clone(),
            issuer: account.issuer.clone(),
            note: metadata.and_then(|value| value.note.clone()),
            tags: metadata.and_then(|value| value.tags.clone()),
            status: account.status.clone(),
            workspace_id: account.workspace_id.clone(),
            chatgpt_account_id: account.chatgpt_account_id.clone(),
            exported_at: now_ts(),
        },
    }
}

/// 函数 `sanitize_file_stem`
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
fn sanitize_file_stem(value: &str) -> String {
    let mut out = String::with_capacity(value.len().min(96));
    for ch in value.trim().chars() {
        if out.len() >= 96 {
            break;
        }
        let invalid =
            matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*') || ch.is_control();
        if invalid {
            out.push('_');
            continue;
        }
        out.push(ch);
    }

    out.trim_matches(|ch: char| ch == ' ' || ch == '.')
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        build_single_export_bundle_json, sanitize_file_stem, select_accounts_for_export,
    };
    use codexmanager_core::storage::{Account, Storage, Token};
    use std::collections::HashMap;

    fn sample_account(id: &str, label: &str) -> Account {
        Account {
            id: id.to_string(),
            label: label.to_string(),
            issuer: "https://auth.openai.com".to_string(),
            chatgpt_account_id: None,
            workspace_id: None,
            group_name: None,
            sort: 0,
            status: "active".to_string(),
            created_at: 0,
            updated_at: 0,
        }
    }

    fn sample_token(account_id: &str) -> Token {
        Token {
            account_id: account_id.to_string(),
            id_token: "id".to_string(),
            access_token: "access".to_string(),
            refresh_token: "refresh".to_string(),
            api_key_access_token: None,
            last_refresh: 0,
        }
    }

    /// 函数 `sanitize_file_stem_replaces_windows_invalid_chars`
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
    fn sanitize_file_stem_replaces_windows_invalid_chars() {
        let actual = sanitize_file_stem(r#"a<b>c:d"e/f\g|h?i*j"#);
        assert_eq!(actual, "a_b_c_d_e_f_g_h_i_j");
    }

    /// 函数 `sanitize_file_stem_trims_tailing_space_and_dot`
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
    fn sanitize_file_stem_trims_tailing_space_and_dot() {
        let actual = sanitize_file_stem(" demo. ");
        assert_eq!(actual, "demo");
    }

    /// 函数 `select_accounts_for_export_returns_selected_subset`
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
    fn select_accounts_for_export_returns_selected_subset() {
        let accounts = vec![
            sample_account("acc-1", "first"),
            sample_account("acc-2", "second"),
            sample_account("acc-3", "third"),
        ];
        let selected = vec!["acc-2".to_string(), "acc-3".to_string()];
        let actual = select_accounts_for_export(accounts, &selected);

        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0].id, "acc-2");
        assert_eq!(actual[1].id, "acc-3");
    }

    /// 函数 `single_export_bundle_uses_array_shape_for_reimport`
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
    fn single_export_bundle_uses_array_shape_for_reimport() {
        let storage = Storage::open_in_memory().expect("open in memory");
        storage.init().expect("init");
        let account = sample_account("acc-1", "first");
        storage.insert_account(&account).expect("insert account");
        storage
            .insert_token(&sample_token("acc-1"))
            .expect("insert token");

        let bundle = build_single_export_bundle_json(&storage, &[account], &HashMap::new())
            .expect("build export bundle");
        let content = bundle.content.expect("bundle content");
        let value: serde_json::Value = serde_json::from_slice(&content).expect("parse bundle");

        assert!(value.is_array());
        assert_eq!(value.as_array().map(Vec::len), Some(1));
    }
}
