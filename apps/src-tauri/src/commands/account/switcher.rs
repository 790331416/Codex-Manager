use crate::app_storage::resolve_db_path_with_legacy_migration;
use chrono::{SecondsFormat, Utc};
use codexmanager_core::storage::{Storage, Token};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const OPENAI_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const OPENAI_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const REFRESH_THRESHOLD_SECS: u64 = 300;

fn get_auth_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    use tauri::Manager;
    let home = app.path().home_dir().map_err(|e| e.to_string())?;
    Ok(home.join(".codex").join("auth.json"))
}

fn stop_codex_processes() -> Result<(), String> {
    let output = Command::new("powershell")
        .args(&[
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            r#"
$ErrorActionPreference = 'SilentlyContinue'
function Test-CodexProcess($p) {
  $name = [string]$p.Name
  $path = [string]$p.ExecutablePath
  return $name -ieq 'Codex.exe' `
    -or $name -ieq 'codex.exe' `
    -or $path -match '\\OpenAI\.Codex_' `
    -or $path -match '\\AppData\\Local\\OpenAI\\Codex\\'
}
function Get-CodexProcessTreeIds {
  $all = @(Get-CimInstance Win32_Process)
  $ids = @{}
  foreach ($p in $all) {
    if (Test-CodexProcess $p) {
      $ids[[int]$p.ProcessId] = $true
    }
  }
  $changed = $true
  while ($changed) {
    $changed = $false
    foreach ($p in $all) {
      $pid = [int]$p.ProcessId
      $ppid = [int]$p.ParentProcessId
      if (-not $ids.ContainsKey($pid) -and $ids.ContainsKey($ppid)) {
        $ids[$pid] = $true
        $changed = $true
      }
    }
  }
  return @($ids.Keys | Sort-Object -Descending)
}
function Get-CodexRootIds {
  $all = @(Get-CimInstance Win32_Process)
  $ids = @{}
  foreach ($p in $all) {
    if (Test-CodexProcess $p) {
      $ids[[int]$p.ProcessId] = [int]$p.ParentProcessId
    }
  }
  $roots = @()
  foreach ($pid in $ids.Keys) {
    $ppid = $ids[$pid]
    if (-not $ids.ContainsKey($ppid)) {
      $roots += [int]$pid
    }
  }
  return @($roots | Sort-Object -Descending)
}
$rootIds = @(Get-CodexRootIds)
foreach ($id in $rootIds) {
  & taskkill.exe /PID $id /T /F | Out-Null
}
for ($i = 0; $i -lt 20; $i++) {
  Start-Sleep -Milliseconds 500
  $remaining = @(Get-CodexProcessTreeIds)
  if ($remaining.Count -eq 0) {
    exit 0
  }
}
$left = @(Get-CodexProcessTreeIds) -join ','
throw "Codex processes still running: $left"
"#,
        ])
        .output()
        .map_err(|e| format!("Failed to stop Codex: {}", e))?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let detail = if stderr.is_empty() { stdout } else { stderr };
        return Err(format!("Failed to stop Codex processes: {}", detail));
    }
    Ok(())
}

fn launch_codex() -> Result<(), String> {
    let executable = find_codex_exe()?;
    let parent_dir = executable.parent().unwrap_or(Path::new("C:\\"));
    let mut child = std::process::Command::new(&executable);
    child.current_dir(parent_dir);
    child
        .spawn()
        .map_err(|e| format!("Failed to start Codex: {}", e))?;
    Ok(())
}

fn find_codex_exe() -> Result<PathBuf, String> {
    if let Ok(output) = Command::new("powershell")
        .args(&[
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            "(Get-AppxPackage -Name 'OpenAI.Codex' | Select-Object -First 1).InstallLocation",
        ])
        .output()
    {
        let install_loc = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !install_loc.is_empty() {
            let exe = PathBuf::from(&install_loc).join("app").join("Codex.exe");
            if exe.exists() {
                return Ok(exe);
            }
        }
    }

    let known_paths = [r"C:\Program Files\WindowsApps"];
    for base in &known_paths {
        if let Ok(entries) = std::fs::read_dir(base) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("OpenAI.Codex_") {
                    let p = entry.path().join("app").join("Codex.exe");
                    if p.exists() {
                        return Ok(p);
                    }
                }
            }
        }
    }

    if let Ok(output) = Command::new("where.exe").arg("Codex.exe").output() {
        let path_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path_str.is_empty() {
            let p = PathBuf::from(path_str.lines().next().unwrap_or(""));
            if p.exists() {
                return Ok(p);
            }
        }
    }

    Err("Cannot find Codex.exe".to_string())
}

fn atomic_write_json(path: &Path, payload: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("mkdir failed: {}", e))?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let serialized =
        serde_json::to_string_pretty(payload).map_err(|e| format!("Serialize failed: {}", e))?;
    fs::write(&tmp_path, serialized).map_err(|e| format!("Write failed: {}", e))?;
    if path.exists() {
        fs::remove_file(path).map_err(|e| format!("Remove old file failed: {}", e))?;
    }
    fs::rename(&tmp_path, path).map_err(|e| format!("Rename failed: {}", e))?;
    Ok(())
}

fn base64_decode_url_safe(input: &str) -> Option<Vec<u8>> {
    let standard: String = input
        .chars()
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            other => other,
        })
        .collect();
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = Vec::new();
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for byte in standard.bytes() {
        if byte == b'=' {
            break;
        }
        let val = alphabet.iter().position(|&b| b == byte)? as u32;
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    Some(output)
}

fn decode_jwt_exp(token: &str) -> Option<u64> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let payload_b64 = parts[1];
    let padded = match payload_b64.len() % 4 {
        2 => format!("{}==", payload_b64),
        3 => format!("{}=", payload_b64),
        _ => payload_b64.to_string(),
    };
    let decoded_bytes = base64_decode_url_safe(&padded)?;
    let payload_str = String::from_utf8(decoded_bytes).ok()?;
    let payload: Value = serde_json::from_str(&payload_str).ok()?;
    payload.get("exp")?.as_u64()
}

fn is_token_expired(tag: &str, token_str: &str) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    match decode_jwt_exp(token_str) {
        Some(exp) => {
            let expired = exp <= now + REFRESH_THRESHOLD_SECS;
            log::info!(
                "[switcher] {} exp={}, now={}, threshold={}, expired={}",
                tag,
                exp,
                now,
                REFRESH_THRESHOLD_SECS,
                expired
            );
            expired
        }
        None => {
            log::warn!(
                "[switcher] {} cannot decode JWT exp, treating as expired",
                tag
            );
            true
        }
    }
}

async fn refresh_tokens(refresh_token: &str) -> Result<(String, String, String), String> {
    log::info!("[switcher] building reqwest client for token refresh...");
    log::info!(
        "[switcher] HTTPS_PROXY env = {:?}",
        std::env::var("HTTPS_PROXY").ok()
    );
    log::info!(
        "[switcher] https_proxy env = {:?}",
        std::env::var("https_proxy").ok()
    );
    log::info!(
        "[switcher] HTTP_PROXY env = {:?}",
        std::env::var("HTTP_PROXY").ok()
    );
    log::info!(
        "[switcher] ALL_PROXY env = {:?}",
        std::env::var("ALL_PROXY").ok()
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("HTTP client build failed: {}", e))?;

    log::info!(
        "[switcher] POST {} with refresh_token={}...",
        OPENAI_TOKEN_URL,
        &refresh_token[..20.min(refresh_token.len())]
    );

    let params = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", OPENAI_CLIENT_ID),
    ];

    let resp = client
        .post(OPENAI_TOKEN_URL)
        .form(&params)
        .send()
        .await
        .map_err(|e| {
            log::error!("[switcher] refresh request FAILED: {}", e);
            format!("Token refresh request failed: {}", e)
        })?;

    let status = resp.status();
    log::info!("[switcher] refresh response status: {}", status);

    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        log::error!("[switcher] refresh FAILED body: {}", body);
        return Err(format!("Token refresh failed (HTTP {}): {}", status, body));
    }

    let json: Value = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse refresh response: {}", e))?;

    let new_at = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or("No access_token in refresh response")?
        .to_string();
    let new_it = json
        .get("id_token")
        .and_then(|v| v.as_str())
        .ok_or("No id_token in refresh response")?
        .to_string();
    let new_rt = json
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .unwrap_or(refresh_token)
        .to_string();

    log::info!(
        "[switcher] refresh SUCCESS! new access_token len={}, new id_token len={}",
        new_at.len(),
        new_it.len()
    );
    Ok((new_at, new_it, new_rt))
}

fn decode_jwt_chatgpt_account_id(token: &str) -> Option<String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let payload_b64 = parts[1];
    let padded = match payload_b64.len() % 4 {
        2 => format!("{}==", payload_b64),
        3 => format!("{}=", payload_b64),
        _ => payload_b64.to_string(),
    };
    let decoded_bytes = base64_decode_url_safe(&padded)?;
    let payload_str = String::from_utf8(decoded_bytes).ok()?;
    let payload: Value = serde_json::from_str(&payload_str).ok()?;

    let auth_obj = payload.get("https://api.openai.com/auth")?;
    let account_id = auth_obj.get("chatgpt_account_id")?.as_str()?;
    Some(account_id.to_string())
}

fn now_unix_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn get_config_toml_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    use tauri::Manager;
    let home = app.path().home_dir().map_err(|e| e.to_string())?;
    Ok(home.join(".codex").join("config.toml"))
}

fn set_config_toml_proxy(app: &tauri::AppHandle, enable: bool) -> Result<(), String> {
    let config_path = get_config_toml_path(app)?;
    if !config_path.exists() {
        if !enable {
            return Ok(()); // Nothing to remove
        }
        // File does not exist, but we want to enable proxy. Create parent dir and write basic template.
        if let Some(parent) = config_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let content = "openai_base_url = \"http://127.0.0.1:45760/v1\"\n";
        fs::write(&config_path, content)
            .map_err(|e| format!("Failed to create config.toml: {}", e))?;
        return Ok(());
    }

    let existing_content = fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
    let mut lines: Vec<&str> = existing_content.lines().collect();
    // Remove all lines starting with "openai_base_url" (ignoring leading whitespace)
    lines.retain(|l| !l.trim_start().starts_with("openai_base_url"));

    // If enabling proxy, we want to add the url at the top or bottom of global config.
    // Usually it's safe to just prepend or append. Let's prepend to the front.
    let mut new_content = String::new();
    if enable {
        new_content.push_str("openai_base_url = \"http://127.0.0.1:45760/v1\"\n");
    }
    for line in lines {
        new_content.push_str(line);
        new_content.push('\n');
    }

    fs::write(&config_path, new_content)
        .map_err(|e| format!("Failed to write config.toml: {}", e))?;
    Ok(())
}

#[tauri::command]
pub async fn enable_local_proxy_mode(app: tauri::AppHandle) -> Result<serde_json::Value, String> {
    log::info!("[switcher] === START enable_local_proxy_mode ===");

    // enable proxy in config.toml
    set_config_toml_proxy(&app, true)?;
    log::info!("[switcher] openai_base_url set to proxy in config.toml");

    // stop codex
    log::info!("[switcher] stopping Codex processes...");
    stop_codex_processes()?;

    // delete auth.json so Codex resets to Login Screen
    let auth_path = get_auth_path(&app)?;
    if auth_path.exists() {
        let backup_dir = auth_path
            .parent()
            .unwrap_or(std::path::Path::new(""))
            .join("backups");
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let backup_path = backup_dir.join(format!("auth_cleared_{}.json", ts));

        let _ = fs::create_dir_all(&backup_dir);
        let _ = fs::rename(&auth_path, &backup_path);
        log::info!(
            "[switcher] renamed old auth.json to {:?} (proxy mode login reset)",
            backup_path
        );
    }
    // write a fresh proxy auth profile ? Or completely blank?
    // Actually, simply deleting it is usually enough for the official Codex to show login.
    let blank_payload = serde_json::json!({
        "auth_mode": "api_key",
        "OPENAI_API_KEY": null,
        "tokens": null
    });
    atomic_write_json(&auth_path, &blank_payload)
        .map_err(|e| format!("Failed to write blank auth.json: {}", e))?;
    log::info!("[switcher] wrote blank auth.json properly");

    // launch codex
    launch_codex()?;
    log::info!("[switcher] Codex launched.");
    log::info!("[switcher] === END enable_local_proxy_mode ===");

    Ok(serde_json::json!({
        "success": true,
        "message": "Local proxy mode enabled and Codex restarted.",
    }))
}

#[tauri::command]
pub async fn local_codex_switch(
    app: tauri::AppHandle,
    account_id: String,
) -> Result<serde_json::Value, String> {
    log::info!(
        "[switcher] === START local_codex_switch for account: {} ===",
        &account_id[..30.min(account_id.len())]
    );

    // Ensure we are fully disconnected from local proxy mode and restored to OpenAI
    let _ = set_config_toml_proxy(&app, false);

    let db_path = resolve_db_path_with_legacy_migration(&app)?;
    log::info!("[switcher] db_path: {:?}", db_path);

    let aid = account_id.clone();
    let db_path_for_read = db_path.clone();
    let db_tokens_res = tauri::async_runtime::spawn_blocking(move || {
        let storage = Storage::open(db_path_for_read).map_err(|e| e.to_string())?;
        storage
            .find_token_by_account_id(&aid)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|err| format!("task failed: {err}"))?;

    let token = db_tokens_res?.ok_or_else(|| "Account not found in local DB".to_string())?;
    log::info!(
        "[switcher] found token in DB, access_token len={}, id_token len={}, refresh_token len={}",
        token.access_token.len(),
        token.id_token.len(),
        token.refresh_token.len()
    );

    // Check expiry of BOTH access_token and id_token
    let at_expired = is_token_expired("access_token", &token.access_token);
    let it_expired = is_token_expired("id_token", &token.id_token);
    let needs_refresh = at_expired || it_expired;
    log::info!(
        "[switcher] access_token expired={}, id_token expired={}, needs_refresh={}",
        at_expired,
        it_expired,
        needs_refresh
    );

    let (final_at, final_it, final_rt, refreshed) = if needs_refresh {
        if token.refresh_token.trim().is_empty() {
            return Err("Account tokens are expired and refresh_token is missing.".to_string());
        }
        log::info!("[switcher] token is EXPIRED, attempting refresh...");
        match refresh_tokens(&token.refresh_token).await {
            Ok((at, it, rt)) => {
                log::info!("[switcher] refresh OK!");
                (at, it, rt, true)
            }
            Err(e) => {
                log::error!("[switcher] refresh FAILED: {}", e);
                return Err(format!("Token refresh failed before switch: {}", e));
            }
        }
    } else {
        log::info!("[switcher] token still VALID, no refresh needed");
        (
            token.access_token.clone(),
            token.id_token.clone(),
            token.refresh_token.clone(),
            false,
        )
    };

    if refreshed {
        let db_path_for_write = db_path.clone();
        let refreshed_token = Token {
            account_id: token.account_id.clone(),
            id_token: final_it.clone(),
            access_token: final_at.clone(),
            refresh_token: final_rt.clone(),
            api_key_access_token: token.api_key_access_token.clone(),
            last_refresh: now_unix_ts(),
        };
        tauri::async_runtime::spawn_blocking(move || {
            let storage = Storage::open(db_path_for_write).map_err(|e| e.to_string())?;
            storage
                .insert_token(&refreshed_token)
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|err| format!("token update task failed: {err}"))??;
        log::info!("[switcher] refreshed tokens persisted to local DB");
    }

    log::info!("[switcher] stopping Codex processes...");
    stop_codex_processes()?;

    let auth_path = get_auth_path(&app)?;
    log::info!("[switcher] auth_path: {:?}", auth_path);

    if auth_path.exists() {
        let backup_dir = auth_path
            .parent()
            .unwrap_or(std::path::Path::new(""))
            .join("backups");
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let backup_path = backup_dir.join(format!("auth_{}.json", ts));

        let _ = std::fs::create_dir_all(&backup_dir);
        if let Err(e) = std::fs::copy(&auth_path, &backup_path) {
            log::warn!("[switcher] failed to backup auth.json: {}", e);
        } else {
            log::info!("[switcher] backed up old auth.json to {:?}", backup_path);
        }
    }

    // Attempt to extract the true UUID account_id from the final id_token or access_token
    let true_account_id = decode_jwt_chatgpt_account_id(&final_it)
        .or_else(|| decode_jwt_chatgpt_account_id(&final_at))
        .unwrap_or_else(|| {
            log::warn!(
                "[switcher] Could not extract chatgpt_account_id from tokens! Fallback to DB id."
            );
            token.account_id.clone()
        });

    log::info!(
        "[switcher] true_account_id for auth.json = {}",
        true_account_id
    );

    let payload = serde_json::json!({
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": null,
        "tokens": {
            "id_token": final_it,
            "access_token": final_at,
            "refresh_token": final_rt,
            "account_id": true_account_id,
        },
        "last_refresh": now_rfc3339()
    });

    atomic_write_json(&auth_path, &payload)
        .map_err(|e| format!("Failed to write auth.json: {}", e))?;
    log::info!("[switcher] auth.json written successfully");

    launch_codex()?;
    log::info!("[switcher] Codex launched. refreshed={}", refreshed);
    log::info!("[switcher] === END local_codex_switch ===");

    Ok(serde_json::json!({
        "success": true,
        "message": "Codex account switched and launched.",
        "refreshed": refreshed,
    }))
}
