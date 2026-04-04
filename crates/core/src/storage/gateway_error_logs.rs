use rusqlite::params;
use rusqlite::Result;
use rusqlite::Row;

use super::{GatewayErrorLog, Storage};

impl Storage {
    /// 函数 `ensure_gateway_error_logs_table`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-04
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(super) fn ensure_gateway_error_logs_table(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS gateway_error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trace_id TEXT,
                key_id TEXT,
                account_id TEXT,
                request_path TEXT NOT NULL,
                method TEXT NOT NULL,
                stage TEXT NOT NULL,
                error_kind TEXT,
                upstream_url TEXT,
                cf_ray TEXT,
                status_code INTEGER,
                compression_enabled INTEGER NOT NULL DEFAULT 0,
                compression_retry_attempted INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gateway_error_logs_created_at
             ON gateway_error_logs(created_at DESC, id DESC)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gateway_error_logs_trace_id
             ON gateway_error_logs(trace_id, created_at DESC)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gateway_error_logs_stage
             ON gateway_error_logs(stage, created_at DESC)",
            [],
        )?;
        Ok(())
    }

    /// 函数 `insert_gateway_error_log`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-04
    ///
    /// # 参数
    /// - self: 参数 self
    /// - log: 参数 log
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn insert_gateway_error_log(&self, log: &GatewayErrorLog) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO gateway_error_logs (
                trace_id, key_id, account_id, request_path, method, stage,
                error_kind, upstream_url, cf_ray, status_code,
                compression_enabled, compression_retry_attempted, message, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                &log.trace_id,
                &log.key_id,
                &log.account_id,
                &log.request_path,
                &log.method,
                &log.stage,
                &log.error_kind,
                &log.upstream_url,
                &log.cf_ray,
                log.status_code,
                if log.compression_enabled { 1 } else { 0 },
                if log.compression_retry_attempted { 1 } else { 0 },
                &log.message,
                log.created_at,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// 函数 `list_gateway_error_logs`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-04
    ///
    /// # 参数
    /// - self: 参数 self
    /// - limit: 参数 limit
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn list_gateway_error_logs(&self, limit: i64) -> Result<Vec<GatewayErrorLog>> {
        let normalized_limit = limit.clamp(1, 500);
        let mut stmt = self.conn.prepare(
            "SELECT trace_id, key_id, account_id, request_path, method, stage,
                    error_kind, upstream_url, cf_ray, status_code,
                    compression_enabled, compression_retry_attempted, message, created_at
             FROM gateway_error_logs
             ORDER BY created_at DESC, id DESC
             LIMIT ?1",
        )?;
        let mut rows = stmt.query([normalized_limit])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push(map_gateway_error_log_row(row)?);
        }
        Ok(items)
    }

    /// 函数 `clear_gateway_error_logs`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-04
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn clear_gateway_error_logs(&self) -> Result<()> {
        self.conn.execute("DELETE FROM gateway_error_logs", [])?;
        Ok(())
    }
}

fn map_gateway_error_log_row(row: &Row<'_>) -> Result<GatewayErrorLog> {
    Ok(GatewayErrorLog {
        trace_id: row.get(0)?,
        key_id: row.get(1)?,
        account_id: row.get(2)?,
        request_path: row.get(3)?,
        method: row.get(4)?,
        stage: row.get(5)?,
        error_kind: row.get(6)?,
        upstream_url: row.get(7)?,
        cf_ray: row.get(8)?,
        status_code: row.get(9)?,
        compression_enabled: row.get::<_, i64>(10)? != 0,
        compression_retry_attempted: row.get::<_, i64>(11)? != 0,
        message: row.get(12)?,
        created_at: row.get(13)?,
    })
}
