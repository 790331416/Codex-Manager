"use client";

import { StartupSnapshot } from "@/types";

export const STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT = 120;
export const STARTUP_SNAPSHOT_STALE_TIME = 15_000;
export const STARTUP_SNAPSHOT_WARMUP_INTERVAL_MS = 2_500;
export const STARTUP_SNAPSHOT_WARMUP_TIMEOUT_MS = 45_000;

/**
 * 函数 `buildStartupSnapshotQueryKey`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - addr: 参数 addr
 * - requestLogLimit: 参数 requestLogLimit
 *
 * # 返回
 * 返回函数执行结果
 */
export function buildStartupSnapshotQueryKey(
  addr: string | null | undefined,
  requestLogLimit = STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
  dayStartTs?: number | null,
) {
  return ["startup-snapshot", addr || null, requestLogLimit, dayStartTs || null] as const;
}

/**
 * 函数 `hasStartupSnapshotSignal`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - snapshot: 参数 snapshot
 *
 * # 返回
 * 返回函数执行结果
 */
export function hasStartupSnapshotSignal(
  snapshot: StartupSnapshot | undefined
): boolean {
  if (!snapshot) return false;
  if (snapshot.usageSnapshots.length > 0) return true;
  if (snapshot.requestLogs.length > 0) return true;
  if (snapshot.requestLogTodaySummary.todayTokens > 0) return true;
  return (
    snapshot.usageAggregateSummary.primaryKnownCount > 0 ||
    snapshot.usageAggregateSummary.secondaryKnownCount > 0
  );
}
