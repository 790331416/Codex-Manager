"use client";

import { useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { useDeferredDesktopActivation } from "@/hooks/useDeferredDesktopActivation";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { useLocalDayRange } from "@/hooks/useLocalDayRange";
import {
  buildStartupSnapshotQueryKey,
  hasStartupSnapshotSignal,
  STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
  STARTUP_SNAPSHOT_STALE_TIME,
  STARTUP_SNAPSHOT_WARMUP_INTERVAL_MS,
  STARTUP_SNAPSHOT_WARMUP_TIMEOUT_MS,
} from "@/lib/api/startup-snapshot";
import { serviceClient } from "@/lib/api/service-client";
import { useAppStore } from "@/lib/store/useAppStore";
import { pickBestRecommendations, pickCurrentAccount } from "@/lib/utils/usage";

/**
 * 函数 `useDashboardStats`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * 无
 *
 * # 返回
 * 返回函数执行结果
 */
export function useDashboardStats() {
  const serviceStatus = useAppStore((state) => state.serviceStatus);
  const localDayRange = useLocalDayRange();
  const isServiceReady = serviceStatus.connected;
  const isPageActive = useDesktopPageActive("/");
  const isSnapshotQueryEnabled = useDeferredDesktopActivation(
    isServiceReady && isPageActive,
  );
  const warmupStartedAtRef = useRef<number | null>(null);

  useEffect(() => {
    if (!isServiceReady || !isPageActive) {
      warmupStartedAtRef.current = null;
      return;
    }
    warmupStartedAtRef.current = Date.now();
  }, [isPageActive, isServiceReady, serviceStatus.addr]);

  const snapshotQuery = useQuery({
    queryKey: buildStartupSnapshotQueryKey(
      serviceStatus.addr,
      STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
      localDayRange.dayStartTs,
    ),
    queryFn: () =>
      serviceClient.getStartupSnapshot({
        requestLogLimit: STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
        dayStartTs: localDayRange.dayStartTs,
        dayEndTs: localDayRange.dayEndTs,
      }),
    enabled: isSnapshotQueryEnabled,
    retry: 1,
    staleTime: STARTUP_SNAPSHOT_STALE_TIME,
    refetchInterval: (query) => {
      if (!isServiceReady || !isPageActive) return false;
      if (typeof document !== "undefined" && document.visibilityState !== "visible") {
        return false;
      }
      const startedAt = warmupStartedAtRef.current;
      if (startedAt != null) {
        if (Date.now() - startedAt >= STARTUP_SNAPSHOT_WARMUP_TIMEOUT_MS) {
          warmupStartedAtRef.current = null;
        } else {
          const snapshot = query.state.data;
          if (
            snapshot &&
            snapshot.accounts.length > 0 &&
            !hasStartupSnapshotSignal(snapshot)
          ) {
            return STARTUP_SNAPSHOT_WARMUP_INTERVAL_MS;
          }
        }
      }

      return STARTUP_SNAPSHOT_STALE_TIME;
    },
    refetchIntervalInBackground: false,
  });

  const data = snapshotQuery.data;
  const accounts = data?.accounts || [];
  const hasStartupSignal = hasStartupSnapshotSignal(data);
  const shouldWarmupPoll =
    isPageActive &&
    isServiceReady &&
    accounts.length > 0 &&
    !hasStartupSignal &&
    snapshotQuery.isFetching;
  const hasSnapshotData = Boolean(data);
  const totalAccounts = accounts.length;
  const availableAccounts = accounts.filter((item) => item.isAvailable).length;
  const unavailableAccounts = totalAccounts - availableAccounts;
  const currentAccount = pickCurrentAccount(
    accounts,
    data?.requestLogs || []
  );
  const recommendations = pickBestRecommendations(accounts);

  return {
    stats: {
      total: totalAccounts,
      available: availableAccounts,
      unavailable: unavailableAccounts,
      todayTokens: data?.requestLogTodaySummary.todayTokens || 0,
      cachedTokens: data?.requestLogTodaySummary.cachedInputTokens || 0,
      reasoningTokens: data?.requestLogTodaySummary.reasoningOutputTokens || 0,
      todayCost: data?.requestLogTodaySummary.estimatedCost || 0,
      poolRemain: {
        primary: data?.usageAggregateSummary.primaryRemainPercent ?? null,
        secondary: data?.usageAggregateSummary.secondaryRemainPercent ?? null,
        primaryKnownCount: data?.usageAggregateSummary.primaryKnownCount ?? 0,
        primaryBucketCount: data?.usageAggregateSummary.primaryBucketCount ?? 0,
        secondaryKnownCount: data?.usageAggregateSummary.secondaryKnownCount ?? 0,
        secondaryBucketCount: data?.usageAggregateSummary.secondaryBucketCount ?? 0,
      },
    },
    currentAccount,
    recommendations,
    requestLogs: data?.requestLogs || [],
    isLoading:
      (!isServiceReady && !hasSnapshotData) ||
      (!isSnapshotQueryEnabled && !data) ||
      snapshotQuery.isPending ||
      shouldWarmupPoll,
    isSyncingSnapshot: shouldWarmupPoll,
    isServiceReady,
    isError: snapshotQuery.isError,
    error: snapshotQuery.error,
  };
}
