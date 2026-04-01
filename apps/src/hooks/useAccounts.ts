"use client";

import { useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { accountClient } from "@/lib/api/account-client";
import { attachUsagesToAccounts } from "@/lib/api/normalize";
import { serviceClient } from "@/lib/api/service-client";
import {
  buildStartupSnapshotQueryKey,
  STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
} from "@/lib/api/startup-snapshot";
import { getAppErrorMessage } from "@/lib/api/transport";
import { useDeferredDesktopActivation } from "@/hooks/useDeferredDesktopActivation";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { useAppStore } from "@/lib/store/useAppStore";
import { AccountListResult, StartupSnapshot } from "@/types";

type ImportByDirectoryResult = Awaited<ReturnType<typeof accountClient.importByDirectory>>;
type ImportByFileResult = Awaited<ReturnType<typeof accountClient.importByFile>>;
type ExportResult = Awaited<ReturnType<typeof accountClient.export>>;
type DeleteUnavailableFreeResult = { deleted?: number };
type AccountSortUpdate = { accountId: string; sort: number };

function isAccountRefreshBlocked(status: string | null | undefined): boolean {
  return String(status || "").trim().toLowerCase() === "disabled";
}

function buildImportSummaryMessage(result: ImportByDirectoryResult): string {
  const total = Number(result?.total || 0);
  const created = Number(result?.created || 0);
  const updated = Number(result?.updated || 0);
  const failed = Number(result?.failed || 0);
  return `导入完成：共${total}，新增${created}，更新${updated}，失败${failed}`;
}

function formatUsageRefreshErrorMessage(error: unknown): string {
  const message = getAppErrorMessage(error);
  if (message.toLowerCase().includes("refresh token failed with status 401")) {
    return "账号长期未登录，refresh 已过期，已改为不可用状态";
  }
  return message;
}

export function useAccounts() {
  const queryClient = useQueryClient();
  const serviceStatus = useAppStore((state) => state.serviceStatus);
  const { canAccessManagementRpc } = useRuntimeCapabilities();
  const isServiceReady = canAccessManagementRpc && serviceStatus.connected;
  const areAccountQueriesEnabled = useDeferredDesktopActivation(isServiceReady);
  const startupSnapshot = queryClient.getQueryData<StartupSnapshot>(
    buildStartupSnapshotQueryKey(
      serviceStatus.addr,
      STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT
    )
  );
  const startupAccounts = startupSnapshot?.accounts || [];
  const startupUsages = startupSnapshot?.usageSnapshots || [];
  const startupManualPreferredAccountId = startupSnapshot?.manualPreferredAccountId || "";
  const hasStartupAccountSnapshot = startupAccounts.length > 0;

  const ensureServiceReady = (actionLabel: string): boolean => {
    if (isServiceReady) {
      return true;
    }
    toast.info(`服务未连接，暂时无法${actionLabel}`);
    return false;
  };

  const accountsQuery = useQuery({
    queryKey: ["accounts", "list"],
    queryFn: () => accountClient.list(),
    enabled: areAccountQueriesEnabled,
    retry: 1,
    placeholderData: (previousData): AccountListResult | undefined =>
      previousData ||
      (startupAccounts.length > 0
        ? {
            items: startupAccounts,
            total: startupAccounts.length,
            page: 1,
            pageSize: startupAccounts.length,
          }
        : undefined),
  });

  const usagesQuery = useQuery({
    queryKey: ["usage", "list"],
    queryFn: () => accountClient.listUsage(),
    enabled: areAccountQueriesEnabled,
    retry: 1,
    placeholderData: (previousData) =>
      previousData || (startupUsages.length > 0 ? startupUsages : undefined),
  });

  const manualPreferredAccountQuery = useQuery({
    queryKey: ["gateway", "manual-account", serviceStatus.addr || null],
    queryFn: () => serviceClient.getManualPreferredAccountId(),
    enabled: areAccountQueriesEnabled,
    retry: 1,
    placeholderData: (previousData) =>
      previousData ?? startupManualPreferredAccountId,
  });

  const accounts = useMemo(() => {
    return attachUsagesToAccounts(
      accountsQuery.data?.items || [],
      usagesQuery.data || []
    );
  }, [accountsQuery.data?.items, usagesQuery.data]);

  const planTypes = useMemo(() => {
    const map = new Map<string, number>();
    const sortOrder = [
      "free",
      "go",
      "plus",
      "pro",
      "team",
      "business",
      "enterprise",
      "edu",
      "unknown",
    ];
    const getSortIndex = (value: string) => {
      const index = sortOrder.indexOf(value);
      return index === -1 ? sortOrder.length : index;
    };

    for (const account of accounts) {
      const planType = String(account.planType || "").trim().toLowerCase() || "unknown";
      map.set(planType, (map.get(planType) || 0) + 1);
    }

    return Array.from(map.entries())
      .sort((left, right) => {
        const sortDiff = getSortIndex(left[0]) - getSortIndex(right[0]);
        if (sortDiff !== 0) {
          return sortDiff;
        }
        return left[0].localeCompare(right[0], "zh-Hans-CN");
      })
      .map(([value, count]) => ({ value, count }));
  }, [accounts]);

  const invalidateAll = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["accounts"] }),
      queryClient.invalidateQueries({ queryKey: ["usage"] }),
      queryClient.invalidateQueries({ queryKey: ["usage-aggregate"] }),
      queryClient.invalidateQueries({ queryKey: ["today-summary"] }),
      queryClient.invalidateQueries({ queryKey: ["startup-snapshot"] }),
      queryClient.invalidateQueries({ queryKey: ["gateway", "manual-account"] }),
      queryClient.invalidateQueries({ queryKey: ["logs"] }),
    ]);
  };

  const invalidateManualPreferred = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["gateway", "manual-account"] }),
      queryClient.invalidateQueries({ queryKey: ["startup-snapshot"] }),
    ]);
  };

  const refreshAccountMutation = useMutation({
    mutationFn: (accountId: string) => accountClient.refreshUsage(accountId),
    onSuccess: () => {
      toast.success("账号用量已刷新");
    },
    onError: (error: unknown) => {
      toast.error(`刷新失败: ${formatUsageRefreshErrorMessage(error)}`);
    },
    onSettled: async () => {
      await invalidateAll();
    },
  });

  const refreshAllMutation = useMutation({
    mutationFn: () => accountClient.refreshUsage(),
    onSuccess: () => {
      toast.success("账号用量已刷新");
    },
    onError: (error: unknown) => {
      toast.error(`刷新失败: ${formatUsageRefreshErrorMessage(error)}`);
    },
    onSettled: async () => {
      await invalidateAll();
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (accountId: string) => accountClient.delete(accountId),
    onSuccess: async () => {
      await invalidateAll();
      toast.success("账号已删除");
    },
    onError: (error: unknown) => {
      toast.error(`删除失败: ${getAppErrorMessage(error)}`);
    },
  });

  const deleteManyMutation = useMutation({
    mutationFn: (accountIds: string[]) => accountClient.deleteMany(accountIds),
    onSuccess: async (_result, accountIds) => {
      await invalidateAll();
      toast.success(`已删除 ${accountIds.length} 个账号`);
    },
    onError: (error: unknown) => {
      toast.error(`批量删除失败: ${getAppErrorMessage(error)}`);
    },
  });

  const deleteUnavailableFreeMutation = useMutation({
    mutationFn: () => accountClient.deleteUnavailableFree(),
    onSuccess: async (result: DeleteUnavailableFreeResult) => {
      await invalidateAll();
      const deleted = Number(result?.deleted || 0);
      if (deleted > 0) {
        toast.success(`已移除 ${deleted} 个不可用免费账号`);
      } else {
        toast.success("未发现可清理的不可用免费账号");
      }
    },
    onError: (error: unknown) => {
      toast.error(`清理失败: ${getAppErrorMessage(error)}`);
    },
  });

  const updateAccountSortMutation = useMutation({
    mutationFn: ({ accountId, sort }: { accountId: string; sort: number }) =>
      accountClient.updateSort(accountId, sort),
    onSuccess: async () => {
      await invalidateAll();
      toast.success("账号顺序已更新");
    },
    onError: (error: unknown) => {
      toast.error(`更新顺序失败: ${getAppErrorMessage(error)}`);
    },
  });

  const reorderAccountsMutation = useMutation({
    mutationFn: async (updates: AccountSortUpdate[]) => {
      for (const update of updates) {
        await accountClient.updateSort(update.accountId, update.sort);
      }
      return updates.length;
    },
    onSuccess: async (count) => {
      await invalidateAll();
      toast.success(count > 1 ? `账号顺序已调整（${count} 项）` : "账号顺序已更新");
    },
    onError: async (error: unknown) => {
      await invalidateAll();
      toast.error(`调整账号顺序失败: ${getAppErrorMessage(error)}`);
    },
  });

  const updateAccountProfileMutation = useMutation({
    mutationFn: ({
      accountId,
      label,
      note,
      tags,
      sort,
    }: {
      accountId: string;
      label?: string | null;
      note?: string | null;
      tags?: string[] | string | null;
      sort?: number | null;
    }) =>
      accountClient.updateProfile(accountId, {
        label,
        note,
        tags,
        sort,
      }),
    onSuccess: async () => {
      await invalidateAll();
      toast.success("账号信息已更新");
    },
    onError: (error: unknown) => {
      toast.error(`更新账号信息失败: ${getAppErrorMessage(error)}`);
    },
  });

  const toggleAccountStatusMutation = useMutation({
    mutationFn: ({
      accountId,
      enabled,
    }: {
      accountId: string;
      enabled: boolean;
      sourceStatus?: string | null;
    }) =>
      enabled
        ? accountClient.enableAccount(accountId)
        : accountClient.disableAccount(accountId),
    onSuccess: async (_result, variables) => {
      await invalidateAll();
      const normalizedSourceStatus = String(variables.sourceStatus || "")
        .trim()
        .toLowerCase();
      toast.success(
        variables.enabled
          ? normalizedSourceStatus === "inactive"
            ? "账号已恢复"
            : "账号已启用"
          : "账号已禁用"
      );
    },
    onError: (error: unknown, variables) => {
      const normalizedSourceStatus = String(variables.sourceStatus || "")
        .trim()
        .toLowerCase();
      const actionLabel = variables.enabled
        ? normalizedSourceStatus === "inactive"
          ? "恢复"
          : "启用"
        : "禁用";
      toast.error(
        `${actionLabel}账号失败: ${getAppErrorMessage(error)}`
      );
    },
  });

  const importByDirectoryMutation = useMutation({
    mutationFn: () => accountClient.importByDirectory(),
    onSuccess: async (result: ImportByDirectoryResult) => {
      if (result?.canceled) {
        toast.info("已取消导入");
        return;
      }
      await invalidateAll();
      toast.success(buildImportSummaryMessage(result));
    },
    onError: (error: unknown) => {
      toast.error(`导入失败: ${getAppErrorMessage(error)}`);
    },
  });

  const importByFileMutation = useMutation({
    mutationFn: () => accountClient.importByFile(),
    onSuccess: async (result: ImportByFileResult) => {
      if (result?.canceled) {
        toast.info("已取消导入");
        return;
      }
      await invalidateAll();
      toast.success(buildImportSummaryMessage(result));
    },
    onError: (error: unknown) => {
      toast.error(`导入失败: ${getAppErrorMessage(error)}`);
    },
  });

  const exportMutation = useMutation({
    mutationFn: () => accountClient.export(),
    onSuccess: (result: ExportResult) => {
      if (result?.canceled) {
        toast.info("已取消导出");
        return;
      }
      const exported = Number(result?.exported || 0);
      const outputDir = String(result?.outputDir || "").trim();
      const isBrowserDownload = outputDir === "browser-download";
      toast.success(
        isBrowserDownload
          ? `已导出 ${exported} 个账号，浏览器将开始下载`
          : outputDir
          ? `已导出 ${exported} 个账号到 ${outputDir}`
          : `已导出 ${exported} 个账号`
      );
    },
    onError: (error: unknown) => {
      toast.error(`导出失败: ${getAppErrorMessage(error)}`);
    },
  });

  const setManualPreferredMutation = useMutation({
    mutationFn: (accountId: string) => serviceClient.setManualPreferredAccount(accountId),
    onSuccess: async () => {
      await invalidateManualPreferred();
      toast.success("已设为优先账号");
    },
    onError: (error: unknown) => {
      toast.error(`设置优先账号失败: ${getAppErrorMessage(error)}`);
    },
  });

  const clearManualPreferredMutation = useMutation({
    mutationFn: () => serviceClient.clearManualPreferredAccount(),
    onSuccess: async () => {
      await invalidateManualPreferred();
      toast.success("已取消优先账号");
    },
    onError: (error: unknown) => {
      toast.error(`取消优先账号失败: ${getAppErrorMessage(error)}`);
    },
  });

  return {
    accounts,
    planTypes,
    total: accountsQuery.data?.total || accounts.length,
    isLoading:
      isServiceReady &&
      !hasStartupAccountSnapshot &&
      (!areAccountQueriesEnabled || accountsQuery.isLoading || usagesQuery.isLoading),
    isServiceReady,
    manualPreferredAccountId: manualPreferredAccountQuery.data || "",
    refreshAccount: (accountId: string) => {
      if (!ensureServiceReady("刷新账号")) return;
      refreshAccountMutation.mutate(accountId);
    },
    refreshAllAccounts: () => {
      if (!ensureServiceReady("刷新账号")) return;
      if (!accounts.some((account) => !isAccountRefreshBlocked(account.status))) {
        toast.info("当前没有可刷新的账号");
        return;
      }
      refreshAllMutation.mutate();
    },
    refreshAccountList: async () => {
      if (!ensureServiceReady("刷新账号列表")) return;
      await invalidateAll();
      toast.success("账号列表已刷新");
    },
    deleteAccount: (accountId: string) => {
      if (!ensureServiceReady("删除账号")) return;
      deleteMutation.mutate(accountId);
    },
    deleteManyAccounts: (accountIds: string[]) => {
      if (!ensureServiceReady("批量删除账号")) return;
      deleteManyMutation.mutate(accountIds);
    },
    deleteUnavailableFree: () => {
      if (!ensureServiceReady("清理账号")) return;
      deleteUnavailableFreeMutation.mutate();
    },
    importByFile: () => {
      if (!ensureServiceReady("导入账号")) return;
      importByFileMutation.mutate();
    },
    importByDirectory: () => {
      if (!ensureServiceReady("导入账号")) return;
      importByDirectoryMutation.mutate();
    },
    exportAccounts: () => {
      if (!ensureServiceReady("导出账号")) return;
      exportMutation.mutate();
    },
    setPreferredAccount: (accountId: string) => {
      if (!ensureServiceReady("设置优先账号")) return;
      setManualPreferredMutation.mutate(accountId);
    },
    clearPreferredAccount: () => {
      if (!ensureServiceReady("取消优先账号")) return;
      clearManualPreferredMutation.mutate();
    },
    updateAccountSort: async (accountId: string, sort: number) => {
      if (!ensureServiceReady("更新账号顺序")) return;
      await updateAccountSortMutation.mutateAsync({ accountId, sort });
    },
    reorderAccounts: async (updates: AccountSortUpdate[]) => {
      if (!ensureServiceReady("调整账号顺序")) return;
      if (!updates.length) return;
      await reorderAccountsMutation.mutateAsync(updates);
    },
    updateAccountProfile: async (
      accountId: string,
      params: {
        label?: string | null;
        note?: string | null;
        tags?: string[] | string | null;
        sort?: number | null;
      }
    ) => {
      if (!ensureServiceReady("更新账号信息")) return;
      await updateAccountProfileMutation.mutateAsync({ accountId, ...params });
    },
    toggleAccountStatus: (
      accountId: string,
      enabled: boolean,
      sourceStatus?: string | null
    ) => {
      if (!ensureServiceReady(enabled ? "启用账号" : "禁用账号")) return;
      toggleAccountStatusMutation.mutate({ accountId, enabled, sourceStatus });
    },
    isRefreshingAccountId:
      refreshAccountMutation.isPending && typeof refreshAccountMutation.variables === "string"
        ? refreshAccountMutation.variables
        : "",
    isRefreshingAllAccounts: refreshAllMutation.isPending,
    isExporting: exportMutation.isPending,
    isDeletingMany: deleteManyMutation.isPending,
    isUpdatingPreferred:
      setManualPreferredMutation.isPending || clearManualPreferredMutation.isPending,
    isUpdatingSortAccountId:
      updateAccountSortMutation.isPending &&
      updateAccountSortMutation.variables &&
      typeof updateAccountSortMutation.variables === "object" &&
      "accountId" in updateAccountSortMutation.variables
        ? String(
            (updateAccountSortMutation.variables as { accountId?: unknown }).accountId || ""
          )
        : "",
    isReorderingAccounts: reorderAccountsMutation.isPending,
    isUpdatingProfileAccountId:
      updateAccountProfileMutation.isPending &&
      updateAccountProfileMutation.variables &&
      typeof updateAccountProfileMutation.variables === "object" &&
      "accountId" in updateAccountProfileMutation.variables
        ? String(
            (updateAccountProfileMutation.variables as { accountId?: unknown }).accountId || ""
          )
        : "",
    isUpdatingStatusAccountId:
      toggleAccountStatusMutation.isPending &&
      toggleAccountStatusMutation.variables &&
      typeof toggleAccountStatusMutation.variables === "object" &&
      "accountId" in toggleAccountStatusMutation.variables
        ? String(
            (toggleAccountStatusMutation.variables as { accountId?: unknown }).accountId || ""
          )
        : "",
  };
}
