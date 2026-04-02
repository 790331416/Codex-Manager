"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { useMutation } from "@tanstack/react-query";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  BarChart3,
  Download,
  Clock3,
  PencilLine,
  ExternalLink,
  FileUp,
  FolderOpen,
  MoreVertical,
  Pin,
  Plus,
  Power,
  PowerOff,
  RefreshCw,
  Search,
  Trash2,
  type LucideIcon,
} from "lucide-react";
import { toast } from "sonner";
import { AddAccountModal } from "@/components/modals/add-account-modal";
import { ConfirmDialog } from "@/components/modals/confirm-dialog";
import UsageModal from "@/components/modals/usage-modal";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useAccounts } from "@/hooks/useAccounts";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { usePageTransitionReady } from "@/hooks/usePageTransitionReady";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { pluginClient } from "@/lib/api/plugin-client";
import { cn } from "@/lib/utils";
import { buildStaticRouteUrl } from "@/lib/utils/static-routes";
import {
  formatTsFromSeconds,
  getUsageDisplayBuckets,
  isBannedAccount,
  isPrimaryWindowOnlyUsage,
  isSecondaryWindowOnlyUsage,
} from "@/lib/utils/usage";
import { Account, PluginCatalogEntry } from "@/types";

type StatusFilter = "all" | "available" | "low_quota" | "banned";
type AccountExportMode = "single" | "multiple";
const UNAVAILABLE_FREE_CLEANUP_PLUGIN_ID = "cleanup-unavailable-free-accounts";
const UNAVAILABLE_FREE_CLEANUP_TASK_ID = `${UNAVAILABLE_FREE_CLEANUP_PLUGIN_ID}::run`;
const UNAVAILABLE_FREE_CLEANUP_DEFAULT_INTERVAL_SECONDS = 60;
const ACCOUNT_SORT_STEP = 5;

const UNAVAILABLE_FREE_CLEANUP_PLUGIN: PluginCatalogEntry = {
  id: UNAVAILABLE_FREE_CLEANUP_PLUGIN_ID,
  name: "清理不可用免费账号",
  version: "1.0.0",
  description: "自动清理状态不可用且属于 free 的账号，适合做定时收尾整理。",
  author: "CodexManager",
  homepageUrl: null,
  scriptUrl: null,
  scriptBody: `
fn run(context) {
    log("开始清理不可用免费账号：" + context["plugin"]["name"]);
    let result = cleanup_unavailable_free_accounts();
    log("清理完成，删除 " + result["deleted"].to_string() + " 个账号");
    result
}
`,
  permissions: ["accounts:cleanup"],
  tasks: [
    {
      id: "run",
      name: "定时自动清理",
      description: "每 60 秒自动清理一次不可用免费账号",
      entrypoint: "run",
      scheduleKind: "interval",
      intervalSeconds: UNAVAILABLE_FREE_CLEANUP_DEFAULT_INTERVAL_SECONDS,
      enabled: true,
    },
  ],
  manifestVersion: "1",
  category: "official",
  runtimeKind: "rhai",
  tags: ["账号治理", "精选", "定时脚本"],
  sourceUrl: "builtin://codexmanager",
};

/**
 * 函数 `formatAccountPlanValueLabel`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - value: 参数 value
 *
 * # 返回
 * 返回函数执行结果
 */
function formatAccountPlanValueLabel(value: string) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  switch (normalized) {
    case "free":
      return "FREE";
    case "go":
      return "GO";
    case "plus":
      return "PLUS";
    case "pro":
      return "PRO";
    case "team":
      return "TEAM";
    case "business":
      return "BUSINESS";
    case "enterprise":
      return "ENTERPRISE";
    case "edu":
      return "EDU";
    case "unknown":
      return "未知";
    default:
      return normalized ? normalized.toUpperCase() : "未知";
  }
}

/**
 * 函数 `normalizeAccountPlanKey`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - account: 参数 account
 *
 * # 返回
 * 返回函数执行结果
 */
function normalizeAccountPlanKey(account: Account) {
  return String(account.planType || "")
    .trim()
    .toLowerCase() || "unknown";
}

/**
 * 函数 `formatPlanFilterLabel`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - value: 参数 value
 *
 * # 返回
 * 返回函数执行结果
 */
function formatPlanFilterLabel(value: string) {
  const nextValue = String(value || "").trim();
  if (!nextValue || nextValue === "all") {
    return "全部类型";
  }
  return formatAccountPlanValueLabel(nextValue);
}

/**
 * 函数 `formatStatusFilterLabel`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - value: 参数 value
 *
 * # 返回
 * 返回函数执行结果
 */
function formatStatusFilterLabel(value: string) {
  const nextValue = String(value || "").trim();
  switch (nextValue) {
    case "available":
      return "可用";
    case "low_quota":
      return "低配额";
    case "banned":
      return "封禁";
    case "all":
    default:
      return "全部";
  }
}

interface QuotaProgressProps {
  label: string;
  remainPercent: number | null;
  resetsAt: number | null;
  icon: LucideIcon;
  tone: "green" | "blue";
  emptyText?: string;
  emptyResetText?: string;
}

/**
 * 函数 `QuotaProgress`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - params: 参数 params
 *
 * # 返回
 * 返回函数执行结果
 */
function QuotaProgress({
  label,
  remainPercent,
  resetsAt,
  icon: Icon,
  tone,
  emptyText = "--",
  emptyResetText = "未知",
}: QuotaProgressProps) {
  const value = remainPercent ?? 0;
  const trackClassName = tone === "blue" ? "bg-blue-500/20" : "bg-green-500/20";
  const indicatorClassName = tone === "blue" ? "bg-blue-500" : "bg-green-500";

  return (
    <div className="flex min-w-[120px] flex-col gap-1">
      <div className="flex items-center justify-between text-[10px]">
        <div className="flex items-center gap-1 text-muted-foreground">
          <Icon className="h-3 w-3" />
          <span>{label}</span>
        </div>
        <span className="font-medium">
          {remainPercent == null ? emptyText : `${value}%`}
        </span>
      </div>
      <Progress
        value={value}
        trackClassName={trackClassName}
        indicatorClassName={indicatorClassName}
      />
      <div className="text-[10px] text-muted-foreground">
        重置: {formatTsFromSeconds(resetsAt, emptyResetText)}
      </div>
    </div>
  );
}

/**
 * 函数 `getAccountStatusAction`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - account: 参数 account
 *
 * # 返回
 * 返回函数执行结果
 */
function getAccountStatusAction(account: Account): {
  action: "enable" | "disable" | null;
  label: string;
  icon: LucideIcon;
} {
  const normalizedStatus = String(account.status || "")
    .trim()
    .toLowerCase();
  if (normalizedStatus === "disabled") {
    return { action: "enable", label: "启用账号", icon: Power };
  }
  if (normalizedStatus === "inactive") {
    return { action: "enable", label: "恢复账号", icon: Power };
  }
  if (normalizedStatus === "banned") {
    return { action: null, label: "封禁账号", icon: PowerOff };
  }
  return { action: "disable", label: "禁用账号", icon: PowerOff };
}

/**
 * 函数 `formatAccountPlanLabel`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - account: 参数 account
 *
 * # 返回
 * 返回函数执行结果
 */
function formatAccountPlanLabel(account: Account): string | null {
  const normalized = normalizeAccountPlanKey(account);
  return normalized === "unknown"
    ? null
    : formatAccountPlanValueLabel(normalized);
}

/**
 * 函数 `getAccountPlanBadgeClassName`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - planLabel: 参数 planLabel
 *
 * # 返回
 * 返回函数执行结果
 */
function getAccountPlanBadgeClassName(planLabel: string | null): string {
  switch (planLabel) {
    case "FREE":
      return "bg-slate-500/10 text-slate-700 dark:text-slate-300";
    case "GO":
      return "bg-sky-500/10 text-sky-700 dark:text-sky-300";
    case "PLUS":
      return "bg-amber-500/10 text-amber-700 dark:text-amber-300";
    case "PRO":
      return "bg-fuchsia-500/10 text-fuchsia-700 dark:text-fuchsia-300";
    case "TEAM":
      return "bg-emerald-500/10 text-emerald-700 dark:text-emerald-300";
    case "BUSINESS":
      return "bg-indigo-500/10 text-indigo-700 dark:text-indigo-300";
    case "ENTERPRISE":
      return "bg-rose-500/10 text-rose-700 dark:text-rose-300";
    case "EDU":
      return "bg-cyan-500/10 text-cyan-700 dark:text-cyan-300";
    default:
      return "bg-accent/50";
  }
}

/**
 * 函数 `formatAccountTags`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - tags: 参数 tags
 *
 * # 返回
 * 返回函数执行结果
 */
function formatAccountTags(tags: string[]): string {
  return tags
    .map((tag) => String(tag || "").trim())
    .filter(Boolean)
    .join("、");
}

/**
 * 函数 `normalizeTagsDraft`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - tagsDraft: 参数 tagsDraft
 *
 * # 返回
 * 返回函数执行结果
 */
function normalizeTagsDraft(tagsDraft: string): string[] {
  return tagsDraft
    .split(",")
    .map((tag) => tag.trim())
    .filter(Boolean);
}

/**
 * 函数 `buildAccountOrderUpdates`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - orderedAccounts: 参数 orderedAccounts
 *
 * # 返回
 * 返回函数执行结果
 */
function buildAccountOrderUpdates(orderedAccounts: Account[]) {
  return orderedAccounts.reduce<Array<{ accountId: string; sort: number }>>(
    (updates, account, index) => {
      const nextSort = index * ACCOUNT_SORT_STEP;
      const currentSort = Number.isFinite(account.priority)
        ? account.priority
        : Number(account.sort) || 0;
      if (currentSort !== nextSort) {
        updates.push({ accountId: account.id, sort: nextSort });
      }
      return updates;
    },
    [],
  );
}

type AccountSizeSortMode = "large-first" | "small-first";

/**
 * 函数 `getAccountSizeGroup`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - account: 参数 account
 *
 * # 返回
 * 返回函数执行结果
 */
function getAccountSizeGroup(account: Account): "large" | "standard" | "small" {
  switch (normalizeAccountPlanKey(account)) {
    case "plus":
    case "pro":
    case "team":
    case "business":
    case "enterprise":
      return "large";
    case "free":
      return "small";
    default:
      return "standard";
  }
}

/**
 * 函数 `buildAccountsBySizeOrder`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - orderedAccounts: 参数 orderedAccounts
 * - mode: 参数 mode
 *
 * # 返回
 * 返回函数执行结果
 */
function buildAccountsBySizeOrder(
  orderedAccounts: Account[],
  mode: AccountSizeSortMode,
) {
  const buckets = {
    large: [] as Account[],
    standard: [] as Account[],
    small: [] as Account[],
  };

  for (const account of orderedAccounts) {
    buckets[getAccountSizeGroup(account)].push(account);
  }

  return mode === "large-first"
    ? [...buckets.large, ...buckets.standard, ...buckets.small]
    : [...buckets.small, ...buckets.standard, ...buckets.large];
}

function formatAccountExportModeLabel(value: string) {
  return value === "single" ? "单 JSON" : "多 JSON";
}

/**
 * 函数 `AccountInfoCell`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - params: 参数 params
 *
 * # 返回
 * 返回函数执行结果
 */
function AccountInfoCell({
  account,
  isPreferred,
}: {
  account: Account;
  isPreferred: boolean;
}) {
  const accountPlanLabel = formatAccountPlanLabel(account);
  const tagsText = formatAccountTags(account.tags);
  const noteText = String(account.note || "").trim();

  return (
    <Tooltip>
      <TooltipTrigger render={<div />} className="block cursor-help text-left">
        <div className="flex flex-col overflow-hidden">
          <div className="flex items-center gap-2 overflow-hidden">
            <span className="truncate text-sm font-semibold">{account.name}</span>
            {accountPlanLabel ? (
              <Badge
                variant="secondary"
                className={cn(
                  "h-4 shrink-0 px-1.5 text-[9px]",
                  getAccountPlanBadgeClassName(accountPlanLabel),
                )}
              >
                {accountPlanLabel}
              </Badge>
            ) : null}
            {isPreferred ? (
              <Badge
                variant="secondary"
                className="h-4 shrink-0 bg-amber-500/15 px-1.5 text-[9px] text-amber-700 dark:text-amber-300"
              >
                优先
              </Badge>
            ) : null}
          </div>
          <span className="truncate font-mono text-[10px] uppercase text-muted-foreground opacity-60">
            {account.id.slice(0, 16)}...
          </span>
          <span className="mt-1 text-[10px] text-muted-foreground">
            最近刷新: {formatTsFromSeconds(account.lastRefreshAt, "从未刷新")}
          </span>
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-sm">
        <div className="flex min-w-[260px] flex-col gap-2">
          <div className="grid gap-2 sm:grid-cols-2">
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">账号类型</div>
              <div className="font-medium">{accountPlanLabel || "未知"}</div>
            </div>
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">当前状态</div>
              <div className="font-medium">{account.availabilityText || "未知"}</div>
            </div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">标签</div>
            <div className="break-words">{tagsText || "未设置"}</div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">备注</div>
            <div className="whitespace-pre-wrap break-words">
              {noteText || "未设置"}
            </div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">账号 ID</div>
            <div className="break-all font-mono text-[11px]">{account.id}</div>
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

export default function AccountsPage() {
  const router = useRouter();
  const { isDesktopRuntime, canUseBrowserDownloadExport } = useRuntimeCapabilities();
  const {
    accounts,
    planTypes,
    isLoading,
    isServiceReady,
    refreshAccount,
    refreshAllAccounts,
    refreshAccountList,
    deleteAccount,
    deleteManyAccounts,
    importByFile,
    importByDirectory,
    exportAccounts,
    isRefreshingAccountId,
    isRefreshingAllAccounts,
    isExporting,
    isDeletingMany,
    manualPreferredAccountId,
    setPreferredAccount,
    clearPreferredAccount,
    isUpdatingPreferred,
    reorderAccounts,
    isReorderingAccounts,
    updateAccountProfile,
    isUpdatingProfileAccountId,
    toggleAccountStatus,
    isUpdatingStatusAccountId,
  } = useAccounts();
  const isPageActive = useDesktopPageActive("/accounts/");
  usePageTransitionReady("/accounts/", !isServiceReady || !isLoading);

  const [search, setSearch] = useState("");
  const [planFilter, setPlanFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [pageSize, setPageSize] = useState("20");
  const [page, setPage] = useState(1);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [addAccountModalOpen, setAddAccountModalOpen] = useState(false);
  const [usageModalOpen, setUsageModalOpen] = useState(false);
  const [cleanupScheduleOpen, setCleanupScheduleOpen] = useState(false);
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const [exportModeDraft, setExportModeDraft] =
    useState<AccountExportMode>("multiple");
  const [cleanupScheduleDraft, setCleanupScheduleDraft] = useState(
    String(UNAVAILABLE_FREE_CLEANUP_DEFAULT_INTERVAL_SECONDS),
  );
  const [selectedAccountId, setSelectedAccountId] = useState("");
  const [labelDraft, setLabelDraft] = useState("");
  const [tagsDraft, setTagsDraft] = useState("");
  const [noteDraft, setNoteDraft] = useState("");
  const [sortDraft, setSortDraft] = useState("");
  const [accountEditorState, setAccountEditorState] = useState<{
    accountId: string;
    accountName: string;
    currentLabel: string;
    currentTags: string;
    currentNote: string;
    currentSort: number;
  } | null>(null);
  const [deleteDialogState, setDeleteDialogState] = useState<
    | { kind: "single"; account: Account }
    | { kind: "selected"; ids: string[]; count: number }
    | null
  >(null);
  const importFileActionLabel = isDesktopRuntime ? "按文件导入" : "选择文件导入";
  const importDirectoryActionLabel = isDesktopRuntime
    ? "按文件夹导入"
    : "选择目录导入";
  const exportActionLabel =
    !isDesktopRuntime && canUseBrowserDownloadExport ? "导出到浏览器" : "导出账号";
  const exportActionShortcut = isExporting
    ? "..."
    : !isDesktopRuntime && canUseBrowserDownloadExport
      ? "DL"
      : "ZIP";

  const scheduleCleanupMutation = useMutation({
    mutationFn: async (intervalSeconds: number) => {
      const installedPlugins = await pluginClient.listInstalled();
      const installed = installedPlugins.find(
        (item) => item.pluginId === UNAVAILABLE_FREE_CLEANUP_PLUGIN_ID,
      );

      if (!installed) {
        await pluginClient.install(UNAVAILABLE_FREE_CLEANUP_PLUGIN);
      }

      await pluginClient.enable(UNAVAILABLE_FREE_CLEANUP_PLUGIN_ID);
      await pluginClient.updateTask(UNAVAILABLE_FREE_CLEANUP_TASK_ID, intervalSeconds);
    },
    onSuccess: () => {
      toast.success("定时脚本已启用");
      setCleanupScheduleOpen(false);
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "启用定时脚本失败");
    },
  });

  const filteredAccounts = useMemo(() => {
    return accounts.filter((account) => {
      const matchSearch =
        !search ||
        account.name.toLowerCase().includes(search.toLowerCase()) ||
        account.id.toLowerCase().includes(search.toLowerCase());
      const matchPlan =
        planFilter === "all" || normalizeAccountPlanKey(account) === planFilter;
      const matchStatus =
        statusFilter === "all" ||
        (statusFilter === "available" && account.isAvailable) ||
        (statusFilter === "low_quota" && account.isLowQuota) ||
        (statusFilter === "banned" && isBannedAccount(account));
      return matchSearch && matchPlan && matchStatus;
    });
  }, [accounts, planFilter, search, statusFilter]);

  const statusFilterOptions = useMemo(
    () => [
      { id: "all" as const, label: `全部 (${accounts.length})` },
      {
        id: "available" as const,
        label: `可用 (${accounts.filter((account) => account.isAvailable).length})`,
      },
      {
        id: "low_quota" as const,
        label: `低配额 (${accounts.filter((account) => account.isLowQuota).length})`,
      },
      {
        id: "banned" as const,
        label: `封禁 (${accounts.filter((account) => isBannedAccount(account)).length})`,
      },
    ],
    [accounts],
  );
  const pageSizeNumber = Number(pageSize) || 20;
  const totalPages = Math.max(
    1,
    Math.ceil(filteredAccounts.length / pageSizeNumber),
  );
  const safePage = Math.min(page, totalPages);
  const accountIdSet = useMemo(
    () => new Set(accounts.map((account) => account.id)),
    [accounts],
  );
  const effectiveSelectedIds = useMemo(
    () => selectedIds.filter((id) => accountIdSet.has(id)),
    [accountIdSet, selectedIds],
  );
  const exportSelectionCount = effectiveSelectedIds.length;
  const exportTargetCount =
    exportSelectionCount > 0 ? exportSelectionCount : accounts.length;
  const exportScopeText =
    exportSelectionCount > 0
      ? `当前已选择 ${exportSelectionCount} 个账号，本次将只导出选中的账号。`
      : `当前未选择账号，本次将导出全部 ${accounts.length} 个账号。`;

  const visibleAccounts = useMemo(() => {
    /**
     * 函数 `offset`
     *
     * 作者: gaohongshun
     *
     * 时间: 2026-04-02
     *
     * # 参数
     * - safePage - 1: 参数 safePage - 1
     *
     * # 返回
     * 返回函数执行结果
     */
    const offset = (safePage - 1) * pageSizeNumber;
    return filteredAccounts.slice(offset, offset + pageSizeNumber);
  }, [filteredAccounts, pageSizeNumber, safePage]);
  const filteredAccountIndexMap = useMemo(
    () => new Map(filteredAccounts.map((account, index) => [account.id, index])),
    [filteredAccounts],
  );

  const selectedAccount = useMemo(
    () => accounts.find((account) => account.id === selectedAccountId) ?? null,
    [accounts, selectedAccountId],
  );
  const currentEditingAccount = useMemo(
    () =>
      accountEditorState
        ? accounts.find((account) => account.id === accountEditorState.accountId) ?? null
        : null,
    [accountEditorState, accounts],
  );

  /**
   * 函数 `handleSearchChange`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - value: 参数 value
   *
   * # 返回
   * 返回函数执行结果
   */
  const handleSearchChange = (value: string) => {
    setSearch(value);
    setPage(1);
  };

  /**
   * 函数 `handlePlanFilterChange`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - value: 参数 value
   *
   * # 返回
   * 返回函数执行结果
   */
  const handlePlanFilterChange = (value: string | null) => {
    setPlanFilter(value || "all");
    setPage(1);
  };

  /**
   * 函数 `handleStatusFilterChange`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - value: 参数 value
   *
   * # 返回
   * 返回函数执行结果
   */
  const handleStatusFilterChange = (value: StatusFilter) => {
    setStatusFilter(value);
    setPage(1);
  };

  /**
   * 函数 `handlePageSizeChange`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - value: 参数 value
   *
   * # 返回
   * 返回函数执行结果
   */
  const handlePageSizeChange = (value: string | null) => {
    setPageSize(value || "20");
    setPage(1);
  };

  /**
   * 函数 `toggleSelect`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - id: 参数 id
   *
   * # 返回
   * 返回函数执行结果
   */
  const toggleSelect = (id: string) => {
    setSelectedIds((current) =>
      current.includes(id)
        ? current.filter((item) => item !== id)
        : [...current, id],
    );
  };

  /**
   * 函数 `toggleSelectAllVisible`
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
  const toggleSelectAllVisible = () => {
    const visibleIds = visibleAccounts.map((account) => account.id);
    const allSelected = visibleIds.every((id) =>
      effectiveSelectedIds.includes(id),
    );
    setSelectedIds((current) => {
      if (allSelected) {
        return current.filter((id) => !visibleIds.includes(id));
      }
      return Array.from(new Set([...current, ...visibleIds]));
    });
  };

  /**
   * 函数 `openUsage`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - account: 参数 account
   *
   * # 返回
   * 返回函数执行结果
   */
  const openUsage = (account: Account) => {
    setSelectedAccountId(account.id);
    setUsageModalOpen(true);
  };

  /**
   * 函数 `handleDeleteSelected`
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
  const handleDeleteSelected = () => {
    if (!effectiveSelectedIds.length) {
      toast.error("请先选择要删除的账号");
      return;
    }
    setDeleteDialogState({
      kind: "selected",
      ids: [...effectiveSelectedIds],
      count: effectiveSelectedIds.length,
    });
  };

  /**
   * 函数 `handleDeleteBanned`
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
  const handleDeleteBanned = () => {
    const bannedIds = accounts
      .filter((account) => isBannedAccount(account))
      .map((account) => account.id);
    if (!bannedIds.length) {
      toast.error("当前没有可清理的封禁账号");
      return;
    }
    setDeleteDialogState({
      kind: "selected",
      ids: bannedIds,
      count: bannedIds.length,
    });
  };

  /**
   * 函数 `openExportDialog`
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
  const openExportDialog = () => {
    if (!isServiceReady) {
      toast.info("服务未连接，暂时无法导出账号");
      return;
    }
    if (!accounts.length) {
      toast.info("当前没有可导出的账号");
      return;
    }
    setExportModeDraft("multiple");
    setExportDialogOpen(true);
  };

  /**
   * 函数 `handleConfirmExport`
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
  const handleConfirmExport = async () => {
    if (exportTargetCount <= 0) {
      toast.info("当前没有可导出的账号");
      return;
    }
    try {
      await exportAccounts({
        selectedAccountIds:
          exportSelectionCount > 0 ? effectiveSelectedIds : [],
        exportMode: exportModeDraft,
      });
      setExportDialogOpen(false);
    } catch {
      // 中文注释：错误提示已在 hook 内统一处理，这里只阻止弹窗误关闭。
    }
  };

  /**
   * 函数 `handleConfirmCleanupSchedule`
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
  const handleConfirmCleanupSchedule = () => {
    const parsed = Number(cleanupScheduleDraft.trim());
    if (!Number.isFinite(parsed) || parsed <= 0) {
      toast.error("请输入有效的定时间隔");
      return;
    }
    void scheduleCleanupMutation.mutateAsync(Math.max(1, Math.trunc(parsed)));
  };

  /**
   * 函数 `handleDeleteSingle`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - account: 参数 account
   *
   * # 返回
   * 返回函数执行结果
   */
  const handleDeleteSingle = (account: Account) => {
    setDeleteDialogState({ kind: "single", account });
  };

  /**
   * 函数 `openAccountEditor`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - account: 参数 account
   *
   * # 返回
   * 返回函数执行结果
   */
  const openAccountEditor = (account: Account) => {
    setAccountEditorState({
      accountId: account.id,
      accountName: account.name,
      currentLabel: account.label,
      currentTags: account.tags.join(", "),
      currentNote: account.note || "",
      currentSort: account.priority,
    });
    setLabelDraft(account.label);
    setTagsDraft(account.tags.join(", "));
    setNoteDraft(account.note || "");
    setSortDraft(String(account.priority));
  };

  /**
   * 函数 `handleMoveAccount`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - account: 参数 account
   * - direction: 参数 direction
   *
   * # 返回
   * 返回函数执行结果
   */
  const handleMoveAccount = async (
    account: Account,
    direction: "up" | "down",
  ) => {
    const filteredIndex = filteredAccountIndexMap.get(account.id);
    if (filteredIndex == null) {
      toast.error("未找到当前账号，请刷新后重试");
      return;
    }

    const targetFilteredIndex =
      direction === "up" ? filteredIndex - 1 : filteredIndex + 1;
    if (targetFilteredIndex < 0) {
      toast.info("当前账号已经在最前面");
      return;
    }
    if (targetFilteredIndex >= filteredAccounts.length) {
      toast.info("当前账号已经在最后面");
      return;
    }

    const targetAccount = filteredAccounts[targetFilteredIndex];
    const reorderedAccounts = accounts.filter((item) => item.id !== account.id);
    const anchorIndex = reorderedAccounts.findIndex(
      (item) => item.id === targetAccount.id,
    );
    if (anchorIndex === -1) {
      toast.error("未找到目标账号，请刷新后重试");
      return;
    }

    reorderedAccounts.splice(direction === "up" ? anchorIndex : anchorIndex + 1, 0, account);
    const updates = buildAccountOrderUpdates(reorderedAccounts);
    if (!updates.length) {
      toast.info("账号顺序未变化");
      return;
    }

    try {
      await reorderAccounts(updates);
    } catch {
      // hook 内统一处理 toast，这里保持静默即可
    }
  };

  /**
   * 函数 `handleApplyAccountSizeSort`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - mode: 参数 mode
   *
   * # 返回
   * 返回函数执行结果
   */
  const handleApplyAccountSizeSort = async (mode: AccountSizeSortMode) => {
    if (accounts.length < 2) {
      toast.info("账号数量不足，无需重新排序");
      return;
    }

    const reorderedAccounts = buildAccountsBySizeOrder(accounts, mode);
    const updates = buildAccountOrderUpdates(reorderedAccounts);
    if (!updates.length) {
      toast.info(
        mode === "large-first"
          ? "当前已经是大号优先顺序"
          : "当前已经是小号优先顺序",
      );
      return;
    }

    try {
      await reorderAccounts(updates);
    } catch {
      // hook 已统一处理 toast，这里保持静默即可
    }
  };

  /**
   * 函数 `handleConfirmAccountEditor`
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
  const handleConfirmAccountEditor = async () => {
    if (!accountEditorState) return;

    const nextLabel = labelDraft.trim();
    const nextTags = normalizeTagsDraft(tagsDraft);
    const nextTagsText = nextTags.join(", ");
    const nextNote = noteDraft.trim();

    if (!nextLabel) {
      toast.error("请输入账号名称");
      return;
    }

    const rawSort = sortDraft.trim();
    if (!rawSort) {
      toast.error("请输入顺序值");
      return;
    }

    const parsed = Number(rawSort);
    if (!Number.isFinite(parsed)) {
      toast.error("顺序必须是数字");
      return;
    }

    const nextSort = Math.max(0, Math.trunc(parsed));
    if (
      nextLabel === accountEditorState.currentLabel &&
      nextTagsText === accountEditorState.currentTags &&
      nextNote === accountEditorState.currentNote &&
      nextSort === accountEditorState.currentSort
    ) {
      setAccountEditorState(null);
      return;
    }

    try {
      await updateAccountProfile(accountEditorState.accountId, {
        label: nextLabel,
        note: nextNote || null,
        tags: nextTags,
        sort: nextSort,
      });
      setAccountEditorState(null);
    } catch {
      // mutation 已统一处理 toast，这里保持弹窗不关闭
    }
  };

  /**
   * 函数 `handleConfirmDelete`
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
  const handleConfirmDelete = () => {
    if (!deleteDialogState) return;
    if (deleteDialogState.kind === "single") {
      deleteAccount(deleteDialogState.account.id);
      return;
    }
    deleteManyAccounts(deleteDialogState.ids);
    setSelectedIds((current) =>
      current.filter((id) => !deleteDialogState.ids.includes(id)),
    );
  };

  return (
    <div className="space-y-6">
      {!isServiceReady ? (
        <Card className="glass-card border-none shadow-sm">
          <CardContent className="pt-6 text-sm text-muted-foreground">
            服务未连接，账号列表与相关操作暂不可用；连接恢复后会自动继续加载。
          </CardContent>
        </Card>
      ) : null}
      <Card className="glass-card border-none shadow-md backdrop-blur-md">
        <CardContent className="grid gap-3 pt-0 lg:grid-cols-[200px_auto_minmax(0,1fr)_auto] lg:items-center">
          <div className="min-w-0">
            <Input
              placeholder="搜索账号名 / 编号..."
              className="glass-card h-10 rounded-xl px-3"
              value={search}
              onChange={(event) => handleSearchChange(event.target.value)}
            />
          </div>

          <div className="flex shrink-0 items-center gap-3">
            <Select value={planFilter} onValueChange={handlePlanFilterChange}>
              <SelectTrigger className="h-10 w-[140px] shrink-0 rounded-xl bg-card/50">
                <SelectValue placeholder="全部类型">
                  {(value) => formatPlanFilterLabel(String(value || ""))}
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">
                  全部类型 ({accounts.length})
                </SelectItem>
                {planTypes.map((planType) => (
                  <SelectItem key={planType.value} value={planType.value}>
                    {formatAccountPlanValueLabel(planType.value)} ({planType.count})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select
              value={statusFilter}
              onValueChange={(value) =>
                handleStatusFilterChange(value as StatusFilter)
              }
            >
              <SelectTrigger className="h-10 w-[152px] shrink-0 rounded-xl bg-card/50">
                <SelectValue placeholder="全部状态">
                  {(value) => formatStatusFilterLabel(String(value || ""))}
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                {statusFilterOptions.map((filter) => (
                  <SelectItem key={filter.id} value={filter.id}>
                    {filter.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="hidden min-w-0 lg:block" />

          <div className="ml-auto flex shrink-0 items-center gap-2 lg:ml-0 lg:justify-self-end">
            <DropdownMenu>
              <DropdownMenuTrigger>
                <Button
                  variant="outline"
                  className="glass-card h-10 min-w-[50px] justify-between gap-2 rounded-xl px-3"
                  render={<span />}
                  nativeButton={false}
                >
                  <span className="flex items-center gap-2">
                    <span className="text-sm font-medium">账号操作</span>
                    {effectiveSelectedIds.length > 0 ? (
                      <span className="rounded-full bg-primary/10 px-2 py-0.5 text-[10px] font-semibold text-primary">
                        {effectiveSelectedIds.length}
                      </span>
                    ) : null}
                  </span>
                  <MoreVertical className="h-4 w-4 text-muted-foreground" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent
                align="end"
                className="w-64 rounded-xl border border-border/70 bg-popover/95 p-2 shadow-xl backdrop-blur-md"
              >
                <DropdownMenuGroup>
                  <DropdownMenuLabel className="px-2 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground/80">
                    刷新
                  </DropdownMenuLabel>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady || isRefreshingAllAccounts}
                    onClick={() => refreshAllAccounts()}
                  >
                    <RefreshCw
                      className={cn("mr-2 h-4 w-4", isRefreshingAllAccounts && "animate-spin")}
                    />
                    刷新账号用量
                    <DropdownMenuShortcut>ALL</DropdownMenuShortcut>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={() => refreshAccountList()}
                  >
                    <RefreshCw className="mr-2 h-4 w-4" />
                    刷新列表
                    <DropdownMenuShortcut>LIST</DropdownMenuShortcut>
                  </DropdownMenuItem>
                </DropdownMenuGroup>
                <DropdownMenuSeparator />
                <DropdownMenuGroup>
                  <DropdownMenuLabel className="px-2 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground/80">
                    账号管理
                  </DropdownMenuLabel>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={() => setAddAccountModalOpen(true)}
                  >
                    <Plus className="mr-2 h-4 w-4" /> 添加账号
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={() => importByFile()}
                  >
                    <FileUp className="mr-2 h-4 w-4" /> {importFileActionLabel}
                    <DropdownMenuShortcut>FILE</DropdownMenuShortcut>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={() => importByDirectory()}
                  >
                    <FolderOpen className="mr-2 h-4 w-4" /> {importDirectoryActionLabel}
                    <DropdownMenuShortcut>DIR</DropdownMenuShortcut>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady || isExporting || accounts.length === 0}
                    onClick={openExportDialog}
                  >
                    <Download className="mr-2 h-4 w-4" />
                    {exportActionLabel}
                    <DropdownMenuShortcut>
                      {exportActionShortcut}
                    </DropdownMenuShortcut>
                  </DropdownMenuItem>
                </DropdownMenuGroup>
                <DropdownMenuSeparator />
                <DropdownMenuGroup>
                  <DropdownMenuLabel className="px-2 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground/80">
                    排序
                  </DropdownMenuLabel>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={
                      !isServiceReady ||
                      isReorderingAccounts ||
                      accounts.length < 2
                    }
                    onClick={() => void handleApplyAccountSizeSort("large-first")}
                  >
                    <ArrowUpDown className="mr-2 h-4 w-4" />
                    大号优先排序
                    <DropdownMenuShortcut>BIZ</DropdownMenuShortcut>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={
                      !isServiceReady ||
                      isReorderingAccounts ||
                      accounts.length < 2
                    }
                    onClick={() => void handleApplyAccountSizeSort("small-first")}
                  >
                    <ArrowDown className="mr-2 h-4 w-4" />
                    小号优先排序
                    <DropdownMenuShortcut>FREE</DropdownMenuShortcut>
                  </DropdownMenuItem>
                </DropdownMenuGroup>
                <DropdownMenuSeparator />
                <DropdownMenuGroup>
                  <DropdownMenuLabel className="px-2 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground/80">
                    清理
                  </DropdownMenuLabel>
                  <DropdownMenuItem
                    disabled={!isServiceReady || !effectiveSelectedIds.length || isDeletingMany}
                    variant="destructive"
                    className="h-9 rounded-lg px-2"
                    onClick={handleDeleteSelected}
                  >
                    <Trash2 className="mr-2 h-4 w-4" /> 删除选中账号
                    <DropdownMenuShortcut>
                      {effectiveSelectedIds.length || "-"}
                    </DropdownMenuShortcut>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={() => {
                      setCleanupScheduleDraft(
                        String(UNAVAILABLE_FREE_CLEANUP_DEFAULT_INTERVAL_SECONDS),
                      );
                      setCleanupScheduleOpen(true);
                    }}
                  >
                    <Clock3 className="mr-2 h-4 w-4" /> 定时脚本
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    variant="destructive"
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={handleDeleteBanned}
                  >
                    <Trash2 className="mr-2 h-4 w-4" /> 一键清理封禁账号
                  </DropdownMenuItem>
                </DropdownMenuGroup>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </CardContent>
      </Card>

      <Dialog open={cleanupScheduleOpen} onOpenChange={setCleanupScheduleOpen}>
        <DialogContent className="glass-card border-border/70 sm:max-w-md">
          <DialogHeader>
            <DialogTitle>定时脚本</DialogTitle>
            <DialogDescription>
              将内置脚本安装为定时任务，默认每分钟执行一次。启用后可在插件中心继续调整任务间隔。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div className="space-y-2">
              <Label htmlFor="cleanup-schedule-interval">运行间隔（秒）</Label>
              <Input
                id="cleanup-schedule-interval"
                type="number"
                min={60}
                step={60}
                value={cleanupScheduleDraft}
                onChange={(event) => setCleanupScheduleDraft(event.target.value)}
                className="glass-card h-10 rounded-xl"
              />
              <div className="text-xs text-muted-foreground">
                例如 `60` 表示每分钟运行一次。
              </div>
            </div>
          </div>
          <DialogFooter>
            <DialogClose className={cn(buttonVariants({ variant: "outline" }), "rounded-xl")}>
              取消
            </DialogClose>
            <Button
              className="rounded-xl"
              onClick={handleConfirmCleanupSchedule}
              disabled={scheduleCleanupMutation.isPending}
            >
              {scheduleCleanupMutation.isPending ? "保存中..." : "安装并启用"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={exportDialogOpen} onOpenChange={setExportDialogOpen}>
        <DialogContent className="glass-card border-border/70 sm:max-w-md">
          <DialogHeader>
            <DialogTitle>导出账号</DialogTitle>
            <DialogDescription>
              导出范围会自动按当前选择决定；如果没有选中账号，就导出全部。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-2">
            <div className="rounded-xl bg-muted/20 px-3 py-3 text-sm text-foreground/80">
              {exportScopeText}
            </div>
            <div className="space-y-2">
              <Label htmlFor="account-export-mode">导出格式</Label>
              <Select
                value={exportModeDraft}
                onValueChange={(value) =>
                  setExportModeDraft(
                    value === "single" ? "single" : "multiple",
                  )
                }
              >
                <SelectTrigger
                  id="account-export-mode"
                  className="glass-card h-10 rounded-xl"
                >
                  <SelectValue>
                    {(value) => formatAccountExportModeLabel(String(value || ""))}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="multiple">多 JSON</SelectItem>
                  <SelectItem value="single">单 JSON</SelectItem>
                </SelectContent>
              </Select>
              <div className="text-xs text-muted-foreground">
                {exportModeDraft === "single"
                  ? "导出为一个 `accounts.json` 数组文件，适合整体备份和再次导入。"
                  : "每个账号导出为一个独立 JSON 文件，适合逐个分发或单独管理。"}
              </div>
            </div>
          </div>
          <DialogFooter>
            <DialogClose
              className={cn(buttonVariants({ variant: "outline" }), "rounded-xl")}
              disabled={isExporting}
            >
              取消
            </DialogClose>
            <Button
              className="rounded-xl"
              onClick={() => void handleConfirmExport()}
              disabled={isExporting || exportTargetCount <= 0}
            >
              {isExporting ? "导出中..." : "开始导出"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Card className="glass-card overflow-hidden border-none py-0 shadow-xl backdrop-blur-md">
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-12 text-center">
                  <Checkbox
                    checked={
                      visibleAccounts.length > 0 &&
                      visibleAccounts.every((account) =>
                        effectiveSelectedIds.includes(account.id),
                      )
                    }
                    onCheckedChange={toggleSelectAllVisible}
                  />
                </TableHead>
                <TableHead className="max-w-[220px]">账号信息</TableHead>
                <TableHead>5h 额度</TableHead>
                <TableHead>7d 额度</TableHead>
                <TableHead className="w-[156px]">顺序</TableHead>
                <TableHead>状态</TableHead>
                <TableHead className="text-center">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                Array.from({ length: 5 }).map((_, index) => (
                  <TableRow key={index}>
                    <TableCell>
                      <Skeleton className="mx-auto h-4 w-4" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-32" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-24" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-24" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-10" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-6 w-16 rounded-full" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="mx-auto h-8 w-24" />
                    </TableCell>
                  </TableRow>
                ))
              ) : visibleAccounts.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-48 text-center">
                    <div className="flex flex-col items-center justify-center gap-2 text-muted-foreground">
                      <Search className="h-8 w-8 opacity-20" />
                      <p>未找到符合条件的账号</p>
                    </div>
                  </TableCell>
                </TableRow>
                ) : (
                  visibleAccounts.map((account) => {
                    const primaryWindowOnly = isPrimaryWindowOnlyUsage(
                      account.usage,
                    );
                    const secondaryWindowOnly = isSecondaryWindowOnlyUsage(
                      account.usage,
                    );
                    const usageBuckets = getUsageDisplayBuckets(account.usage);
                    const statusAction = getAccountStatusAction(account);
                    const StatusActionIcon = statusAction.icon;
                    const filteredIndex =
                      filteredAccountIndexMap.get(account.id) ?? -1;
                    const canMoveUp = filteredIndex > 0;
                    const canMoveDown =
                      filteredIndex !== -1 &&
                      filteredIndex < filteredAccounts.length - 1;
                    return (
                      <TableRow key={account.id} className="group">
                        <TableCell className="text-center">
                          <Checkbox
                            checked={effectiveSelectedIds.includes(account.id)}
                            onCheckedChange={() => toggleSelect(account.id)}
                          />
                        </TableCell>
                        <TableCell className="max-w-[220px]">
                          <AccountInfoCell
                            account={account}
                            isPreferred={manualPreferredAccountId === account.id}
                          />
                        </TableCell>
                        <TableCell>
                          <QuotaProgress
                            label="5小时"
                            remainPercent={account.primaryRemainPercent}
                            resetsAt={usageBuckets.primaryResetsAt}
                            icon={RefreshCw}
                            tone="green"
                            emptyText={secondaryWindowOnly ? "未提供" : "--"}
                            emptyResetText={
                              secondaryWindowOnly ? "未提供" : "未知"
                            }
                          />
                        </TableCell>
                        <TableCell>
                          <QuotaProgress
                          label="7天"
                          remainPercent={account.secondaryRemainPercent}
                          resetsAt={usageBuckets.secondaryResetsAt}
                          icon={RefreshCw}
                          tone="blue"
                          emptyText={primaryWindowOnly ? "未提供" : "--"}
                          emptyResetText={primaryWindowOnly ? "未提供" : "未知"}
                        />
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <span className="rounded bg-muted/50 px-2 py-0.5 font-mono text-xs">
                            {account.priority}
                          </span>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7 text-muted-foreground transition-colors hover:text-primary"
                            disabled={
                              !isServiceReady ||
                              !canMoveUp ||
                              isReorderingAccounts ||
                              isUpdatingProfileAccountId === account.id
                            }
                            onClick={() => void handleMoveAccount(account, "up")}
                            title="上移一位"
                          >
                            <ArrowUp className="h-3.5 w-3.5" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7 text-muted-foreground transition-colors hover:text-primary"
                            disabled={
                              !isServiceReady ||
                              !canMoveDown ||
                              isReorderingAccounts ||
                              isUpdatingProfileAccountId === account.id
                            }
                            onClick={() => void handleMoveAccount(account, "down")}
                            title="下移一位"
                          >
                            <ArrowDown className="h-3.5 w-3.5" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7 text-muted-foreground transition-colors hover:text-primary"
                            disabled={
                              !isServiceReady ||
                              isReorderingAccounts ||
                              isUpdatingProfileAccountId === account.id
                            }
                            onClick={() => openAccountEditor(account)}
                            title="编辑账号信息"
                          >
                            <PencilLine className="h-3.5 w-3.5" />
                          </Button>
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1.5">
                          <div
                            className={cn(
                              "h-1.5 w-1.5 rounded-full",
                              account.isAvailable
                                ? "bg-green-500"
                                : "bg-red-500",
                            )}
                          />
                          <span
                            className={cn(
                              "text-[11px] font-medium",
                              account.isAvailable
                                ? "text-green-600 dark:text-green-400"
                                : "text-red-600 dark:text-red-400",
                            )}
                          >
                            {account.availabilityText}
                          </span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="table-action-cell gap-1">
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 text-muted-foreground transition-colors hover:text-primary"
                            disabled={!isServiceReady}
                            onClick={() => openUsage(account)}
                            title="用量详情"
                          >
                            <BarChart3 className="h-4 w-4" />
                          </Button>
                          <DropdownMenu>
                            <DropdownMenuTrigger>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8"
                                render={<span />}
                                nativeButton={false}
                                disabled={!isServiceReady}
                              >
                                <MoreVertical className="h-4 w-4" />
                              </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                              <DropdownMenuItem
                                className="gap-2"
                                disabled={!isServiceReady || isUpdatingPreferred}
                                onClick={() =>
                                  manualPreferredAccountId === account.id
                                    ? clearPreferredAccount()
                                    : setPreferredAccount(account.id)
                                }
                              >
                                <Pin className="h-4 w-4" />
                                {manualPreferredAccountId === account.id
                                  ? "取消优先"
                                  : "设为优先"}
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                className="gap-2"
                                disabled={
                                  !isServiceReady ||
                                  isUpdatingStatusAccountId === account.id ||
                                  statusAction.action === null
                                }
                                onClick={() =>
                                  statusAction.action &&
                                  toggleAccountStatus(
                                    account.id,
                                    statusAction.action === "enable",
                                    account.status,
                                  )
                                }
                              >
                                <StatusActionIcon className="h-4 w-4" />
                                {statusAction.label}
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                className="gap-2"
                                onClick={() =>
                                  router.push(
                                    buildStaticRouteUrl(
                                      "/logs",
                                      `?query=${encodeURIComponent(account.id)}`,
                                    ),
                                  )
                                }
                              >
                                <ExternalLink className="h-4 w-4" /> 详情与日志
                              </DropdownMenuItem>
                              <DropdownMenuSeparator />
                              <DropdownMenuItem
                                className="gap-2 text-red-500"
                                disabled={!isServiceReady}
                                onClick={() => handleDeleteSingle(account)}
                              >
                                <Trash2 className="h-4 w-4" /> 删除
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <div className="flex items-center justify-between px-2">
        <div className="text-xs text-muted-foreground">
          共 {filteredAccounts.length} 个账号
          {effectiveSelectedIds.length > 0 ? (
            <span className="ml-1 text-primary">
              (已选择 {effectiveSelectedIds.length} 个)
            </span>
          ) : null}
        </div>
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2">
            <span className="whitespace-nowrap text-xs text-muted-foreground">
              每页显示
            </span>
            <Select value={pageSize} onValueChange={handlePageSizeChange}>
              <SelectTrigger className="h-8 w-[70px] text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {["5", "10", "20", "50", "100", "500"].map((value) => (
                  <SelectItem key={value} value={value}>
                    {value}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              className="h-8 px-3 text-xs"
              disabled={safePage <= 1}
              onClick={() => setPage((current) => Math.max(1, current - 1))}
            >
              上一页
            </Button>
            <div className="min-w-[60px] text-center text-xs font-medium">
              第 {safePage} / {totalPages} 页
            </div>
            <Button
              variant="outline"
              size="sm"
              className="h-8 px-3 text-xs"
              disabled={safePage >= totalPages}
              onClick={() =>
                setPage((current) => Math.min(totalPages, current + 1))
              }
            >
              下一页
            </Button>
          </div>
        </div>
      </div>

      {addAccountModalOpen ? (
        <AddAccountModal
          open={isPageActive && addAccountModalOpen}
          onOpenChange={setAddAccountModalOpen}
        />
      ) : null}
      <UsageModal
        account={selectedAccount}
        open={isPageActive && usageModalOpen}
        onOpenChange={(open) => {
          setUsageModalOpen(open);
          if (!open) {
            setSelectedAccountId("");
          }
        }}
        onRefresh={refreshAccount}
        isRefreshing={
          isRefreshingAllAccounts ||
          (!!selectedAccount && isRefreshingAccountId === selectedAccount.id)
        }
      />
      <ConfirmDialog
        open={isPageActive && Boolean(deleteDialogState)}
        onOpenChange={(open) => {
          if (!open) {
            setDeleteDialogState(null);
          }
        }}
        title={
          deleteDialogState?.kind === "single" ? "删除账号" : "批量删除账号"
        }
        description={
          deleteDialogState?.kind === "single"
            ? `确定删除账号 ${deleteDialogState.account.name} 吗？删除后不可恢复。`
            : `确定删除选中的 ${deleteDialogState?.count || 0} 个账号吗？删除后不可恢复。`
        }
        confirmText="删除"
        confirmVariant="destructive"
        onConfirm={handleConfirmDelete}
      />
      <Dialog
        open={isPageActive && Boolean(accountEditorState)}
        onOpenChange={(open) => {
          if (!open && !isUpdatingProfileAccountId) {
            setAccountEditorState(null);
          }
        }}
      >
        <DialogContent className="glass-card border-none sm:max-w-[560px]">
          <DialogHeader>
            <DialogTitle>编辑账号信息</DialogTitle>
            <DialogDescription>
              {accountEditorState
                ? `修改 ${accountEditorState.accountName} 的名称、标签、备注与排序。`
                : "修改账号的基础资料。"}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-2">
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="grid gap-2">
                <Label htmlFor="account-label-input">账号名称</Label>
                <Input
                  id="account-label-input"
                  value={labelDraft}
                  disabled={Boolean(isUpdatingProfileAccountId)}
                  onChange={(event) => setLabelDraft(event.target.value)}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="account-tags-input">标签（逗号分隔）</Label>
                <Input
                  id="account-tags-input"
                  value={tagsDraft}
                  disabled={Boolean(isUpdatingProfileAccountId)}
                  onChange={(event) => setTagsDraft(event.target.value)}
                  placeholder="例如：高频, 团队A"
                />
              </div>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="account-note-input">备注</Label>
              <Textarea
                id="account-note-input"
                value={noteDraft}
                disabled={Boolean(isUpdatingProfileAccountId)}
                onChange={(event) => setNoteDraft(event.target.value)}
                placeholder="例如：主账号 / 测试号 / 团队共享"
                className="min-h-[108px]"
              />
            </div>
            <div className="grid gap-2 sm:grid-cols-[minmax(0,1fr)_120px] sm:items-end">
              <div className="grid gap-2">
                <Label htmlFor="account-sort-input">顺序值</Label>
                <Input
                  id="account-sort-input"
                  type="number"
                  min={0}
                  step={1}
                  value={sortDraft}
                  disabled={Boolean(isUpdatingProfileAccountId)}
                  onChange={(event) => setSortDraft(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      void handleConfirmAccountEditor();
                    }
                  }}
                />
              </div>
              <div className="grid gap-1 rounded-xl bg-muted/30 px-3 py-2 text-[11px] text-muted-foreground">
                <span>值越小越靠前</span>
                <span>仅修改当前账号</span>
              </div>
            </div>
            <div className="grid gap-3 rounded-xl bg-muted/20 px-3 py-3 text-[11px] text-muted-foreground sm:grid-cols-2">
              <div className="space-y-1">
                <div>账号 ID</div>
                <div className="break-all font-mono">
                  {accountEditorState?.accountId || "-"}
                </div>
              </div>
              <div className="space-y-1">
                <div>账号类型</div>
                <div className="font-medium text-foreground/80">
                  {currentEditingAccount
                    ? formatAccountPlanLabel(currentEditingAccount) || "未知"
                    : "未知"}
                </div>
              </div>
            </div>
          </div>
          <DialogFooter className="gap-2 sm:gap-2">
            <DialogClose
              className={buttonVariants({ variant: "outline" })}
              type="button"
              disabled={Boolean(isUpdatingProfileAccountId)}
            >
              取消
            </DialogClose>
            <Button
              disabled={Boolean(isUpdatingProfileAccountId)}
              onClick={() => void handleConfirmAccountEditor()}
            >
              保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
