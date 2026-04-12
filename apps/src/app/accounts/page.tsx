"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  BarChart3,
  Download,
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
  Zap,
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
import { useI18n } from "@/lib/i18n/provider";
import { cn } from "@/lib/utils";
import { buildStaticRouteUrl } from "@/lib/utils/static-routes";
import {
  formatTsFromSeconds,
  getExtraUsageDisplayRows,
  getUsageDisplayBuckets,
  isBannedAccount,
  isPrimaryWindowOnlyUsage,
  isSecondaryWindowOnlyUsage,
} from "@/lib/utils/usage";
import { Account } from "@/types";

type StatusFilter = "all" | "available" | "low_quota" | "banned";
type AccountExportMode = "single" | "multiple";
const ACCOUNT_SORT_STEP = 5;
type TranslateFn = (key: string, values?: Record<string, string | number>) => string;

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
function formatAccountPlanValueLabel(value: string, t: TranslateFn) {
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
      return t("未知");
    default:
      return normalized ? normalized.toUpperCase() : t("未知");
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
function formatPlanFilterLabel(value: string, t: TranslateFn) {
  const nextValue = String(value || "").trim();
  if (!nextValue || nextValue === "all") {
    return t("全部类型");
  }
  return formatAccountPlanValueLabel(nextValue, t);
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
function formatStatusFilterLabel(value: string, t: TranslateFn) {
  const nextValue = String(value || "").trim();
  switch (nextValue) {
    case "available":
      return t("可用");
    case "low_quota":
      return t("低配额");
    case "banned":
      return t("封禁");
    case "all":
    default:
      return t("全部");
  }
}

interface QuotaProgressProps {
  label: string;
  remainPercent: number | null;
  resetsAt: number | null;
  icon: LucideIcon;
  tone: "green" | "blue" | "amber";
  caption?: string;
  emptyText?: string;
  emptyResetText?: string;
}

interface QuotaSummaryItem extends QuotaProgressProps {
  id: string;
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
  caption,
  emptyText = "--",
  emptyResetText = "未知",
}: QuotaProgressProps) {
  const { t } = useI18n();
  const value = remainPercent ?? 0;
  const toneClasses = {
    blue: {
      track: "bg-blue-500/20",
      indicator: "bg-blue-500",
      icon: "text-blue-500",
    },
    green: {
      track: "bg-green-500/20",
      indicator: "bg-green-500",
      icon: "text-green-500",
    },
    amber: {
      track: "bg-amber-500/20",
      indicator: "bg-amber-500",
      icon: "text-amber-500",
    },
  } as const;
  const palette = toneClasses[tone];

  return (
    <div className="flex min-w-[180px] flex-col gap-1.5">
      <div className="flex items-center justify-between text-[10px]">
        <div className="min-w-0">
          <div className="flex items-center gap-1 text-muted-foreground">
            <Icon className={cn("h-3 w-3", palette.icon)} />
            <span>{label}</span>
          </div>
          {caption ? <div className="truncate text-[9px] text-muted-foreground/80">{caption}</div> : null}
        </div>
        <span className="font-medium">
          {remainPercent == null ? emptyText : `${value}%`}
        </span>
      </div>
      <Progress
        value={value}
        trackClassName={palette.track}
        indicatorClassName={palette.indicator}
      />
      <div className="text-[10px] text-muted-foreground">
        {t("重置")}: {formatTsFromSeconds(resetsAt, emptyResetText)}
      </div>
    </div>
  );
}

function QuotaOverviewCell({ items }: { items: QuotaSummaryItem[] }) {
  const { t } = useI18n();
  const summaryItems = items.slice(0, 2);

  return (
    <Tooltip>
      <TooltipTrigger render={<div />} className="block cursor-help">
        <div className="rounded-xl border border-primary/5 bg-accent/10 px-3 py-2">
          <div className="flex items-center gap-3">
            {summaryItems.map((item) => (
              <div key={item.id} className="min-w-0 flex-1 space-y-1">
                <div className="flex items-center justify-between text-[10px]">
                  <span className="text-muted-foreground">{item.label}</span>
                  <span className="font-medium text-foreground/80">
                    {item.remainPercent == null ? item.emptyText ?? "--" : `${item.remainPercent}%`}
                  </span>
                </div>
                <Progress
                  value={item.remainPercent ?? 0}
                  trackClassName={
                    item.tone === "blue"
                      ? "bg-blue-500/20"
                      : item.tone === "amber"
                        ? "bg-amber-500/20"
                        : "bg-green-500/20"
                  }
                  indicatorClassName={
                    item.tone === "blue"
                      ? "bg-blue-500"
                      : item.tone === "amber"
                        ? "bg-amber-500"
                        : "bg-green-500"
                  }
                />
              </div>
            ))}
          </div>
          <div className="mt-1 text-[10px] text-muted-foreground">
            {t("悬停查看全部额度")}
          </div>
        </div>
      </TooltipTrigger>
      <TooltipContent
        side="right"
        align="center"
        sideOffset={10}
        className="max-w-[340px] rounded-2xl bg-background p-3 text-foreground shadow-2xl"
      >
        <div className="space-y-3">
          <div className="space-y-1">
            <p className="text-sm font-semibold">{t("额度详情")}</p>
            <p className="text-[10px] text-muted-foreground">
              {t("标准额度与专属额度统一在这里查看。")}
            </p>
          </div>
          <div className="space-y-2">
            {items.map((item) => (
              <QuotaProgress
                key={item.id}
                label={item.label}
                remainPercent={item.remainPercent}
                resetsAt={item.resetsAt}
                icon={item.icon}
                tone={item.tone}
                caption={item.caption}
                emptyText={item.emptyText}
                emptyResetText={item.emptyResetText}
              />
            ))}
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
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
function getAccountStatusAction(account: Account, t: TranslateFn): {
  action: "enable" | "disable" | null;
  label: string;
  icon: LucideIcon;
} {
  const normalizedStatus = String(account.status || "")
    .trim()
    .toLowerCase();
  if (normalizedStatus === "disabled") {
    return { action: "enable", label: t("启用账号"), icon: Power };
  }
  if (normalizedStatus === "inactive") {
    return { action: "enable", label: t("恢复账号"), icon: Power };
  }
  if (normalizedStatus === "banned") {
    return { action: null, label: t("封禁账号"), icon: PowerOff };
  }
  return { action: "disable", label: t("禁用账号"), icon: PowerOff };
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
function formatAccountPlanLabel(account: Account, t: TranslateFn): string | null {
  const normalized = normalizeAccountPlanKey(account);
  return normalized === "unknown"
    ? null
    : formatAccountPlanValueLabel(normalized, t);
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

function formatAccountExportModeLabel(value: string, t: TranslateFn) {
  return value === "single" ? t("单 JSON") : t("多 JSON");
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
  const { t } = useI18n();
  const accountPlanLabel = formatAccountPlanLabel(account, t);
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
                {t("优先")}
              </Badge>
            ) : null}
          </div>
          <span className="truncate font-mono text-[10px] uppercase text-muted-foreground opacity-60">
            {account.id.slice(0, 16)}...
          </span>
          <span className="mt-1 text-[10px] text-muted-foreground">
            {t("最近刷新")}: {formatTsFromSeconds(account.lastRefreshAt, t("从未刷新"))}
          </span>
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-sm">
        <div className="flex min-w-[260px] flex-col gap-2">
          <div className="grid gap-2 sm:grid-cols-2">
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("账号类型")}</div>
              <div className="font-medium">{accountPlanLabel || t("未知")}</div>
            </div>
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("当前状态")}</div>
              <div className="font-medium">
                {t(account.availabilityText || "未知")}
              </div>
            </div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("标签")}</div>
            <div className="break-words">{tagsText || t("未设置")}</div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("备注")}</div>
            <div className="whitespace-pre-wrap break-words">
              {noteText || t("未设置")}
            </div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("账号 ID")}</div>
            <div className="break-all font-mono text-[11px]">{account.id}</div>
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

export default function AccountsPage() {
  const router = useRouter();
  const { t } = useI18n();
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
    deleteUnavailableFree,
    importByFile,
    importByDirectory,
    exportAccounts,
    isRefreshingAccountId,
    isRefreshingAllAccounts,
    isExporting,
    isDeletingMany,
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
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const [exportModeDraft, setExportModeDraft] =
    useState<AccountExportMode>("multiple");
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
  const importFileActionLabel = isDesktopRuntime ? t("按文件导入") : t("选择文件导入");
  const importDirectoryActionLabel = isDesktopRuntime
    ? t("按文件夹导入")
    : t("选择目录导入");
  const exportActionLabel =
    !isDesktopRuntime && canUseBrowserDownloadExport ? t("导出到浏览器") : t("导出账号");
  const exportActionShortcut = isExporting
    ? "..."
    : !isDesktopRuntime && canUseBrowserDownloadExport
      ? "DL"
      : "ZIP";

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
      { id: "all" as const, label: `${t("全部")} (${accounts.length})` },
      {
        id: "available" as const,
        label: `${t("可用")} (${accounts.filter((account) => account.isAvailable).length})`,
      },
      {
        id: "low_quota" as const,
        label: `${t("低配额")} (${accounts.filter((account) => account.isLowQuota).length})`,
      },
      {
        id: "banned" as const,
        label: `${t("封禁")} (${accounts.filter((account) => isBannedAccount(account)).length})`,
      },
    ],
    [accounts, t],
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
      ? `${t("当前已选择")} ${exportSelectionCount} ${t("个账号，本次将只导出选中的账号。")}`
      : `${t("当前未选择账号，本次将导出全部")} ${accounts.length} ${t("个账号。")}`;

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
      toast.error(t("请先选择要删除的账号"));
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
      toast.error(t("当前没有可清理的封禁账号"));
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
      toast.info(t("服务未连接，暂时无法导出账号"));
      return;
    }
    if (!accounts.length) {
      toast.info(t("当前没有可导出的账号"));
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
      toast.info(t("当前没有可导出的账号"));
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
      toast.error(t("未找到当前账号，请刷新后重试"));
      return;
    }

    const targetFilteredIndex =
      direction === "up" ? filteredIndex - 1 : filteredIndex + 1;
    if (targetFilteredIndex < 0) {
      toast.info(t("当前账号已经在最前面"));
      return;
    }
    if (targetFilteredIndex >= filteredAccounts.length) {
      toast.info(t("当前账号已经在最后面"));
      return;
    }

    const targetAccount = filteredAccounts[targetFilteredIndex];
    const reorderedAccounts = accounts.filter((item) => item.id !== account.id);
    const anchorIndex = reorderedAccounts.findIndex(
      (item) => item.id === targetAccount.id,
    );
    if (anchorIndex === -1) {
      toast.error(t("未找到目标账号，请刷新后重试"));
      return;
    }

    reorderedAccounts.splice(direction === "up" ? anchorIndex : anchorIndex + 1, 0, account);
    const updates = buildAccountOrderUpdates(reorderedAccounts);
    if (!updates.length) {
      toast.info(t("账号顺序未变化"));
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
      toast.info(t("账号数量不足，无需重新排序"));
      return;
    }

    const reorderedAccounts = buildAccountsBySizeOrder(accounts, mode);
    const updates = buildAccountOrderUpdates(reorderedAccounts);
    if (!updates.length) {
      toast.info(
        mode === "large-first"
          ? t("当前已经是大号优先顺序")
          : t("当前已经是小号优先顺序"),
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
      toast.error(t("请输入账号名称"));
      return;
    }

    const rawSort = sortDraft.trim();
    if (!rawSort) {
      toast.error(t("请输入顺序值"));
      return;
    }

    const parsed = Number(rawSort);
    if (!Number.isFinite(parsed)) {
      toast.error(t("顺序必须是数字"));
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
            {t("服务未连接，账号列表与相关操作暂不可用；连接恢复后会自动继续加载。")}
          </CardContent>
        </Card>
      ) : null}
      <Card className="glass-card border-none shadow-md backdrop-blur-md">
        <CardContent className="grid gap-3 pt-0 lg:grid-cols-[200px_auto_minmax(0,1fr)_auto] lg:items-center">
          <div className="min-w-0">
            <Input
              placeholder={t("搜索账号名 / 编号...")}
              className="glass-card h-10 rounded-xl px-3"
              value={search}
              onChange={(event) => handleSearchChange(event.target.value)}
            />
          </div>

          <div className="flex shrink-0 items-center gap-3">
            <Select value={planFilter} onValueChange={handlePlanFilterChange}>
              <SelectTrigger className="h-10 w-[140px] shrink-0 rounded-xl bg-card/50">
                <SelectValue placeholder={t("全部类型")}>
                  {(value) => formatPlanFilterLabel(String(value || ""), t)}
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">
                  {t("全部类型")} ({accounts.length})
                </SelectItem>
                {planTypes.map((planType) => (
                  <SelectItem key={planType.value} value={planType.value}>
                    {formatAccountPlanValueLabel(planType.value, t)} ({planType.count})
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
                <SelectValue placeholder={t("全部状态")}>
                  {(value) => formatStatusFilterLabel(String(value || ""), t)}
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
                    <span className="text-sm font-medium">{t("账号操作")}</span>
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
                    {t("刷新")}
                  </DropdownMenuLabel>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady || isRefreshingAllAccounts}
                    onClick={() => refreshAllAccounts()}
                  >
                    <RefreshCw
                      className={cn("mr-2 h-4 w-4", isRefreshingAllAccounts && "animate-spin")}
                    />
                    {t("刷新账号用量")}
                    <DropdownMenuShortcut>ALL</DropdownMenuShortcut>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={() => refreshAccountList()}
                  >
                    <RefreshCw className="mr-2 h-4 w-4" />
                    {t("刷新列表")}
                    <DropdownMenuShortcut>LIST</DropdownMenuShortcut>
                  </DropdownMenuItem>
                </DropdownMenuGroup>
                <DropdownMenuSeparator />
                <DropdownMenuGroup>
                  <DropdownMenuLabel className="px-2 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground/80">
                    {t("账号管理")}
                  </DropdownMenuLabel>
                  <DropdownMenuItem
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={() => setAddAccountModalOpen(true)}
                  >
                    <Plus className="mr-2 h-4 w-4" /> {t("添加账号")}
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
                    {t("排序")}
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
                    {t("大号优先排序")}
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
                    {t("小号优先排序")}
                    <DropdownMenuShortcut>FREE</DropdownMenuShortcut>
                  </DropdownMenuItem>
                </DropdownMenuGroup>
                <DropdownMenuSeparator />
                <DropdownMenuGroup>
                  <DropdownMenuLabel className="px-2 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground/80">
                    {t("清理")}
                  </DropdownMenuLabel>
                  <DropdownMenuItem
                    disabled={!isServiceReady || !effectiveSelectedIds.length || isDeletingMany}
                    variant="destructive"
                    className="h-9 rounded-lg px-2"
                    onClick={handleDeleteSelected}
                  >
                    <Trash2 className="mr-2 h-4 w-4" /> {t("删除选中账号")}
                    <DropdownMenuShortcut>
                      {effectiveSelectedIds.length || "-"}
                    </DropdownMenuShortcut>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    variant="destructive"
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={() => deleteUnavailableFree()}
                  >
                    <Trash2 className="mr-2 h-4 w-4" /> {t("清理免费不可用账号")}
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    variant="destructive"
                    className="h-9 rounded-lg px-2"
                    disabled={!isServiceReady}
                    onClick={handleDeleteBanned}
                  >
                    <Trash2 className="mr-2 h-4 w-4" /> {t("一键清理封禁账号")}
                  </DropdownMenuItem>
                </DropdownMenuGroup>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </CardContent>
      </Card>

      <Dialog open={exportDialogOpen} onOpenChange={setExportDialogOpen}>
        <DialogContent className="glass-card border-border/70 sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{t("导出账号")}</DialogTitle>
            <DialogDescription>
              {t("导出范围会自动按当前选择决定；如果没有选中账号，就导出全部。")}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-2">
            <div className="rounded-xl bg-muted/20 px-3 py-3 text-sm text-foreground/80">
              {exportScopeText}
            </div>
            <div className="space-y-2">
              <Label htmlFor="account-export-mode">{t("导出格式")}</Label>
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
                    {(value) => formatAccountExportModeLabel(String(value || ""), t)}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="multiple">{t("多 JSON")}</SelectItem>
                  <SelectItem value="single">{t("单 JSON")}</SelectItem>
                </SelectContent>
              </Select>
              <div className="text-xs text-muted-foreground">
                {exportModeDraft === "single"
                  ? t("导出为一个 `accounts.json` 数组文件，适合整体备份和再次导入。")
                  : t("每个账号导出为一个独立 JSON 文件，适合逐个分发或单独管理。")}
              </div>
            </div>
          </div>
          <DialogFooter>
            <DialogClose
              className={cn(buttonVariants({ variant: "outline" }), "rounded-xl")}
              disabled={isExporting}
            >
              {t("取消")}
            </DialogClose>
            <Button
              className="rounded-xl"
              onClick={() => void handleConfirmExport()}
              disabled={isExporting || exportTargetCount <= 0}
            >
              {isExporting ? t("导出中...") : t("开始导出")}
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
                <TableHead className="max-w-[220px]">{t("账号信息")}</TableHead>
                <TableHead className="min-w-[250px] text-center">{t("额度详情")}</TableHead>
                <TableHead className="w-[156px]">{t("顺序")}</TableHead>
                <TableHead>{t("状态")}</TableHead>
                <TableHead className="table-sticky-action-head w-[112px] text-center">
                  {t("操作")}
                </TableHead>
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
                      <div className="space-y-2">
                        <Skeleton className="h-4 w-40" />
                        <Skeleton className="h-4 w-40" />
                        <Skeleton className="h-4 w-40" />
                      </div>
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-10" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-6 w-16 rounded-full" />
                    </TableCell>
                    <TableCell className="table-sticky-action-cell">
                      <Skeleton className="mx-auto h-8 w-24" />
                    </TableCell>
                  </TableRow>
                ))
              ) : visibleAccounts.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-48 text-center">
                    <div className="flex flex-col items-center justify-center gap-2 text-muted-foreground">
                      <Search className="h-8 w-8 opacity-20" />
                      <p>{t("未找到符合条件的账号")}</p>
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
                    const extraUsageRows = getExtraUsageDisplayRows(account.usage);
                    const quotaItems: QuotaSummaryItem[] = [
                      {
                        id: `${account.id}-primary`,
                        label: t("5小时"),
                        remainPercent: account.primaryRemainPercent,
                        resetsAt: usageBuckets.primaryResetsAt,
                        icon: RefreshCw,
                        tone: "green",
                        caption: t("标准模型窗口"),
                        emptyText: secondaryWindowOnly ? t("未提供") : "--",
                        emptyResetText: secondaryWindowOnly ? t("未提供") : t("未知"),
                      },
                      {
                        id: `${account.id}-secondary`,
                        label: t("7天"),
                        remainPercent: account.secondaryRemainPercent,
                        resetsAt: usageBuckets.secondaryResetsAt,
                        icon: RefreshCw,
                        tone: "blue",
                        caption: t("长周期窗口"),
                        emptyText: primaryWindowOnly ? t("未提供") : "--",
                        emptyResetText: primaryWindowOnly ? t("未提供") : t("未知"),
                      },
                      ...extraUsageRows.map((item) => ({
                        id: item.id,
                        label: `${t(item.label, item.labelValues)}${item.labelSuffix ? t(item.labelSuffix) : ""}`,
                        remainPercent: item.remainPercent,
                        resetsAt: item.resetsAt,
                        icon: Zap,
                        tone: "amber" as const,
                        caption: t(item.windowLabel, item.windowLabelValues),
                        emptyText: "--",
                        emptyResetText: t("未知"),
                      })),
                    ];
                    const statusAction = getAccountStatusAction(account, t);
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
                            isPreferred={account.preferred}
                          />
                        </TableCell>
                        <TableCell>
                          <QuotaOverviewCell items={quotaItems} />
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
                            title={t("上移一位")}
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
                            title={t("下移一位")}
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
                            title={t("编辑账号信息")}
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
                            {t(account.availabilityText || "未知")}
                          </span>
                        </div>
                      </TableCell>
                      <TableCell className="table-sticky-action-cell">
                        <div className="table-action-cell gap-1">
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 text-muted-foreground transition-colors hover:text-primary"
                            disabled={!isServiceReady}
                            onClick={() => openUsage(account)}
                            title={t("用量详情")}
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
                                  account.preferred
                                    ? clearPreferredAccount(account.id)
                                    : setPreferredAccount(account.id)
                                }
                              >
                                <Pin className="h-4 w-4" />
                                {account.preferred
                                  ? t("取消优先")
                                  : t("设为优先")}
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
                                <ExternalLink className="h-4 w-4" /> {t("详情与日志")}
                              </DropdownMenuItem>
                              <DropdownMenuSeparator />
                              <DropdownMenuItem
                                className="gap-2 text-red-500"
                                disabled={!isServiceReady}
                                onClick={() => handleDeleteSingle(account)}
                              >
                                <Trash2 className="h-4 w-4" /> {t("删除")}
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
          {t("共")} {filteredAccounts.length} {t("个账号")}
          {effectiveSelectedIds.length > 0 ? (
            <span className="ml-1 text-primary">
              ({t("已选择")} {effectiveSelectedIds.length} {t("个")})
            </span>
          ) : null}
        </div>
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2">
            <span className="whitespace-nowrap text-xs text-muted-foreground">
              {t("每页显示")}
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
              {t("上一页")}
            </Button>
            <div className="min-w-[60px] text-center text-xs font-medium">
              {t("第")} {safePage} / {totalPages} {t("页")}
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
              {t("下一页")}
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
          deleteDialogState?.kind === "single" ? t("删除账号") : t("批量删除账号")
        }
        description={
          deleteDialogState?.kind === "single"
            ? `${t("确定删除账号")} ${deleteDialogState.account.name} ${t("吗？删除后不可恢复。")}`
            : `${t("确定删除选中的")} ${deleteDialogState?.count || 0} ${t("个账号吗？删除后不可恢复。")}`
        }
        confirmText={t("删除")}
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
            <DialogTitle>{t("编辑账号信息")}</DialogTitle>
            <DialogDescription>
              {accountEditorState
                ? `${t("修改")} ${accountEditorState.accountName} ${t("的名称、标签、备注与排序。")}`
                : t("修改账号的基础资料。")}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-2">
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="grid gap-2">
                <Label htmlFor="account-label-input">{t("账号名称")}</Label>
                <Input
                  id="account-label-input"
                  value={labelDraft}
                  disabled={Boolean(isUpdatingProfileAccountId)}
                  onChange={(event) => setLabelDraft(event.target.value)}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="account-tags-input">{t("标签（逗号分隔）")}</Label>
                <Input
                  id="account-tags-input"
                  value={tagsDraft}
                  disabled={Boolean(isUpdatingProfileAccountId)}
                  onChange={(event) => setTagsDraft(event.target.value)}
                  placeholder={t("例如：高频, 团队A")}
                />
              </div>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="account-note-input">{t("备注")}</Label>
              <Textarea
                id="account-note-input"
                value={noteDraft}
                disabled={Boolean(isUpdatingProfileAccountId)}
                onChange={(event) => setNoteDraft(event.target.value)}
                placeholder={t("例如：主账号 / 测试号 / 团队共享")}
                className="min-h-[108px]"
              />
            </div>
            <div className="grid gap-2 sm:grid-cols-[minmax(0,1fr)_120px] sm:items-end">
              <div className="grid gap-2">
                <Label htmlFor="account-sort-input">{t("顺序值")}</Label>
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
                <span>{t("值越小越靠前")}</span>
                <span>{t("仅修改当前账号")}</span>
              </div>
            </div>
            <div className="grid gap-3 rounded-xl bg-muted/20 px-3 py-3 text-[11px] text-muted-foreground sm:grid-cols-2">
              <div className="space-y-1">
                <div>{t("账号 ID")}</div>
                <div className="break-all font-mono">
                  {accountEditorState?.accountId || "-"}
                </div>
              </div>
              <div className="space-y-1">
                <div>{t("账号类型")}</div>
                <div className="font-medium text-foreground/80">
                  {currentEditingAccount
                    ? formatAccountPlanLabel(currentEditingAccount, t) || t("未知")
                    : t("未知")}
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
              {t("取消")}
            </DialogClose>
            <Button
              disabled={Boolean(isUpdatingProfileAccountId)}
              onClick={() => void handleConfirmAccountEditor()}
            >
              {t("保存")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
