"use client";

import { Suspense, useEffect, useMemo, useState, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  CheckCircle2,
  Copy,
  Database,
  RefreshCw,
  Shield,
  Trash2,
  Zap,
  type LucideIcon,
} from "lucide-react";
import { toast } from "sonner";
import { ConfirmDialog } from "@/components/modals/confirm-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { accountClient } from "@/lib/api/account-client";
import {
  buildStartupSnapshotQueryKey,
  STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
} from "@/lib/api/startup-snapshot";
import { serviceClient } from "@/lib/api/service-client";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { useDeferredDesktopActivation } from "@/hooks/useDeferredDesktopActivation";
import { useLocalDayRange } from "@/hooks/useLocalDayRange";
import { usePageTransitionReady } from "@/hooks/usePageTransitionReady";
import { useI18n } from "@/lib/i18n/provider";
import { useAppStore } from "@/lib/store/useAppStore";
import { copyTextToClipboard } from "@/lib/utils/clipboard";
import { formatCompactNumber, formatTsFromSeconds } from "@/lib/utils/usage";
import { cn } from "@/lib/utils";
import {
  AccountListResult,
  AggregateApi,
  ApiKey,
  GatewayErrorLog,
  RequestLog,
  RequestLogFilterSummary,
  RequestLogListResult,
  StartupSnapshot,
} from "@/types";

type StatusFilter = "all" | "2xx" | "4xx" | "5xx";
type LogsTab = "requests" | "gateway-errors";
type TranslateFn = (message: string, values?: Record<string, string | number>) => string;

/**
 * 函数 `getStatusBadge`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - statusCode: 参数 statusCode
 *
 * # 返回
 * 返回函数执行结果
 */
function getStatusBadge(statusCode: number | null) {
  if (statusCode == null) {
    return <Badge variant="secondary">-</Badge>;
  }
  if (statusCode >= 200 && statusCode < 300) {
    return (
      <Badge className="border-green-500/20 bg-green-500/10 text-green-500">
        {statusCode}
      </Badge>
    );
  }
  if (statusCode >= 400 && statusCode < 500) {
    return (
      <Badge className="border-yellow-500/20 bg-yellow-500/10 text-yellow-500">
        {statusCode}
      </Badge>
    );
  }
  return (
    <Badge className="border-red-500/20 bg-red-500/10 text-red-500">
      {statusCode}
    </Badge>
  );
}

/**
 * 函数 `SummaryCard`
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
function SummaryCard({
  title,
  value,
  description,
  icon: Icon,
  toneClass,
}: {
  title: string;
  value: string;
  description: string;
  icon: LucideIcon;
  toneClass: string;
}) {
  return (
    <Card
      size="sm"
      className="glass-card border-none shadow-sm backdrop-blur-md transition-all hover:-translate-y-0.5"
    >
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-1.5">
        <CardTitle className="text-[13px] font-medium text-muted-foreground">
          {title}
        </CardTitle>
        <div
          className={cn(
            "flex h-8 w-8 items-center justify-center rounded-xl",
            toneClass,
          )}
        >
          <Icon className="h-3.5 w-3.5" />
        </div>
      </CardHeader>
      <CardContent className="space-y-0.5">
        <div className="text-[2rem] leading-none font-semibold tracking-tight">
          {value}
        </div>
        <p className="text-[11px] text-muted-foreground">{description}</p>
      </CardContent>
    </Card>
  );
}

/**
 * 函数 `LogsPageSkeleton`
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
function LogsPageSkeleton() {
  return (
    <div className="space-y-5">
      <Skeleton className="h-28 w-full rounded-3xl" />
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <Skeleton key={index} className="h-32 w-full rounded-3xl" />
        ))}
      </div>
      <Skeleton className="h-[420px] w-full rounded-3xl" />
    </div>
  );
}

/**
 * 函数 `formatDuration`
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
function formatDuration(value: number | null): string {
  if (value == null) return "-";
  if (value >= 10_000) return `${Math.round(value / 1000)}s`;
  if (value >= 1000) return `${(value / 1000).toFixed(1).replace(/\.0$/, "")}s`;
  return `${Math.round(value)}ms`;
}

/**
 * 函数 `formatTokenAmount`
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
function formatTokenAmount(value: number | null | undefined): string {
  const normalized =
    typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
  return normalized.toLocaleString("zh-CN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

/**
 * 函数 `formatCompactTokenAmount`
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
function formatCompactTokenAmount(value: number | null | undefined): string {
  const normalized =
    typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
  if (normalized < 1000) {
    return formatTokenAmount(normalized);
  }
  return formatCompactNumber(normalized, "0.00", 2, true);
}

/**
 * 函数 `formatTableTokenAmount`
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
function formatTableTokenAmount(value: number | null | undefined): string {
  const normalized =
    typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
  return Math.round(normalized).toLocaleString("zh-CN");
}

/**
 * 函数 `fallbackAccountNameFromId`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - accountId: 参数 accountId
 *
 * # 返回
 * 返回函数执行结果
 */
function fallbackAccountNameFromId(accountId: string): string {
  const raw = accountId.trim();
  if (!raw) return "";
  const sep = raw.indexOf("::");
  if (sep < 0) return "";
  return raw.slice(sep + 2).trim();
}

/**
 * 函数 `fallbackAccountDisplayFromKey`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - keyId: 参数 keyId
 *
 * # 返回
 * 返回函数执行结果
 */
function fallbackAccountDisplayFromKey(keyId: string): string {
  const raw = keyId.trim();
  if (!raw) return "";
  return `Key ${raw.slice(0, 10)}`;
}

/**
 * 函数 `formatCompactKeyLabel`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - keyId: 参数 keyId
 *
 * # 返回
 * 返回函数执行结果
 */
function formatCompactKeyLabel(keyId: string): string {
  if (!keyId) return "-";
  if (keyId.length <= 12) return keyId;
  return `${keyId.slice(0, 8)}...`;
}

/**
 * 函数 `resolveDisplayRequestPath`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - log: 参数 log
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveDisplayRequestPath(log: RequestLog): string {
  const originalPath = String(log.originalPath || "").trim();
  if (originalPath) {
    return originalPath;
  }
  return String(log.path || log.requestPath || "").trim();
}

/**
 * 函数 `resolveUpstreamDisplay`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - upstreamUrl: 参数 upstreamUrl
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveUpstreamDisplay(upstreamUrl: string, t: TranslateFn): string {
  const raw = String(upstreamUrl || "").trim();
  if (!raw) return "";
  if (raw === "默认" || raw === "本地" || raw === "自定义") {
    return t(raw);
  }
  try {
    const url = new URL(raw);
    const pathname = url.pathname.replace(/\/+$/, "");
    return pathname ? `${url.host}${pathname}` : url.host;
  } catch {
    return raw;
  }
}

/**
 * 函数 `resolveAccountDisplayName`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - log: 参数 log
 * - accountNameMap: 参数 accountNameMap
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveAccountDisplayName(
  log: RequestLog,
  accountNameMap: Map<string, string>,
): string {
  if (log.accountId) {
    const label = accountNameMap.get(log.accountId);
    if (label) {
      return label;
    }
    const fallbackName = fallbackAccountNameFromId(log.accountId);
    if (fallbackName) {
      return fallbackName;
    }
  }
  return fallbackAccountDisplayFromKey(log.keyId);
}

/**
 * 函数 `resolveAccountDisplayNameById`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - accountId: 参数 accountId
 * - accountNameMap: 参数 accountNameMap
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveAccountDisplayNameById(
  accountId: string,
  accountNameMap: Map<string, string>,
): string {
  const normalized = String(accountId || "").trim();
  if (!normalized) return "";
  return (
    accountNameMap.get(normalized) ||
    fallbackAccountNameFromId(normalized) ||
    normalized
  );
}

/**
 * 函数 `resolveDisplayedStatusCode`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - log: 参数 log
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveDisplayedStatusCode(log: RequestLog): number | null {
  const statusCode = log.statusCode;
  const hasError = Boolean(String(log.error || "").trim());
  if (statusCode == null) {
    return hasError ? 502 : null;
  }
  if (hasError && statusCode < 400) {
    return 502;
  }
  return statusCode;
}

/**
 * 函数 `resolveAggregateApiDisplayName`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - log: 参数 log
 * - aggregateApi: 参数 aggregateApi
 * - apiKey: 参数 apiKey
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveAggregateApiDisplayName(
  log: RequestLog,
  aggregateApi: AggregateApi | null,
  apiKey: ApiKey | null,
): string {
  if (log.aggregateApiSupplierName && log.aggregateApiSupplierName.trim()) {
    return log.aggregateApiSupplierName.trim();
  }
  if (aggregateApi?.supplierName && aggregateApi.supplierName.trim()) {
    return aggregateApi.supplierName.trim();
  }
  if (apiKey?.aggregateApiUrl) {
    return apiKey.aggregateApiUrl.trim();
  }
  return "-";
}

/**
 * 函数 `resolveAggregateApiTooltipUrl`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - log: 参数 log
 * - aggregateApi: 参数 aggregateApi
 * - apiKey: 参数 apiKey
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveAggregateApiTooltipUrl(
  log: RequestLog,
  aggregateApi: AggregateApi | null,
  apiKey: ApiKey | null,
): string {
  if (log.aggregateApiUrl && log.aggregateApiUrl.trim()) {
    return log.aggregateApiUrl.trim();
  }
  if (aggregateApi?.url && aggregateApi.url.trim()) {
    return aggregateApi.url.trim();
  }
  if (apiKey?.aggregateApiUrl) {
    return apiKey.aggregateApiUrl.trim();
  }
  return "-";
}

/**
 * 函数 `resolveAggregateApiDisplayNameById`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - aggregateApiId: 参数 aggregateApiId
 * - aggregateApiMap: 参数 aggregateApiMap
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveAggregateApiDisplayNameById(
  aggregateApiId: string,
  aggregateApiMap: Map<string, AggregateApi>,
): string {
  const normalized = String(aggregateApiId || "").trim();
  if (!normalized) return "";
  const aggregateApi = aggregateApiMap.get(normalized);
  if (aggregateApi?.supplierName && aggregateApi.supplierName.trim()) {
    return aggregateApi.supplierName.trim();
  }
  if (aggregateApi?.url && aggregateApi.url.trim()) {
    return aggregateApi.url.trim();
  }
  return normalized;
}

/**
 * 函数 `normalizeAggregateApiUrl`
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
function normalizeAggregateApiUrl(value: string): string {
  return String(value || "").trim().replace(/\/+$/, "");
}

/**
 * 函数 `formatModelEffortDisplay`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - log: 参数 log
 *
 * # 返回
 * 返回函数执行结果
 */
function formatModelEffortDisplay(log: RequestLog): string {
  const model = String(log.model || "").trim();
  const effort = String(log.reasoningEffort || "").trim();
  if (model && effort) {
    return `${model}/${effort}`;
  }
  return model || effort || "-";
}

function normalizeRequestType(value: string): "ws" | "http" {
  return String(value || "").trim().toLowerCase() === "ws" ? "ws" : "http";
}

function normalizeDisplayServiceTier(value: string | null | undefined): string {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized || normalized === "auto") {
    return "";
  }
  if (normalized === "priority") {
    return "fast";
  }
  return normalized;
}

function resolveDisplayServiceTier(
  requestServiceTier: string | null | undefined,
): string {
  const direct = normalizeDisplayServiceTier(requestServiceTier);
  if (direct) {
    return direct;
  }
  return "auto";
}

function RequestTypeBadge({ requestType }: { requestType: string }) {
  const normalized = normalizeRequestType(requestType);
  const label = normalized.toUpperCase();
  const toneClass =
    normalized === "ws"
      ? "border-cyan-500/20 bg-cyan-500/10 text-cyan-500"
      : "border-slate-500/20 bg-slate-500/10 text-slate-500";
  return (
    <Badge className={cn("h-5 rounded-full px-1.5 text-[10px] font-medium", toneClass)}>
      {label}
    </Badge>
  );
}

function ServiceTierBadge({ serviceTier }: { serviceTier: string }) {
  const normalized = resolveDisplayServiceTier(serviceTier);
  const toneClass =
    normalized === "fast"
      ? "border-amber-500/20 bg-amber-500/10 text-amber-500"
      : "border-slate-500/20 bg-slate-500/10 text-slate-500";
  return (
    <Badge className={cn("h-5 rounded-full px-1.5 text-[10px] font-medium", toneClass)}>
      {normalized}
    </Badge>
  );
}

/**
 * 函数 `AccountKeyInfoCell`
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
function AccountKeyInfoCell({
  log,
  accountLabel,
  accountNameMap,
  apiKeyMap,
  aggregateApiMap,
}: {
  log: RequestLog;
  accountLabel: string;
  accountNameMap: Map<string, string>;
  apiKeyMap: Map<string, ApiKey>;
  aggregateApiMap: Map<string, AggregateApi>;
}) {
  const { t } = useI18n();
  const displayAccount = accountLabel || log.accountId || "-";
  const hasNamedAccount =
    Boolean(accountLabel) &&
    accountLabel.trim() !== "" &&
    accountLabel !== log.accountId;
  const attemptedAccountLabels = log.attemptedAccountIds
    .map((accountId) =>
      resolveAccountDisplayNameById(accountId, accountNameMap),
    )
    .filter((value) => value.trim().length > 0);
  const initialAccountLabel = resolveAccountDisplayNameById(
    log.initialAccountId,
    accountNameMap,
  );
  const attemptedAggregateApiLabels = log.attemptedAggregateApiIds
    .map((aggregateApiId) =>
      resolveAggregateApiDisplayNameById(aggregateApiId, aggregateApiMap),
    )
    .filter((value) => value.trim().length > 0);
  const initialAggregateApiLabel = resolveAggregateApiDisplayNameById(
    log.initialAggregateApiId,
    aggregateApiMap,
  );
  const apiKey = apiKeyMap.get(log.keyId) || null;
  const aggregateApiById = apiKey?.aggregateApiId
    ? aggregateApiMap.get(apiKey.aggregateApiId) || null
    : null;
  /**
   * 函数 `aggregateApiByUrl`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - (): 参数 ()
   *
   * # 返回
   * 返回函数执行结果
   */
  const aggregateApiByUrl = (() => {
    const upstreamUrl = normalizeAggregateApiUrl(log.upstreamUrl);
    if (!upstreamUrl) return null;
    for (const aggregateApi of aggregateApiMap.values()) {
      if (normalizeAggregateApiUrl(aggregateApi.url) === upstreamUrl) {
        return aggregateApi;
      }
    }
    return null;
  })();
  const aggregateApi = aggregateApiById || aggregateApiByUrl;
  const selectedAggregateApiId = aggregateApi?.id || "";
  const isAggregateApi = Boolean(
    log.aggregateApiSupplierName || log.aggregateApiUrl || aggregateApi,
  );
  const aggregateApiDisplayName = resolveAggregateApiDisplayName(
    log,
    aggregateApi,
    apiKey,
  );
  const aggregateApiDisplayUrl = resolveAggregateApiTooltipUrl(
    log,
    aggregateApi,
    apiKey,
  );
  const showAttemptHint =
    attemptedAccountLabels.length > 1 &&
    initialAccountLabel &&
    initialAccountLabel !== displayAccount;
  const showAggregateAttemptHint =
    attemptedAggregateApiLabels.length > 1 &&
    initialAggregateApiLabel &&
    String(log.initialAggregateApiId || "").trim() !== selectedAggregateApiId;

  if (isAggregateApi) {
    return (
      <Tooltip>
        <TooltipTrigger render={<div />} className="block text-left">
          <div className="flex max-w-[180px] flex-col gap-0.5 opacity-80">
            <div className="flex items-center gap-1">
              <Database className="h-3 w-3 text-primary" />
              <span className="truncate text-[11px] font-medium">
                {aggregateApiDisplayName}
              </span>
            </div>
            <div className="truncate font-mono text-[9px] text-muted-foreground">
              {aggregateApiDisplayUrl}
            </div>
            <div className="flex items-center gap-1 text-[9px] text-muted-foreground">
              <Shield className="h-2.5 w-2.5" />
              <span className="font-mono">{formatCompactKeyLabel(log.keyId)}</span>
            </div>
            {showAggregateAttemptHint ? (
              <div className="text-[9px] text-amber-500">
                {t("先试")} {initialAggregateApiLabel}
              </div>
            ) : null}
          </div>
        </TooltipTrigger>
        <TooltipContent className="max-w-sm">
          <div className="flex min-w-[240px] flex-col gap-2">
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("供应商名称")}</div>
              <div className="break-all font-mono text-[11px]">
                {aggregateApiDisplayName}
              </div>
            </div>
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">URL</div>
              <div className="break-all font-mono text-[11px]">
                {aggregateApiDisplayUrl}
              </div>
            </div>
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("密钥")}</div>
              <div className="break-all font-mono text-[11px]">
                {log.keyId || "-"}
              </div>
            </div>
            {attemptedAggregateApiLabels.length > 1 ? (
              <div className="space-y-0.5">
                <div className="text-[10px] text-background/70">{t("尝试链路")}</div>
                <div className="break-all font-mono text-[11px]">
                  {attemptedAggregateApiLabels.join(" -> ")}
                </div>
              </div>
            ) : null}
            {initialAggregateApiLabel ? (
              <div className="space-y-0.5">
                <div className="text-[10px] text-background/70">{t("首尝试渠道")}</div>
                <div className="break-all font-mono text-[11px]">
                  {initialAggregateApiLabel}
                </div>
              </div>
            ) : null}
          </div>
        </TooltipContent>
      </Tooltip>
    );
  }

  return (
    <Tooltip>
      <TooltipTrigger render={<div />} className="block text-left">
        <div className="flex flex-col gap-0.5 opacity-80">
          <div className="flex items-center gap-1">
            <Zap className="h-3 w-3 text-yellow-500" />
            <span className="max-w-[140px] truncate">{displayAccount}</span>
          </div>
          <div className="flex items-center gap-1 text-[9px] text-muted-foreground">
            <Shield className="h-2.5 w-2.5" />
            <span className="font-mono">
              {formatCompactKeyLabel(log.keyId)}
            </span>
          </div>
          {showAttemptHint ? (
            <div className="text-[9px] text-amber-500">
              {t("先试")} {initialAccountLabel}
            </div>
          ) : null}
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-sm">
        <div className="flex min-w-[240px] flex-col gap-2">
          {initialAccountLabel ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("首尝试账号")}</div>
              <div className="break-all font-mono text-[11px]">
                {initialAccountLabel}
              </div>
            </div>
          ) : null}
          {attemptedAccountLabels.length > 1 ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("尝试链路")}</div>
              <div className="break-all font-mono text-[11px]">
                {attemptedAccountLabels.join(" -> ")}
              </div>
            </div>
          ) : null}
          {hasNamedAccount ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("邮箱 / 名称")}</div>
              <div className="break-all font-mono text-[11px]">
                {accountLabel}
              </div>
            </div>
          ) : null}
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("账号 ID")}</div>
            <div className="break-all font-mono text-[11px]">
              {log.accountId || "-"}
            </div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("密钥")}</div>
            <div className="break-all font-mono text-[11px]">
              {log.keyId || "-"}
            </div>
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * 函数 `RequestRouteInfoCell`
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
function RequestRouteInfoCell({ log }: { log: RequestLog }) {
  const { t } = useI18n();
  const displayPath = resolveDisplayRequestPath(log) || "-";
  const recordedPath = String(log.path || log.requestPath || "").trim();
  const originalPath = String(log.originalPath || "").trim();
  const adaptedPath = String(log.adaptedPath || "").trim();
  const upstreamUrl = String(log.upstreamUrl || "").trim();
  const upstreamDisplay = resolveUpstreamDisplay(upstreamUrl, t);
  const requestType = normalizeRequestType(log.requestType);

  return (
    <Tooltip>
      <TooltipTrigger render={<div />} className="block text-left">
        <div className="flex flex-col gap-0.5">
          <div className="flex items-center gap-1.5">
            <RequestTypeBadge requestType={requestType} />
            <span className="font-bold text-primary">{log.method || "-"}</span>
          </div>
          <span className="max-w-[200px] truncate text-muted-foreground">
            {displayPath}
          </span>
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-md">
        <div className="flex min-w-[280px] flex-col gap-2">
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("请求类型")}</div>
            <div className="font-mono text-[11px] uppercase">{requestType}</div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("方法")}</div>
            <div className="font-mono text-[11px]">{log.method || "-"}</div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("显示地址")}</div>
            <div className="break-all font-mono text-[11px]">{displayPath}</div>
          </div>
          {recordedPath && recordedPath !== displayPath ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("记录地址")}</div>
              <div className="break-all font-mono text-[11px]">
                {recordedPath}
              </div>
            </div>
          ) : null}
          {originalPath && originalPath !== displayPath ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("原始地址")}</div>
              <div className="break-all font-mono text-[11px]">
                {originalPath}
              </div>
            </div>
          ) : null}
          {adaptedPath && adaptedPath !== displayPath ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("转发地址")}</div>
              <div className="break-all font-mono text-[11px]">
                {adaptedPath}
              </div>
            </div>
          ) : null}
          {log.responseAdapter ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("适配器")}</div>
              <div className="break-all font-mono text-[11px]">
                {log.responseAdapter}
              </div>
            </div>
          ) : null}
          {upstreamDisplay ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("上游")}</div>
              <div className="break-all font-mono text-[11px]">
                {upstreamDisplay}
              </div>
            </div>
          ) : null}
          {upstreamUrl ? (
            <div className="space-y-0.5">
              <div className="text-[10px] text-background/70">{t("上游地址")}</div>
              <div className="break-all font-mono text-[11px]">
                {upstreamUrl}
              </div>
            </div>
          ) : null}
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * 函数 `ErrorInfoCell`
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
function ErrorInfoCell({ error }: { error: string }) {
  const text = String(error || "").trim();
  if (!text) {
    return <span className="text-muted-foreground">-</span>;
  }

  return (
    <Tooltip>
      <TooltipTrigger render={<div />} className="block text-left">
        <span className="block max-w-[220px] truncate font-medium text-red-400">
          {text}
        </span>
      </TooltipTrigger>
      <TooltipContent className="max-w-md">
        <div className="max-w-[360px] break-all font-mono text-[11px]">
          {text}
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * 函数 `GatewayTooltipCell`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-04
 *
 * # 参数
 * - params: 参数 params
 *
 * # 返回
 * 返回函数执行结果
 */
function GatewayTooltipCell({
  preview,
  content,
  triggerClassName,
  contentClassName,
}: {
  preview: ReactNode;
  content: ReactNode;
  triggerClassName?: string;
  contentClassName?: string;
}) {
  return (
    <Tooltip>
      <TooltipTrigger render={<div />} className="block w-full text-left">
        <div className={cn("w-full", triggerClassName)}>{preview}</div>
      </TooltipTrigger>
      <TooltipContent
        className={cn("max-w-md whitespace-pre-wrap break-all", contentClassName)}
      >
        {content}
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * 函数 `ModelEffortCell`
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
function ModelEffortCell({
  log,
}: {
  log: RequestLog;
}) {
  const { t } = useI18n();
  const model = String(log.model || "").trim();
  const effort = String(log.reasoningEffort || "").trim();
  const clientServiceTier = resolveDisplayServiceTier(log.serviceTier);
  const effectiveServiceTier = resolveDisplayServiceTier(
    log.effectiveServiceTier || log.serviceTier,
  );
  const badgeServiceTier =
    effectiveServiceTier !== "auto" ? effectiveServiceTier : clientServiceTier;
  const display = formatModelEffortDisplay(log);

  return (
    <Tooltip>
      <TooltipTrigger render={<div />} className="block text-left">
        <div className="flex flex-col gap-1">
          <span className="block max-w-[160px] truncate font-medium text-foreground">
            {display}
          </span>
          <ServiceTierBadge serviceTier={badgeServiceTier} />
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-sm">
        <div className="flex min-w-[220px] flex-col gap-2">
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("模型")}</div>
            <div className="break-all font-mono text-[11px]">
              {model || "-"}
            </div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">{t("推理")}</div>
            <div className="break-all font-mono text-[11px]">
              {effort || "-"}
            </div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">
              {t("客户端显式服务等级")}
            </div>
            <div className="break-all font-mono text-[11px]">
              {clientServiceTier}
            </div>
          </div>
          <div className="space-y-0.5">
            <div className="text-[10px] text-background/70">
              {t("最终生效服务等级")}
            </div>
            <div className="break-all font-mono text-[11px]">
              {effectiveServiceTier}
            </div>
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

/**
 * 函数 `buildSummaryPlaceholder`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - logs: 参数 logs
 *
 * # 返回
 * 返回函数执行结果
 */
function buildSummaryPlaceholder(logs: RequestLog[]): RequestLogFilterSummary {
  const successCount = logs.filter((item) => {
    const statusCode = item.statusCode ?? 0;
    return statusCode >= 200 && statusCode < 300 && !String(item.error || "").trim();
  }).length;
  const errorCount = logs.filter((item) => {
    const statusCode = item.statusCode;
    return Boolean(String(item.error || "").trim()) || (statusCode != null && statusCode >= 400);
  }).length;
  const totalTokens = logs.reduce(
    (sum, item) => sum + Math.max(0, item.totalTokens || 0),
    0
  );
  const totalCostUsd = logs.reduce(
    (sum, item) => sum + Math.max(0, item.estimatedCostUsd || 0),
    0
  );

  return {
    totalCount: logs.length,
    filteredCount: logs.length,
    successCount,
    errorCount,
    totalTokens,
    totalCostUsd,
  };
}

/**
 * 函数 `LogsPageContent`
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
function LogsPageContent() {
  const { t } = useI18n();
  const localDayRange = useLocalDayRange();
  const searchParams = useSearchParams();
  const { serviceStatus } = useAppStore();
  const isPageActive = useDesktopPageActive("/logs/");
  const queryClient = useQueryClient();
  const areLogQueriesEnabled = useDeferredDesktopActivation(serviceStatus.connected);
  const routeQuery = searchParams.get("query") || "";
  const [search, setSearch] = useState(routeQuery);
  const [filter, setFilter] = useState<StatusFilter>("all");
  const [pageSize, setPageSize] = useState("10");
  const [page, setPage] = useState(1);
  const [gatewayPageSize, setGatewayPageSize] = useState("10");
  const [gatewayPage, setGatewayPage] = useState(1);
  const [clearConfirmOpen, setClearConfirmOpen] = useState(false);
  const [clearGatewayConfirmOpen, setClearGatewayConfirmOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<LogsTab>("requests");
  const [gatewayStageFilter, setGatewayStageFilter] = useState("all");
  const pageSizeNumber = Number(pageSize) || 10;
  const gatewayPageSizeNumber = Number(gatewayPageSize) || 10;
  const startupSnapshot = queryClient.getQueryData<StartupSnapshot>(
    buildStartupSnapshotQueryKey(
      serviceStatus.addr,
      STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
      localDayRange.dayStartTs,
    )
  );
  const startupAccounts = startupSnapshot?.accounts || [];
  const startupApiKeys = startupSnapshot?.apiKeys || [];
  const startupRequestLogs = startupSnapshot?.requestLogs || [];
  const canUseStartupLogsPlaceholder =
    !routeQuery.trim() && !search.trim() && filter === "all" && page === 1;
  const hasStartupLogsSnapshot =
    canUseStartupLogsPlaceholder && startupRequestLogs.length > 0;

  const { data: accountsResult } = useQuery({
    queryKey: ["accounts", "lookup"],
    queryFn: () => accountClient.list(),
    enabled: areLogQueriesEnabled && isPageActive,
    staleTime: 60_000,
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

  const { data: apiKeysResult } = useQuery({
    queryKey: ["apikeys", "lookup"],
    queryFn: () => accountClient.listApiKeys(),
    enabled: areLogQueriesEnabled && isPageActive,
    staleTime: 60_000,
    retry: 1,
    placeholderData: (previousData): ApiKey[] | undefined =>
      previousData || (startupApiKeys.length > 0 ? startupApiKeys : undefined),
  });

  const { data: aggregateApisResult } = useQuery({
    queryKey: ["aggregate-apis", "lookup"],
    queryFn: () => accountClient.listAggregateApis(),
    enabled: areLogQueriesEnabled && isPageActive,
    staleTime: 60_000,
    retry: 1,
  });

  const { data: logsResult, isLoading, isError: isLogsError } = useQuery({
    queryKey: ["logs", "list", search, filter, page, pageSizeNumber],
    queryFn: () =>
      serviceClient.listRequestLogs({
        query: search,
        statusFilter: filter,
        page,
        pageSize: pageSizeNumber,
      }),
    enabled: areLogQueriesEnabled && isPageActive,
    refetchInterval: 5000,
    retry: 1,
    placeholderData: (previousData): RequestLogListResult | undefined =>
      previousData ||
      (hasStartupLogsSnapshot
        ? {
            items: startupRequestLogs,
            total: startupRequestLogs.length,
            page: 1,
            pageSize: pageSizeNumber,
          }
        : undefined),
  });

  const { data: summaryResult, isError: isSummaryError } = useQuery({
    queryKey: ["logs", "summary", search, filter],
    queryFn: () =>
      serviceClient.getRequestLogSummary({
        query: search,
        statusFilter: filter,
      }),
    enabled: areLogQueriesEnabled && isPageActive,
    refetchInterval: 5000,
    retry: 1,
    placeholderData: (previousData) =>
      previousData ||
      (canUseStartupLogsPlaceholder
        ? buildSummaryPlaceholder(startupRequestLogs)
        : undefined),
  });

  const { data: gatewayLogsResult } = useQuery({
    queryKey: [
      "logs",
      "gateway-error-list",
      gatewayStageFilter,
      gatewayPage,
      gatewayPageSizeNumber,
    ],
    queryFn: () =>
      serviceClient.listGatewayErrorLogs({
        page: gatewayPage,
        pageSize: gatewayPageSizeNumber,
        stageFilter: gatewayStageFilter,
      }),
    enabled: areLogQueriesEnabled && isPageActive,
    refetchInterval: 5000,
    retry: 1,
  });

  const clearMutation = useMutation({
    mutationFn: () => serviceClient.clearRequestLogs(),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["logs"] }),
        queryClient.invalidateQueries({ queryKey: ["today-summary"] }),
        queryClient.invalidateQueries({ queryKey: ["startup-snapshot"] }),
      ]);
      toast.success(t("日志已清空"));
    },
    onError: (error: unknown) => {
      toast.error(error instanceof Error ? error.message : String(error));
    },
  });

  const clearGatewayMutation = useMutation({
    mutationFn: () => serviceClient.clearGatewayErrorLogs(),
    onSuccess: async () => {
      setGatewayPage(1);
      await queryClient.invalidateQueries({
        queryKey: ["logs", "gateway-error-list"],
      });
      toast.success(t("诊断日志已清空"));
    },
    onError: (error: unknown) => {
      toast.error(error instanceof Error ? error.message : String(error));
    },
  });

  const accountNameMap = useMemo(() => {
    return new Map(
      (accountsResult?.items || []).map((account) => [
        account.id,
        account.label || account.name || account.id,
      ]),
    );
  }, [accountsResult?.items]);

  const apiKeyMap = useMemo(() => {
    return new Map((apiKeysResult || []).map((apiKey) => [apiKey.id, apiKey]));
  }, [apiKeysResult]);

  const aggregateApiMap = useMemo(() => {
    return new Map(
      (aggregateApisResult || []).map((aggregateApi) => [
        aggregateApi.id,
        aggregateApi,
      ]),
    );
  }, [aggregateApisResult]);

  const logs = logsResult?.items || [];
  const isLogsLoading =
    serviceStatus.connected &&
    !hasStartupLogsSnapshot &&
    (!areLogQueriesEnabled || isLoading);
  usePageTransitionReady(
    "/logs/",
    !serviceStatus.connected ||
      (!isLogsLoading &&
        (Boolean(summaryResult) || isLogsError || isSummaryError)),
  );
  const currentPage = logsResult?.page || page;
  const summary = summaryResult || {
    totalCount: logsResult?.total || 0,
    filteredCount: logsResult?.total || 0,
    successCount: 0,
    errorCount: 0,
    totalTokens: 0,
  };
  const totalPages = Math.max(
    1,
    Math.ceil((logsResult?.total || 0) / pageSizeNumber),
  );

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const frameId = window.requestAnimationFrame(() => {
      setSearch((current) => (current === routeQuery ? current : routeQuery));
      setPage(1);
    });
    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [routeQuery]);

  useEffect(() => {
    if (isPageActive) {
      return;
    }
    if (typeof window === "undefined") {
      return;
    }
    const frameId = window.requestAnimationFrame(() => {
      setClearConfirmOpen(false);
      setClearGatewayConfirmOpen(false);
    });
    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [isPageActive]);

  const currentFilterLabel =
    filter === "all"
      ? t("全部状态")
      : filter === "2xx"
        ? t("成功请求")
        : filter === "4xx"
          ? t("客户端错误")
          : t("服务端错误");
  const compactMetaText = `${summary.filteredCount}/${summary.totalCount} ${t("条")} · ${currentFilterLabel} · ${
    serviceStatus.connected ? t("5 秒刷新") : t("服务未连接")
  }`;

  const renderGatewayErrorContext = (item: GatewayErrorLog) => {
    const parts = [
      item.errorKind ? `kind=${item.errorKind}` : "",
      item.cfRay ? `cf_ray=${item.cfRay}` : "",
      item.compressionEnabled ? "compression=zstd" : "compression=none",
      item.compressionRetryAttempted ? "retry=no-compression" : "",
    ].filter(Boolean);
    return parts.join(" · ");
  };

  const gatewayStageFilterLabel =
    gatewayStageFilter === "all" ? t("全部阶段") : gatewayStageFilter;

  const gatewayErrorLogs = gatewayLogsResult?.items || [];
  const gatewayStageOptions = gatewayLogsResult?.stages || [];
  const gatewayCurrentPage = gatewayLogsResult?.page || gatewayPage;
  const gatewayTotal = gatewayLogsResult?.total || 0;
  const gatewayTotalPages = Math.max(
    1,
    Math.ceil(gatewayTotal / gatewayPageSizeNumber),
  );

  const copyGatewayErrorSummary = async (item: GatewayErrorLog) => {
    const payload = [
      `time=${formatTsFromSeconds(item.createdAt)}`,
      `stage=${item.stage || "-"}`,
      `path=${item.requestPath || "-"}`,
      `method=${item.method || "-"}`,
      `status=${item.statusCode ?? "-"}`,
      `cf_ray=${item.cfRay || "-"}`,
      `kind=${item.errorKind || "-"}`,
      `compression=${item.compressionEnabled ? "zstd" : "none"}`,
      `retry_without_compression=${item.compressionRetryAttempted ? "yes" : "no"}`,
      `account=${item.accountId || "-"}`,
      `key=${item.keyId || "-"}`,
      `message=${item.message || "-"}`,
    ].join("\n");

    try {
      await copyTextToClipboard(payload);
      toast.success(t("诊断信息已复制"));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t("复制失败"));
    }
  };

  return (
    <div className="animate-in space-y-5 fade-in duration-500">
      <Tabs
        value={activeTab}
        onValueChange={(value) => {
          if (value === "requests" || value === "gateway-errors") {
            setActiveTab(value);
          }
        }}
        className="w-full"
      >
        <TabsList className="glass-card flex h-11 w-full justify-start overflow-x-auto rounded-xl border-none p-1 no-scrollbar lg:w-fit">
          <TabsTrigger value="requests" className="gap-2 px-5 shrink-0">
            <Database className="h-4 w-4" /> {t("请求日志")}
          </TabsTrigger>
          <TabsTrigger value="gateway-errors" className="gap-2 px-5 shrink-0">
            <Shield className="h-4 w-4" /> {t("网关错误诊断")}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="requests" className="space-y-5">
          <Card className="glass-card border-none shadow-md backdrop-blur-md">
            <CardContent className="grid gap-3 pt-0 lg:grid-cols-[minmax(0,1fr)_auto_auto_auto] lg:items-center">
              <div className="min-w-0">
                <Input
                  placeholder={t("搜索路径、账号或密钥...")}
                  className="glass-card h-10 rounded-xl px-3"
                  value={search}
                  onChange={(event) => {
                    setSearch(event.target.value);
                    setPage(1);
                  }}
                />
              </div>
              <div className="flex shrink-0 items-center gap-1 rounded-xl border border-border/60 bg-muted/30 p-1">
                {["all", "2xx", "4xx", "5xx"].map((item) => (
                  <button
                    key={item}
                    onClick={() => {
                      setFilter(item as StatusFilter);
                      setPage(1);
                    }}
                    className={cn(
                      "rounded-lg px-3 py-1.5 text-xs font-semibold uppercase tracking-wide transition-all",
                      filter === item
                        ? "bg-background text-foreground shadow-sm"
                        : "text-muted-foreground hover:bg-background/60 hover:text-foreground",
                    )}
                  >
                    {item.toUpperCase()}
                  </button>
                ))}
              </div>
              <div className="flex shrink-0 items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  className="glass-card h-9 rounded-xl px-3.5"
                  onClick={() =>
                    queryClient.invalidateQueries({ queryKey: ["logs"] })
                  }
                >
                  <RefreshCw className="mr-1.5 h-4 w-4" /> {t("刷新")}
                </Button>
                <Button
                  variant="destructive"
                  size="sm"
                  className="h-9 rounded-xl px-3.5"
                  onClick={() => setClearConfirmOpen(true)}
                  disabled={clearMutation.isPending}
                >
                  <Trash2 className="mr-1.5 h-4 w-4" /> {t("清空日志")}
                </Button>
              </div>
              <div className="text-[11px] text-muted-foreground lg:justify-self-end lg:text-right">
                <span className="font-medium text-foreground">
                  {compactMetaText}
                </span>
              </div>
            </CardContent>
          </Card>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <SummaryCard
              title={t("当前结果")}
              value={`${summary.filteredCount}`}
              description={`${t("总日志")} ${summary.totalCount} ${t("条")}`}
              icon={Zap}
              toneClass="bg-primary/12 text-primary"
            />
            <SummaryCard
              title={t("2XX 成功")}
              value={`${summary.successCount}`}
              description={t("状态码 200-299")}
              icon={CheckCircle2}
              toneClass="bg-green-500/12 text-green-500"
            />
            <SummaryCard
              title={t("异常请求")}
              value={`${summary.errorCount}`}
              description={t("4xx / 5xx 或显式错误")}
              icon={AlertTriangle}
              toneClass="bg-red-500/12 text-red-500"
            />
            <SummaryCard
              title={t("累计Token")}
              value={formatCompactTokenAmount(summary.totalTokens)}
              description={t("当前筛选结果中的总Token")}
              icon={Database}
              toneClass="bg-amber-500/12 text-amber-500"
            />
          </div>

          <Card className="glass-card overflow-hidden border-none gap-0 py-0 shadow-xl backdrop-blur-md">
            <CardHeader className="flex min-h-1 items-center border-b border-border/40 bg-[var(--table-section-bg)] py-3">
              <div className="flex w-full flex-col gap-1 xl:flex-row xl:items-center xl:justify-between">
                <div>
                  <CardTitle className="text-[15px] font-semibold">
                    {t("请求明细 按")}{" "}
                    <span className="font-medium text-foreground">
                      {currentFilterLabel}
                    </span>{" "}
                    {t("展示")}
                  </CardTitle>
                </div>
                <div className="text-xs text-muted-foreground"></div>
              </div>
            </CardHeader>
            <CardContent className="px-0">
              <Table className="min-w-[1320px] table-fixed">
            <TableHeader>
              <TableRow>
                <TableHead className="h-12 w-[150px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                  {t("时间")}
                </TableHead>
                <TableHead className="w-[120px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                  {t("类型 / 方法 / 路径")}
                </TableHead>
                <TableHead className="w-[224px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                  {t("账号 / 密钥")}
                </TableHead>
                <TableHead className="w-[180px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                  {t("模型 / 推理 / 等级")}
                </TableHead>
                <TableHead className="w-[92px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                  {t("状态")}
                </TableHead>
                <TableHead className="w-[110px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                  {t("请求时长")}
                </TableHead>
                <TableHead className="w-[148px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                  {t("Token")}
                </TableHead>
                <TableHead className="w-[240px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                  {t("错误")}
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLogsLoading ? (
                Array.from({ length: 10 }).map((_, index) => (
                  <TableRow key={index}>
                    <TableCell>
                      <Skeleton className="h-4 w-32" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-40" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-32" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-24" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-6 w-12 rounded-full" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-12" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-20" />
                    </TableCell>
                    <TableCell>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                  </TableRow>
                ))
              ) : logs.length === 0 ? (
                <TableRow>
                  <TableCell
                    colSpan={8}
                    className="h-52 px-4 text-center text-sm text-muted-foreground"
                  >
                    {!serviceStatus.connected
                      ? t("服务未连接，无法获取日志")
                      : t("暂无请求日志")}
                  </TableCell>
                </TableRow>
              ) : (
                logs.map((log: RequestLog) => (
                  <TableRow
                    key={log.id}
                    className="group text-xs hover:bg-muted/20"
                  >
                    <TableCell className="px-4 py-3 font-mono text-[11px] text-muted-foreground">
                      {formatTsFromSeconds(log.createdAt, t("未知时间"))}
                    </TableCell>
                    <TableCell className="px-4 py-3 align-top">
                      <RequestRouteInfoCell log={log} />
                    </TableCell>
                    <TableCell className="px-4 py-3 align-top">
                      <AccountKeyInfoCell
                        log={log}
                        accountLabel={resolveAccountDisplayName(
                          log,
                          accountNameMap,
                        )}
                        accountNameMap={accountNameMap}
                        apiKeyMap={apiKeyMap}
                        aggregateApiMap={aggregateApiMap}
                      />
                    </TableCell>
                    <TableCell className="px-4 py-3 align-top">
                      <ModelEffortCell log={log} />
                    </TableCell>
                    <TableCell className="px-4 py-3 align-top">
                      {getStatusBadge(resolveDisplayedStatusCode(log))}
                    </TableCell>
                    <TableCell className="px-4 py-3 font-mono text-primary">
                      {formatDuration(log.durationMs)}
                    </TableCell>
                    <TableCell className="px-4 py-3 align-top">
                      <div className="flex flex-col gap-0.5 text-[10px] text-muted-foreground">
                        <span>{t("总")} {formatTableTokenAmount(log.totalTokens)}</span>
                        <span>
                          {t("输入")} {formatTableTokenAmount(log.inputTokens)}
                        </span>
                        <span className="opacity-60">
                          {t("缓存")} {formatTableTokenAmount(log.cachedInputTokens)}
                        </span>
                      </div>
                    </TableCell>
                    <TableCell className="px-4 py-3 text-left align-top">
                      <ErrorInfoCell error={log.error} />
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
            </CardContent>
          </Card>

          <div className="flex items-center justify-between px-2">
            <div className="text-xs text-muted-foreground">
              {t("共")} {summary.filteredCount} {t("条匹配日志")}
            </div>
            <div className="flex items-center gap-6">
              <div className="flex items-center gap-2">
                <span className="whitespace-nowrap text-xs text-muted-foreground">
                  {t("每页显示")}
                </span>
                <Select
                  value={pageSize}
                  onValueChange={(value) => {
                    setPageSize(value || "10");
                    setPage(1);
                  }}
                >
                  <SelectTrigger className="h-8 w-[78px] text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {["5", "10", "20", "50", "100", "200"].map((value) => (
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
                  disabled={currentPage <= 1}
                  onClick={() => setPage(Math.max(1, currentPage - 1))}
                >
                  {t("上一页")}
                </Button>
                <div className="min-w-[68px] text-center text-xs font-medium">
                  {t("第")} {currentPage} / {totalPages} {t("页")}
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 px-3 text-xs"
                  disabled={currentPage >= totalPages}
                  onClick={() => setPage(Math.min(totalPages, currentPage + 1))}
                >
                  {t("下一页")}
                </Button>
              </div>
            </div>
          </div>
        </TabsContent>

        <TabsContent value="gateway-errors" className="space-y-5">
          <Card className="glass-card border-none shadow-md backdrop-blur-md">
            <CardContent className="grid gap-4 pt-0 xl:grid-cols-[minmax(0,1fr)_auto] xl:items-center">
              <div className="space-y-1">
                <div className="text-sm font-medium text-foreground">
                  {t("网关错误诊断")}
                </div>
                <p className="text-xs text-muted-foreground">
                  {t("专门记录 challenge、无压缩重试和关键网关错误事件，便于排查 Cloudflare 拦截。")}
                </p>
              </div>
              <div className="flex flex-wrap items-center justify-between gap-3 xl:min-w-[520px] xl:justify-self-end">
                <div className="flex flex-wrap items-center gap-3">
                  <span className="whitespace-nowrap text-xs text-muted-foreground">
                    {t("阶段筛选")}
                  </span>
                  <Select
                    value={gatewayStageFilter}
                    onValueChange={(value) => {
                      setGatewayStageFilter(value || "all");
                      setGatewayPage(1);
                    }}
                  >
                    <SelectTrigger className="h-9 min-w-[220px] text-xs">
                      <SelectValue>{gatewayStageFilterLabel}</SelectValue>
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">{t("全部阶段")}</SelectItem>
                      {gatewayStageOptions.map((stage) => (
                        <SelectItem key={stage} value={stage}>
                          {stage}
                        </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                </div>
                <div className="flex flex-wrap items-center justify-end gap-3">
                  <Button
                    variant="outline"
                    size="sm"
                    className="glass-card h-9 rounded-xl px-3.5"
                    onClick={() =>
                      queryClient.invalidateQueries({
                        queryKey: ["logs", "gateway-error-list"],
                      })
                    }
                  >
                    <RefreshCw className="mr-1.5 h-4 w-4" /> {t("刷新")}
                  </Button>
                  <Button
                    variant="destructive"
                    size="sm"
                    className="h-9 rounded-xl px-3.5"
                    onClick={() => setClearGatewayConfirmOpen(true)}
                    disabled={clearGatewayMutation.isPending}
                  >
                    <Trash2 className="mr-1.5 h-4 w-4" /> {t("清空诊断")}
                  </Button>
                  <div className="whitespace-nowrap text-xs text-muted-foreground text-right">
                    {t("当前页")} {gatewayErrorLogs.length} {t("条")} / {t("共")} {gatewayTotal} {t("条")}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="glass-card overflow-hidden border-none gap-0 py-0 shadow-xl backdrop-blur-md">
            <CardHeader className="flex min-h-1 items-center border-b border-border/40 bg-[var(--table-section-bg)] py-3">
              <div className="flex w-full flex-col gap-1 xl:flex-row xl:items-center xl:justify-between">
                <div>
                  <CardTitle className="text-[15px] font-semibold">
                    {t("错误事件明细")}
                  </CardTitle>
                </div>
                <div className="text-xs text-muted-foreground">
                  {t("challenge / retry / transport")}
                </div>
              </div>
            </CardHeader>
            <CardContent className="px-0">
              <Table className="min-w-[1080px] table-fixed">
                <TableHeader>
                  <TableRow>
                    <TableHead className="h-12 w-[150px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                      {t("时间")}
                    </TableHead>
                    <TableHead className="w-[200px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                      {t("阶段")}
                    </TableHead>
                    <TableHead className="w-[120px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                      {t("方法 / 路径")}
                    </TableHead>
                    <TableHead className="w-[120px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                      {t("状态")}
                    </TableHead>
                    <TableHead className="w-[200px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                      {t("上下文")}
                    </TableHead>
                    <TableHead className="w-[290px] px-4 text-[11px] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
                      {t("消息")}
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {gatewayErrorLogs.length ? (
                    gatewayErrorLogs.map((item, index) => {
                      const gatewayContext = renderGatewayErrorContext(item) || "-";
                      const gatewayIdentity = item.accountId || item.keyId || "-";
                      const gatewayMethod = String(item.method || "-").trim() || "-";
                      const gatewayPath = String(item.requestPath || "-").trim() || "-";
                      const gatewayMessage = String(item.message || "-").trim() || "-";
                      const gatewayUpstreamUrl = String(item.upstreamUrl || "").trim();

                      return (
                        <TableRow
                          key={`${item.createdAt || 0}-${item.stage}-${index}`}
                        >
                          <TableCell className="px-4 py-3 align-top text-xs">
                            {formatTsFromSeconds(item.createdAt)}
                          </TableCell>
                          <TableCell className="px-4 py-3 align-top">
                            <GatewayTooltipCell
                              preview={
                                <>
                                  <div className="max-w-[180px] truncate font-mono text-[11px] text-foreground">
                                    {item.stage}
                                  </div>
                                  <div className="mt-1 max-w-[180px] truncate text-[11px] text-muted-foreground">
                                    {gatewayIdentity}
                                  </div>
                                </>
                              }
                              content={
                                <div className="flex min-w-[240px] flex-col gap-2">
                                  <div className="space-y-0.5">
                                    <div className="text-[10px] text-background/70">
                                      {t("阶段")}
                                    </div>
                                    <div className="font-mono text-[11px]">
                                      {item.stage}
                                    </div>
                                  </div>
                                  <div className="space-y-0.5">
                                    <div className="text-[10px] text-background/70">
                                      {t("账号 / 密钥")}
                                    </div>
                                    <div className="font-mono text-[11px]">
                                      {gatewayIdentity}
                                    </div>
                                  </div>
                                </div>
                              }
                            />
                          </TableCell>
                          <TableCell className="px-4 py-3 align-top">
                            <GatewayTooltipCell
                              preview={
                                <>
                                  <div className="max-w-[100px] truncate font-mono text-[11px] text-foreground">
                                    {gatewayMethod}
                                  </div>
                                  <div className="mt-1 max-w-[100px] truncate font-mono text-[11px] text-muted-foreground">
                                    {gatewayPath}
                                  </div>
                                </>
                              }
                              content={
                                <div className="flex min-w-[220px] flex-col gap-2">
                                  <div className="space-y-0.5">
                                    <div className="text-[10px] text-background/70">
                                      {t("方法")}
                                    </div>
                                    <div className="font-mono text-[11px]">
                                      {gatewayMethod}
                                    </div>
                                  </div>
                                  <div className="space-y-0.5">
                                    <div className="text-[10px] text-background/70">
                                      {t("路径")}
                                    </div>
                                    <div className="font-mono text-[11px]">
                                      {gatewayPath}
                                    </div>
                                  </div>
                                </div>
                              }
                            />
                          </TableCell>
                          <TableCell className="px-4 py-3 align-top">
                            {getStatusBadge(item.statusCode)}
                          </TableCell>
                          <TableCell className="px-4 py-3 align-top">
                            <GatewayTooltipCell
                              preview={
                                <div className="max-w-[180px] truncate font-mono text-[11px] text-muted-foreground">
                                  {gatewayContext}
                                </div>
                              }
                              content={
                                <div className="max-w-[360px] font-mono text-[11px]">
                                  {gatewayContext}
                                </div>
                              }
                            />
                          </TableCell>
                          <TableCell className="px-4 py-3 align-top">
                            <GatewayTooltipCell
                              preview={
                                <>
                                  <div className="max-w-[260px] truncate font-mono text-[11px] text-foreground">
                                    {gatewayMessage}
                                  </div>
                                  {gatewayUpstreamUrl ? (
                                    <div className="mt-1 max-w-[260px] truncate font-mono text-[11px] text-muted-foreground">
                                      {gatewayUpstreamUrl}
                                    </div>
                                  ) : null}
                                </>
                              }
                              content={
                                <div className="flex min-w-[260px] flex-col gap-2">
                                  <div className="space-y-0.5">
                                    <div className="text-[10px] text-background/70">
                                      {t("消息")}
                                    </div>
                                    <div className="font-mono text-[11px]">
                                      {gatewayMessage}
                                    </div>
                                  </div>
                                  {gatewayUpstreamUrl ? (
                                    <div className="space-y-0.5">
                                      <div className="text-[10px] text-background/70">
                                        {t("上游地址")}
                                      </div>
                                      <div className="font-mono text-[11px]">
                                        {gatewayUpstreamUrl}
                                      </div>
                                    </div>
                                  ) : null}
                                </div>
                              }
                            />
                            <div className="mt-2">
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-7 px-2 text-[11px]"
                                onClick={() => void copyGatewayErrorSummary(item)}
                              >
                                <Copy className="mr-1 h-3.5 w-3.5" /> {t("复制诊断")}
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      );
                    })
                  ) : (
                    <TableRow>
                      <TableCell
                        colSpan={6}
                        className="px-4 py-10 text-center text-sm text-muted-foreground"
                      >
                        {gatewayStageFilter !== "all"
                          ? t("当前筛选下没有匹配的诊断日志")
                          : t("暂无专门错误诊断日志")}
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <div className="flex items-center justify-between px-2">
            <div className="text-xs text-muted-foreground">
              {t("共")} {gatewayTotal} {t("条匹配诊断日志")}
            </div>
            <div className="flex items-center gap-6">
              <div className="flex items-center gap-2">
                <span className="whitespace-nowrap text-xs text-muted-foreground">
                  {t("每页显示")}
                </span>
                <Select
                  value={gatewayPageSize}
                  onValueChange={(value) => {
                    setGatewayPageSize(value || "10");
                    setGatewayPage(1);
                  }}
                >
                  <SelectTrigger className="h-8 w-[78px] text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {["10", "20", "50", "100"].map((value) => (
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
                  disabled={gatewayCurrentPage <= 1}
                  onClick={() =>
                    setGatewayPage(Math.max(1, gatewayCurrentPage - 1))
                  }
                >
                  {t("上一页")}
                </Button>
                <div className="min-w-[68px] text-center text-xs font-medium">
                  {t("第")} {gatewayCurrentPage} / {gatewayTotalPages} {t("页")}
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 px-3 text-xs"
                  disabled={gatewayCurrentPage >= gatewayTotalPages}
                  onClick={() =>
                    setGatewayPage(
                      Math.min(gatewayTotalPages, gatewayCurrentPage + 1),
                    )
                  }
                >
                  {t("下一页")}
                </Button>
              </div>
            </div>
          </div>
        </TabsContent>
      </Tabs>

      <ConfirmDialog
        open={clearConfirmOpen}
        onOpenChange={setClearConfirmOpen}
        title={t("清空请求日志")}
        description={t("确定清空全部请求日志吗？该操作不可恢复。")}
        confirmText={t("清空")}
        confirmVariant="destructive"
        onConfirm={() => clearMutation.mutate()}
      />
      <ConfirmDialog
        open={clearGatewayConfirmOpen}
        onOpenChange={setClearGatewayConfirmOpen}
        title={t("清空网关诊断日志")}
        description={t("确定清空全部网关错误诊断日志吗？该操作不可恢复。")}
        confirmText={t("清空")}
        confirmVariant="destructive"
        onConfirm={() => clearGatewayMutation.mutate()}
      />
    </div>
  );
}

export default function LogsPage() {
  return (
    <Suspense fallback={<LogsPageSkeleton />}>
      <LogsPageContent />
    </Suspense>
  );
}
