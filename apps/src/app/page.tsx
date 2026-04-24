"use client";

import type { ReactNode } from "react";
import {
  Activity,
  BrainCircuit,
  CheckCircle2,
  Database,
  DollarSign,
  LoaderCircle,
  PieChart,
  Users,
  XCircle,
  Zap,
  type LucideIcon,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { useCodexAccountSwitcher } from "@/hooks/useCodexAccountSwitcher";
import { useDashboardStats } from "@/hooks/useDashboardStats";
import { usePageTransitionReady } from "@/hooks/usePageTransitionReady";
import { useI18n } from "@/lib/i18n/provider";
import { cn } from "@/lib/utils";
import { formatCompactNumber } from "@/lib/utils/usage";

interface StatProgressCardProps {
  title: string;
  value: number;
  total: number;
  icon: LucideIcon;
  color: string;
  sub: string;
}

interface PercentBarProps {
  label: string;
  value: number | null | undefined;
  tone?: "default" | "green" | "blue";
}

interface AccountHighlightCardProps {
  title: string;
  name: string;
  subtitle: string;
  tone?: "green" | "blue";
  progressLabel?: string;
  progressValue?: number | null | undefined;
  action?: ReactNode;
}

/**
 * 函数 `formatPercent`
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
function formatPercent(value: number | null | undefined): string {
  return value == null ? "--" : `${Math.max(0, Math.round(value))}%`;
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
    return normalized.toLocaleString("zh-CN", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }
  return formatCompactNumber(normalized, "0.00", 2, true);
}

/**
 * 函数 `PercentBar`
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
function PercentBar({ label, value, tone = "default" }: PercentBarProps) {
  const normalized = value == null ? 0 : Math.max(0, Math.min(100, Math.round(value)));
  const colorClass =
    tone === "green"
      ? "bg-green-500"
      : tone === "blue"
        ? "bg-blue-500"
        : "bg-primary";

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between text-[10px]">
        <span className="text-muted-foreground">{label}</span>
        <span className="font-semibold">{formatPercent(value)}</span>
      </div>
      <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted/60">
        <div
          className={cn("h-full rounded-full transition-all", colorClass)}
          style={{ width: `${normalized}%` }}
        />
      </div>
    </div>
  );
}

/**
 * 函数 `quotaTrackClass`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - tone: 参数 tone
 *
 * # 返回
 * 返回函数执行结果
 */
function quotaTrackClass(tone: "green" | "blue") {
  return tone === "blue" ? "bg-blue-500/20" : "bg-green-500/20";
}

/**
 * 函数 `quotaIndicatorClass`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - tone: 参数 tone
 *
 * # 返回
 * 返回函数执行结果
 */
function quotaIndicatorClass(tone: "green" | "blue") {
  return tone === "blue" ? "bg-blue-500" : "bg-green-500";
}

/**
 * 函数 `AccountHighlightCard`
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
function AccountHighlightCard({
  title,
  name,
  subtitle,
  tone = "green",
  progressLabel,
  progressValue,
  action,
}: AccountHighlightCardProps) {
  const iconToneClass =
    tone === "blue"
      ? "bg-blue-500/20 text-blue-500"
      : "bg-green-500/20 text-green-500";

  return (
    <div className="rounded-2xl border border-border/40 bg-accent/20 p-4 shadow-sm">
      <div className="flex items-center gap-4">
        <div
          className={cn(
            "flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl",
            iconToneClass,
          )}
        >
          <CheckCircle2 className="h-5 w-5" />
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-[11px] font-medium text-muted-foreground">{title}</p>
          <p className="truncate text-sm font-semibold leading-5">{name}</p>
          <p className="truncate text-xs text-muted-foreground">{subtitle}</p>
        </div>
      </div>
      {progressLabel ? (
        <div className="mt-3 border-t border-border/40 pt-3">
          <PercentBar label={progressLabel} value={progressValue} tone={tone} />
        </div>
      ) : null}
      {action ? <div className="mt-3 flex justify-end">{action}</div> : null}
    </div>
  );
}

/**
 * 函数 `StatProgressCard`
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
function StatProgressCard({
  title,
  value,
  total,
  icon: Icon,
  color,
  sub,
}: StatProgressCardProps) {
  const { t } = useI18n();
  const percentage = total > 0 ? Math.min(Math.round((value / total) * 100), 100) : 0;

  return (
    <Card className="glass-card overflow-hidden border-none shadow-md backdrop-blur-md transition-all hover:scale-[1.02]">
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium">{title}</CardTitle>
        <Icon className={cn("h-4 w-4", color)} />
      </CardHeader>
      <CardContent className="space-y-3">
        <div>
          <div className="text-2xl font-bold">{value}</div>
          <p className="mt-1 text-[10px] text-muted-foreground">{sub}</p>
        </div>
        <div className="space-y-1">
          <div className="flex items-center justify-between text-[10px]">
            <span className="text-muted-foreground">{t("占比")}</span>
            <span className="font-mono font-medium">{percentage}%</span>
          </div>
          <Progress value={percentage} className="h-1.5" />
        </div>
      </CardContent>
    </Card>
  );
}

export default function DashboardPage() {
  const { t } = useI18n();
  const { stats, currentAccount, recommendations, requestLogs, isLoading, isServiceReady } =
    useDashboardStats();
  const {
    canSwitchCodexAccount,
    switchCodexAccount,
    isSwitchingCodexAccount,
    switchingCodexAccountId,
  } = useCodexAccountSwitcher();
  usePageTransitionReady("/", !isServiceReady || !isLoading);
  const poolPrimary = stats.poolRemain?.primary ?? 0;
  const poolSecondary = stats.poolRemain?.secondary ?? 0;

  return (
    <div className="space-y-6 animate-in fade-in duration-700">
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {isLoading ? (
          Array.from({ length: 4 }).map((_, index) => (
            <Skeleton key={index} className="h-36 w-full rounded-2xl" />
          ))
        ) : (
          <>
            <Card className="glass-card overflow-hidden border-none shadow-md backdrop-blur-md transition-all hover:scale-[1.02]">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">{t("总账号数")}</CardTitle>
                <Users className="h-4 w-4 text-blue-500" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{stats.total}</div>
                <p className="mt-1 text-[10px] text-muted-foreground">{t("池中所有配置账号")}</p>
                <div className="mt-4 flex w-fit items-center gap-2 rounded-full bg-blue-500/10 px-2 py-0.5 text-[10px] text-blue-600 dark:text-blue-400">
                  <Activity className="h-3 w-3" />
                  {t("最近日志")} {requestLogs.length} {t("条")}
                </div>
              </CardContent>
            </Card>

            <StatProgressCard
              title={t("可用账号")}
              value={stats.available}
              total={stats.total}
              icon={CheckCircle2}
              color="text-green-500"
              sub={t("当前健康可调用的账号")}
            />

            <StatProgressCard
              title={t("不可用账号")}
              value={stats.unavailable}
              total={stats.total}
              icon={XCircle}
              color="text-red-500"
              sub={t("额度耗尽或授权失效")}
            />

            <Card className="overflow-hidden border-none bg-primary/10 shadow-md backdrop-blur-md transition-all hover:scale-[1.02]">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-primary">{t("账号池剩余")}</CardTitle>
                <PieChart className="h-4 w-4 text-primary" />
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-1.5">
                  <div className="flex items-center justify-between text-[10px]">
                    <span className="text-muted-foreground">{t("5小时内")}</span>
                    <span className="font-bold">{formatPercent(stats.poolRemain?.primary)}</span>
                  </div>
                  <Progress
                    value={poolPrimary}
                    trackClassName={quotaTrackClass("green")}
                    indicatorClassName={quotaIndicatorClass("green")}
                  />
                </div>
                <div className="space-y-1.5">
                  <div className="flex items-center justify-between text-[10px]">
                    <span className="text-muted-foreground">{t("7天内")}</span>
                    <span className="font-bold">{formatPercent(stats.poolRemain?.secondary)}</span>
                  </div>
                  <Progress
                    value={poolSecondary}
                    trackClassName={quotaTrackClass("blue")}
                    indicatorClassName={quotaIndicatorClass("blue")}
                  />
                </div>
              </CardContent>
            </Card>
          </>
        )}
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {[ 
          {
            title: t("今日Token"),
            value: formatCompactTokenAmount(stats.todayTokens),
            icon: Zap,
            color: "text-yellow-500",
            sub: t("输入 + 输出合计"),
          },
          {
            title: t("缓存Token"),
            value: formatCompactTokenAmount(stats.cachedTokens),
            icon: Database,
            color: "text-indigo-500",
            sub: t("上下文缓存命中"),
          },
          {
            title: t("推理Token"),
            value: formatCompactTokenAmount(stats.reasoningTokens),
            icon: BrainCircuit,
            color: "text-purple-500",
            sub: t("大模型思考过程"),
          },
          {
            title: t("预计费用"),
            value: `$${Number(stats.todayCost || 0).toFixed(2)}`,
            icon: DollarSign,
            color: "text-emerald-500",
            sub: t("按官价估算"),
          },
        ].map((card) => (
          isLoading ? (
            <Skeleton key={card.title} className="h-32 w-full rounded-2xl" />
          ) : (
            <Card
              key={card.title}
              className="glass-card overflow-hidden border-none shadow-md backdrop-blur-md transition-all hover:scale-[1.02]"
            >
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">{card.title}</CardTitle>
                <card.icon className={cn("h-4 w-4", card.color)} />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{card.value}</div>
                <p className="mt-1 text-[10px] text-muted-foreground">{card.sub}</p>
              </CardContent>
            </Card>
          )
        ))}
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <Card className="glass-card min-h-[300px] border-none shadow-md">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-base font-semibold">{t("当前活跃账号")}</CardTitle>
          </CardHeader>
          <CardContent className="flex min-h-[200px] flex-col justify-start">
            {isLoading ? (
              <div className="space-y-4">
                <Skeleton className="h-28 w-full rounded-2xl" />
                <div className="grid grid-cols-2 gap-4">
                  <Skeleton className="h-32 w-full rounded-xl" />
                  <Skeleton className="h-32 w-full rounded-xl" />
                </div>
              </div>
            ) : currentAccount ? (
              <div className="space-y-4">
                <AccountHighlightCard
                  title={t("当前活跃账号")}
                  name={currentAccount.name}
                  subtitle={currentAccount.id}
                  tone="green"
                />
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div className="space-y-3 rounded-xl bg-muted/30 p-4">
                    <p className="text-xs text-muted-foreground">{t("5小时剩余")}</p>
                    <p className="text-lg font-bold">{formatPercent(currentAccount.primaryRemainPercent)}</p>
                    <PercentBar label={t("剩余额度")} value={currentAccount.primaryRemainPercent} tone="green" />
                  </div>
                  <div className="space-y-3 rounded-xl bg-muted/30 p-4">
                    <p className="text-xs text-muted-foreground">{t("7天剩余")}</p>
                    <p className="text-lg font-bold">{formatPercent(currentAccount.secondaryRemainPercent)}</p>
                    <PercentBar label={t("剩余额度")} value={currentAccount.secondaryRemainPercent} tone="blue" />
                  </div>
                </div>
              </div>
            ) : (
              <div className="flex h-full flex-col items-center justify-center gap-2 text-sm text-muted-foreground">
                <div className="rounded-full bg-accent/30 p-4 animate-pulse">
                  <Activity className="h-8 w-8 opacity-20" />
                </div>
                <p>{isServiceReady ? t("暂无可识别的活跃账号") : t("正在等待服务连接")}</p>
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="glass-card min-h-[300px] border-none shadow-md">
          <CardHeader>
            <CardTitle className="text-base font-semibold">{t("智能推荐")}</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <p className="text-xs text-muted-foreground">
              {t("基于当前配额，系统会优先推荐剩余额度更高且仍可参与路由的账号。")}
            </p>
            {isLoading ? (
              <div className="space-y-4">
                <Skeleton className="h-28 w-full rounded-2xl" />
                <Skeleton className="h-28 w-full rounded-2xl" />
              </div>
            ) : recommendations.primaryPick || recommendations.secondaryPick ? (
              <>
                {recommendations.primaryPick ? (
                  <AccountHighlightCard
                    title={t("5小时优先账号")}
                    name={recommendations.primaryPick.name}
                    subtitle={recommendations.primaryPick.id}
                    tone="green"
                    progressLabel={t("剩余额度")}
                    progressValue={recommendations.primaryPick.primaryRemainPercent}
                    action={
                      canSwitchCodexAccount ? (
                        <Button
                          size="sm"
                          onClick={() => switchCodexAccount(recommendations.primaryPick!.id)}
                          disabled={isSwitchingCodexAccount}
                        >
                          {switchingCodexAccountId === recommendations.primaryPick.id ? (
                            <LoaderCircle className="animate-spin" />
                          ) : null}
                          {switchingCodexAccountId === recommendations.primaryPick.id
                            ? "切号中..."
                            : "一键切号"}
                        </Button>
                      ) : null
                    }
                  />
                ) : null}
                {recommendations.secondaryPick ? (
                  <AccountHighlightCard
                    title={t("7天优先账号")}
                    name={recommendations.secondaryPick.name}
                    subtitle={recommendations.secondaryPick.id}
                    tone="blue"
                    progressLabel={t("剩余额度")}
                    progressValue={recommendations.secondaryPick.secondaryRemainPercent}
                    action={
                      canSwitchCodexAccount ? (
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => switchCodexAccount(recommendations.secondaryPick!.id)}
                          disabled={isSwitchingCodexAccount}
                        >
                          {switchingCodexAccountId === recommendations.secondaryPick.id ? (
                            <LoaderCircle className="animate-spin" />
                          ) : null}
                          {switchingCodexAccountId === recommendations.secondaryPick.id
                            ? "切号中..."
                            : "一键切号"}
                        </Button>
                      ) : null
                    }
                  />
                ) : null}
              </>
            ) : (
              <div className="rounded-xl bg-accent/20 p-4 text-sm text-muted-foreground">
                {isServiceReady ? t("当前没有可推荐的可用账号。") : t("正在等待服务连接。")}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
