"use client";

import { useEffect, useMemo, useState } from "react";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button, buttonVariants } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { accountClient } from "@/lib/api/account-client";
import { useAppStore } from "@/lib/store/useAppStore";
import { useI18n } from "@/lib/i18n/provider";
import { copyTextToClipboard } from "@/lib/utils/clipboard";
import { findBestMatchingModel } from "@/lib/api/model-catalog";
import { toast } from "sonner";
import { useQueryClient, useQuery } from "@tanstack/react-query";
import { Key, Clipboard, ShieldCheck } from "lucide-react";
import { ApiKey } from "@/types";

const PROTOCOL_LABELS: Record<string, string> = {
  openai_compat: "通配兼容 (Codex / Claude Code / Gemini CLI)",
  anthropic_native: "通配兼容 (Codex / Claude Code / Gemini CLI)",
  gemini_native: "通配兼容 (Codex / Claude Code / Gemini CLI)",
};

const REASONING_LABELS: Record<string, string> = {
  auto: "跟随请求",
  low: "低 (low)",
  medium: "中 (medium)",
  high: "高 (high)",
  xhigh: "极高 (xhigh)",
};

const SERVICE_TIER_LABELS: Record<string, string> = {
  auto: "跟随请求",
  fast: "Fast",
};

function normalizeEditableServiceTier(value?: string | null): string {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "fast" ? "fast" : "";
}

const ROTATION_STRATEGY_LABELS: Record<string, string> = {
  account_rotation: "账号轮转",
  aggregate_api_rotation: "聚合API轮转",
  hybrid_rotation: "混合轮转（账号优先）",
};

const ACCOUNT_PLAN_FILTER_LABELS: Record<string, string> = {
  all: "全部账号",
  free: "Free",
  go: "Go",
  plus: "Plus",
  pro: "Pro",
  team: "Team",
  business: "Business",
  enterprise: "Enterprise",
  edu: "Edu",
  unknown: "未知计划",
};

interface ApiKeyModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  apiKey?: ApiKey | null;
}

/**
 * 函数 `ApiKeyModal`
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
export function ApiKeyModal({ open, onOpenChange, apiKey }: ApiKeyModalProps) {
  const { t } = useI18n();
  const serviceStatus = useAppStore((state) => state.serviceStatus);
  const { canAccessManagementRpc } = useRuntimeCapabilities();
  const [name, setName] = useState("");
  const [protocolType, setProtocolType] = useState("openai_compat");
  const [modelSlug, setModelSlug] = useState("");
  const [reasoningEffort, setReasoningEffort] = useState("");
  const [serviceTier, setServiceTier] = useState("");
  const [rotationStrategy, setRotationStrategy] = useState("account_rotation");
  const [accountPlanFilter, setAccountPlanFilter] = useState("all");
  const [upstreamBaseUrl, setUpstreamBaseUrl] = useState("");
  const [generatedKey, setGeneratedKey] = useState("");

  const [isLoading, setIsLoading] = useState(false);
  const queryClient = useQueryClient();
  const isServiceReady = canAccessManagementRpc && serviceStatus.connected;
  const usesAccountPlanFilter =
    rotationStrategy === "account_rotation" ||
    rotationStrategy === "hybrid_rotation";
  const unavailableMessage = canAccessManagementRpc
    ? t("服务未连接，平台密钥与模型配置暂不可编辑；连接恢复后可继续操作。")
    : t("当前运行环境暂不支持平台密钥管理。");

  const { data: models } = useQuery({
    queryKey: ["apikey-models"],
    queryFn: async () => {
      const cached = await accountClient.listModels(false);
      if (cached.models.length > 0) {
        return cached;
      }
      try {
        return await accountClient.listModels(true);
      } catch {
        return cached;
      }
    },
    enabled: open && isServiceReady,
  });

  const selectedModelInfo = useMemo(
    () => findBestMatchingModel(models?.models || [], modelSlug),
    [modelSlug, models?.models],
  );

  const visibleModels = useMemo(() => {
    const catalog = models?.models || [];
    const selectedSlug = String(modelSlug || "").trim();
    const baseModels = catalog.filter((model) => {
      if (model.supportedInApi) {
        return true;
      }
      return Boolean(selectedSlug) && model.slug === selectedModelInfo?.slug;
    });
    if (selectedModelInfo && selectedModelInfo.slug !== selectedSlug) {
      return [
        {
          ...selectedModelInfo,
          slug: selectedSlug,
          displayName: selectedModelInfo.displayName || selectedSlug,
        },
        ...baseModels,
      ];
    }
    return baseModels;
  }, [models?.models, selectedModelInfo]);

  const modelLabelMap = Object.fromEntries(
    visibleModels.map((model) => [model.slug, model.displayName || model.slug]),
  );

  useEffect(() => {
    if (!open) return;

    if (!apiKey) {
      setName("");
      setProtocolType("openai_compat");
      setModelSlug("");
      setReasoningEffort("");
      setServiceTier("");
      setRotationStrategy("account_rotation");
      setAccountPlanFilter("all");
      setUpstreamBaseUrl("");
      setGeneratedKey("");
      return;
    }

    setName(apiKey.name || "");
    setProtocolType("openai_compat");
    setModelSlug(apiKey.modelSlug || "");
    setReasoningEffort(apiKey.reasoningEffort || "");
    setServiceTier(normalizeEditableServiceTier(apiKey.serviceTier));
    setRotationStrategy(apiKey.rotationStrategy || "account_rotation");
    setAccountPlanFilter(apiKey.accountPlanFilter || "all");
    setGeneratedKey("");
    setUpstreamBaseUrl(apiKey.upstreamBaseUrl || "");
  }, [apiKey, open]);

  /**
   * 函数 `handleSave`
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
  const handleSave = async () => {
    if (!isServiceReady) {
      toast.info(
        canAccessManagementRpc
          ? t("服务未连接，暂时无法保存平台密钥")
          : t("当前运行环境暂不支持平台密钥管理"),
      );
      return;
    }
    setIsLoading(true);
    try {
      const params = {
        name: name || null,
        modelSlug: !modelSlug || modelSlug === "auto" ? null : modelSlug,
        reasoningEffort:
          !reasoningEffort || reasoningEffort === "auto"
            ? null
            : reasoningEffort,
        serviceTier:
          !serviceTier || serviceTier === "auto" ? null : serviceTier,
        protocolType,
        upstreamBaseUrl: upstreamBaseUrl || null,
        staticHeadersJson: null,
        rotationStrategy,
        accountPlanFilter:
          usesAccountPlanFilter && accountPlanFilter !== "all"
            ? accountPlanFilter
            : null,
      };

      if (apiKey?.id) {
        await accountClient.updateApiKey(apiKey.id, params);
        toast.success(t("密钥配置已更新"));
      } else {
        const result = await accountClient.createApiKey(params);
        setGeneratedKey(result.key);
        toast.success(t("平台密钥已创建"));
      }

      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["apikeys"] }),
        queryClient.invalidateQueries({ queryKey: ["apikey-models"] }),
        queryClient.invalidateQueries({ queryKey: ["startup-snapshot"] }),
      ]);
      if (apiKey?.id) onOpenChange(false);
    } catch (err: unknown) {
      toast.error(
        `操作失败: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * 函数 `copyKey`
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
  const copyKey = async () => {
    try {
      await copyTextToClipboard(generatedKey);
      toast.success(t("密钥已复制"));
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : String(error));
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[calc(100%-2rem)] max-w-[calc(100%-2rem)] sm:max-w-[680px] md:max-w-[760px] max-h-[90vh] overflow-y-auto glass-card border-none">
        <DialogHeader>
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 rounded-full bg-primary/10">
              <Key className="h-5 w-5 text-primary" />
            </div>
            <DialogTitle>
              {apiKey?.id ? t("编辑平台密钥") : t("创建平台密钥")}
            </DialogTitle>
          </div>
          <DialogDescription>
            {t("配置网关访问凭据，您可以绑定特定模型、推理等级或自定义上游。")}
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-5 py-4">
          {!isServiceReady ? (
            <div className="rounded-lg border border-border/60 bg-muted/30 p-3 text-sm text-muted-foreground">
              {unavailableMessage}
            </div>
          ) : null}
          <div className="grid grid-cols-2 gap-4 items-start">
            <div className="grid gap-2 content-start">
              <Label htmlFor="name">{t("密钥名称 (可选)")}</Label>
              <Input
                id="name"
                placeholder={t("例如：主机房 / 测试")}
                value={name}
                disabled={!isServiceReady}
                onChange={(e) => setName(e.target.value)}
              />
            </div>
            <div className="grid gap-2 content-start">
              <Label>{t("轮转策略")}</Label>
              <Select
                value={rotationStrategy}
                onValueChange={(val) => {
                  if (!val) return;
                  setRotationStrategy(val);
                }}
                disabled={!isServiceReady}
              >
                <SelectTrigger className="w-full">
                  <SelectValue>
                    {(value) =>
                      t(ROTATION_STRATEGY_LABELS[String(value || "")] || "账号轮转")
                    }
                  </SelectValue>
                </SelectTrigger>
                <SelectContent align="start">
                  <SelectItem value="account_rotation">{t("账号轮转")}</SelectItem>
                  <SelectItem value="aggregate_api_rotation">
                    {t("聚合API轮转")}
                  </SelectItem>
                  <SelectItem value="hybrid_rotation">
                    {t("混合轮转（账号优先）")}
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>
            <p className="col-span-2 -mt-1 text-[11px] text-muted-foreground">
              {t(
                "账号轮转只走账号池；聚合API轮转只走聚合API；混合轮转先走账号池，账号耗尽后使用聚合API兜底。",
              )}
            </p>
          </div>

          {usesAccountPlanFilter ? (
            <div className="grid gap-2">
              <Label>{t("账号组筛选")}</Label>
              <Select
                value={accountPlanFilter}
                onValueChange={(val) => val && setAccountPlanFilter(val)}
                disabled={!isServiceReady}
              >
                <SelectTrigger className="w-full">
                  <SelectValue>
                    {(value) =>
                      t(
                        ACCOUNT_PLAN_FILTER_LABELS[String(value || "")] ||
                          "全部账号",
                      )
                    }
                  </SelectValue>
                </SelectTrigger>
                <SelectContent align="start">
                  {Object.entries(ACCOUNT_PLAN_FILTER_LABELS).map(
                    ([value, label]) => (
                      <SelectItem key={value} value={value}>
                        {t(label)}
                      </SelectItem>
                    ),
                  )}
                </SelectContent>
              </Select>
              <p className="text-[11px] text-muted-foreground">
                {t(
                  "仅对账号轮转和混合轮转生效，可限制这把平台密钥只从指定账号计划类型中选路由账号。",
                )}
              </p>
            </div>
          ) : null}

          <div className="grid grid-cols-2 gap-4">
            <div className="grid gap-2 content-start">
              <Label>{t("协议类型")}</Label>
              <Select
                value={protocolType}
                onValueChange={(val) => val && setProtocolType(val)}
                disabled={!isServiceReady}
              >
                <SelectTrigger className="w-full">
                  <SelectValue>
                    {(value) =>
                      t(
                        PROTOCOL_LABELS[String(value || "")] ||
                          "通配兼容 (Codex / Claude Code / Gemini CLI)",
                      )
                    }
                  </SelectValue>
                </SelectTrigger>
                <SelectContent align="start">
                  <SelectItem value="openai_compat">
                    {t("通配兼容 (Codex / Claude Code / Gemini CLI)")}
                  </SelectItem>
                </SelectContent>
              </Select>
              <p className="min-h-[32px] text-[11px] text-muted-foreground">
                {t("默认按路径通配：")}<code>/v1/messages*</code> {t("走 Claude 语义，")}<code>/v1beta/models/*:generateContent</code> {t("这类路径走 Gemini 语义，其它标准路径走 Codex / OpenAI 语义。")}
              </p>
            </div>
            <div className="grid gap-2 content-start">
              <Label>{t("绑定模型 (可选)")}</Label>
              <Select
                value={modelSlug}
                onValueChange={(val) => val && setModelSlug(val)}
                disabled={!isServiceReady}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder={t("跟随请求")}>
                    {(value) => {
                      const nextValue = String(value || "").trim();
                      if (!nextValue || nextValue === "auto") return t("跟随请求");
                      const resolvedModel = findBestMatchingModel(
                        models?.models || [],
                        nextValue,
                      );
                      return resolvedModel?.displayName || modelLabelMap[nextValue] || nextValue;
                    }}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent align="start">
                  <SelectItem value="auto">{t("跟随请求")}</SelectItem>
                  {visibleModels.map((model) => (
                    <SelectItem key={model.slug} value={model.slug}>
                      {model.displayName || model.slug}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-[11px] text-muted-foreground">
                {t("选择“跟随请求”时，会使用请求体里的实际模型；请求日志展示的是最终生效模型。")}
              </p>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="grid gap-2 content-start">
              <Label>{t("推理等级 (可选)")}</Label>
              <Select
                value={reasoningEffort}
                onValueChange={(val) => val && setReasoningEffort(val)}
                disabled={!isServiceReady}
              >
                <SelectTrigger className="w-full">
                    <SelectValue placeholder={t("跟随请求等级")}>
                    {(value) => {
                      const nextValue = String(value || "").trim();
                      if (!nextValue) return t("跟随请求等级");
                      return t(REASONING_LABELS[nextValue] || nextValue);
                    }}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent align="start">
                  <SelectItem value="auto">{t("跟随请求")}</SelectItem>
                  <SelectItem value="low">{t("低 (low)")}</SelectItem>
                  <SelectItem value="medium">{t("中 (medium)")}</SelectItem>
                  <SelectItem value="high">{t("高 (high)")}</SelectItem>
                  <SelectItem value="xhigh">{t("极高 (xhigh)")}</SelectItem>
                </SelectContent>
              </Select>
              <p className="min-h-[32px] text-[11px] text-muted-foreground">
                {t("会覆盖请求里的 reasoning effort。")}
              </p>
            </div>
            <div className="grid gap-2 content-start">
              <Label>{t("服务等级 (可选)")}</Label>
              <Select
                value={serviceTier}
                onValueChange={(val) => val && setServiceTier(val)}
                disabled={!isServiceReady}
              >
                <SelectTrigger className="w-full">
                    <SelectValue placeholder={t("跟随请求")}>
                    {(value) => {
                      const nextValue = String(value || "").trim();
                      if (!nextValue) return t("跟随请求");
                      return t(SERVICE_TIER_LABELS[nextValue] || nextValue);
                    }}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent align="start">
                  <SelectItem value="auto">{t("跟随请求")}</SelectItem>
                  <SelectItem value="fast">Fast</SelectItem>
                </SelectContent>
              </Select>
              <p className="text-[11px] text-muted-foreground">
                {t("Fast 会映射为上游 priority；未设置时跟随请求。")}
              </p>
            </div>
          </div>

          {generatedKey && (
            <div className="space-y-2 pt-4 border-t">
              <Label className="text-xs text-primary flex items-center gap-1.5">
                <ShieldCheck className="h-3.5 w-3.5" /> {t("平台密钥已生成")}
              </Label>
              <div className="flex gap-2">
                <Input
                  value={generatedKey}
                  readOnly
                  className="font-mono text-sm bg-primary/5"
                />
                <Button
                  variant="outline"
                  onClick={() => void copyKey()}
                  disabled={!generatedKey}
                >
                  <Clipboard className="h-4 w-4" />
                </Button>
              </div>
            </div>
          )}
        </div>

        <DialogFooter>
          <DialogClose
            className={buttonVariants({ variant: "ghost" })}
            type="button"
          >
            {generatedKey ? t("关闭") : t("取消")}
          </DialogClose>
          {!generatedKey && (
            <Button
              onClick={handleSave}
              disabled={!isServiceReady || isLoading}
            >
              {isLoading ? t("保存中...") : t("完成")}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
