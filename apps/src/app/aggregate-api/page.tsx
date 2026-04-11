"use client";

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ArrowUp,
  Copy,
  Eye,
  EyeOff,
  MoreVertical,
  Plus,
  RefreshCw,
  Settings2,
  ShieldCheck,
  Trash2,
} from "lucide-react";
import { toast } from "sonner";
import { AggregateApiModal } from "@/components/modals/aggregate-api-modal";
import { ConfirmDialog } from "@/components/modals/confirm-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { accountClient } from "@/lib/api/account-client";
import { copyTextToClipboard } from "@/lib/utils/clipboard";
import { formatTsFromSeconds } from "@/lib/utils/usage";
import { useAppStore } from "@/lib/store/useAppStore";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { useDeferredDesktopActivation } from "@/hooks/useDeferredDesktopActivation";
import { usePageTransitionReady } from "@/hooks/usePageTransitionReady";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { useI18n } from "@/lib/i18n/provider";
import { AggregateApi, AggregateApiSecretResult } from "@/types";

type TranslateFn = (key: string, values?: Record<string, string | number>) => string;

const AGGREGATE_API_PROVIDER_LABELS: Record<string, string> = {
  codex: "Codex",
  claude: "Claude",
};

const AGGREGATE_API_PROVIDER_FILTER_LABELS: Record<string, string> = {
  all: "全部类型",
  codex: "Codex",
  claude: "Claude",
};

/**
 * 函数 `getTestBadge`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - api: 参数 api
 *
 * # 返回
 * 返回函数执行结果
 */
function getTestBadge(api: AggregateApi, t: TranslateFn) {
  if (api.lastTestStatus === "success") {
    return (
      <Badge className="border-green-500/20 bg-green-500/10 text-green-500">
        {t("已连通")}
      </Badge>
    );
  }
  if (api.lastTestStatus === "failed") {
    return (
      <Badge className="border-red-500/20 bg-red-500/10 text-red-500">
        {t("失败")}
      </Badge>
    );
  }
  return <Badge variant="secondary">{t("未测试")}</Badge>;
}

export default function AggregateApiPage() {
  const { t } = useI18n();
  const queryClient = useQueryClient();
  const serviceStatus = useAppStore((state) => state.serviceStatus);
  const { canAccessManagementRpc } = useRuntimeCapabilities();
  const isServiceReady = canAccessManagementRpc && serviceStatus.connected;
  const isPageActive = useDesktopPageActive("/aggregate-api/");
  const isQueryEnabled = useDeferredDesktopActivation(isServiceReady);
  const [modalOpen, setModalOpen] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [deleteId, setDeleteId] = useState<string | null>(null);
  const [providerFilter, setProviderFilter] = useState("all");
  const [revealedSecrets, setRevealedSecrets] = useState<
    Record<string, AggregateApiSecretResult>
  >({});
  const [loadingSecretId, setLoadingSecretId] = useState<string | null>(null);
  const [testingApiId, setTestingApiId] = useState<string | null>(null);
  const [togglingApiId, setTogglingApiId] = useState<string | null>(null);

  const { data: aggregateApis = [], isLoading } = useQuery({
    queryKey: ["aggregate-apis"],
    queryFn: () => accountClient.listAggregateApis(),
    enabled: isQueryEnabled,
    retry: 1,
  });

  usePageTransitionReady("/aggregate-api/", !isServiceReady || !isLoading);

  useEffect(() => {
    if (isPageActive) return;
    setModalOpen(false);
    setEditingId(null);
    setDeleteId(null);
  }, [isPageActive]);

  const editingApi = useMemo(
    () => aggregateApis.find((item) => item.id === editingId) || null,
    [aggregateApis, editingId],
  );

  const filteredAggregateApis = useMemo(() => {
    if (providerFilter === "all") {
      return aggregateApis;
    }
    return aggregateApis.filter((api) => api.providerType === providerFilter);
  }, [aggregateApis, providerFilter]);

  const defaultCreateSort = useMemo(() => {
    const maxSort = aggregateApis.reduce(
      (max, api) => Math.max(max, Number(api.sort) || 0),
      0,
    );
    return maxSort + 5;
  }, [aggregateApis]);

  /**
   * 函数 `renderTestStatus`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - api: 参数 api
   *
   * # 返回
   * 返回函数执行结果
   */
  const renderTestStatus = (api: AggregateApi) => {
    const badge = getTestBadge(api, t);
    if (api.lastTestStatus !== "failed" || !api.lastTestError) {
      return badge;
    }

    return (
      <Tooltip>
        <TooltipTrigger render={<div />} className="inline-flex cursor-help">
          {badge}
        </TooltipTrigger>
        <TooltipContent className="max-w-sm whitespace-pre-wrap break-words">
          {api.lastTestError}
        </TooltipContent>
      </Tooltip>
    );
  };

  const testMutation = useMutation({
    mutationFn: (apiId: string) =>
      accountClient.testAggregateApiConnection(apiId),
    onMutate: async (apiId) => {
      setTestingApiId(apiId);
    },
    onSuccess: async (result) => {
      if (result.ok) {
        toast.success(t("已连通"));
        return;
      }
      toast.error(
        t("连通性测试失败: {reason}", {
          reason: result.message || result.statusCode || t("未返回具体错误信息"),
        }),
      );
    },
    onSettled: async (_result, _error, apiId) => {
      await queryClient.invalidateQueries({ queryKey: ["aggregate-apis"] });
      setTestingApiId((current) => (current === apiId ? null : current));
    },
    onError: (error: unknown) => {
      toast.error(`${t("测试")} ${t("失败")}: ${error instanceof Error ? error.message : String(error)}`);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (apiId: string) => accountClient.deleteAggregateApi(apiId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["aggregate-apis"] });
      await queryClient.invalidateQueries({ queryKey: ["apikeys"] });
      await queryClient.invalidateQueries({ queryKey: ["startup-snapshot"] });
      toast.success(`${t("聚合API")} ${t("删除")}`);
    },
    onError: (error: unknown) => {
      toast.error(`${t("删除")} ${t("失败")}: ${error instanceof Error ? error.message : String(error)}`);
    },
  });

  const prioritizeMutation = useMutation({
    mutationFn: async (api: AggregateApi) => {
      const currentMinSort = aggregateApis.reduce(
        (min, item) => Math.min(min, Number(item.sort) || 0),
        Number(api.sort) || 0,
      );
      const nextSort =
        (Number(api.sort) || 0) <= currentMinSort ? currentMinSort : currentMinSort - 5;

      if ((Number(api.sort) || 0) === nextSort) {
        return false;
      }

      await accountClient.updateAggregateApi(api.id, {
        providerType: api.providerType,
        supplierName: api.supplierName || "",
        sort: nextSort,
        url: api.url,
        key: null,
      });
      return true;
    },
    onSuccess: async (changed) => {
      if (!changed) {
        toast.info(t("设为优先"));
        return;
      }
      await queryClient.invalidateQueries({ queryKey: ["aggregate-apis"] });
      toast.success(t("设为优先"));
    },
    onError: (error: unknown) => {
      toast.error(`${t("设为优先")} ${t("失败")}: ${error instanceof Error ? error.message : String(error)}`);
    },
  });

  const toggleStatusMutation = useMutation({
    mutationFn: async ({
      api,
      enabled,
    }: {
      api: AggregateApi;
      enabled: boolean;
    }) => {
      await accountClient.updateAggregateApi(api.id, {
        supplierName: api.supplierName || api.url,
        status: enabled ? "active" : "disabled",
      });
      return enabled;
    },
    onMutate: async ({ api }) => {
      setTogglingApiId(api.id);
    },
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["aggregate-apis"] }),
        queryClient.invalidateQueries({ queryKey: ["apikeys"] }),
        queryClient.invalidateQueries({ queryKey: ["startup-snapshot"] }),
      ]);
      toast.success(t("状态已更新"));
    },
    onError: (error: unknown) => {
      toast.error(
        `${t("更新状态失败")}: ${error instanceof Error ? error.message : String(error)}`,
      );
    },
    onSettled: async (_result, _error, variables) => {
      setTogglingApiId((current) =>
        current === variables.api.id ? null : current,
      );
    },
  });

  /**
   * 函数 `openCreateModal`
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
  const openCreateModal = () => {
    setEditingId(null);
    setModalOpen(true);
  };

  /**
   * 函数 `openEditModal`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - apiId: 参数 apiId
   *
   * # 返回
   * 返回函数执行结果
   */
  const openEditModal = (apiId: string) => {
    setEditingId(apiId);
    setModalOpen(true);
  };

  /**
   * 函数 `ensureSecretLoaded`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - apiId: 参数 apiId
   *
   * # 返回
   * 返回函数执行结果
   */
  const ensureSecretLoaded = async (apiId: string) => {
    if (revealedSecrets[apiId]) {
      return revealedSecrets[apiId];
    }
    setLoadingSecretId(apiId);
    try {
      const secretResult = await accountClient.readAggregateApiSecret(apiId);
      const authType = String(secretResult.authType || "").trim().toLowerCase();
      if (authType === "userpass") {
        if (!secretResult.username || !secretResult.password) {
          throw new Error(t("后端未返回账号密码明文"));
        }
      } else if (!secretResult.key) {
        throw new Error(t("后端未返回密钥明文"));
      }
      setRevealedSecrets((current) => ({ ...current, [apiId]: secretResult }));
      return secretResult;
    } finally {
      setLoadingSecretId(null);
    }
  };

  /**
   * 函数 `toggleSecret`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - apiId: 参数 apiId
   *
   * # 返回
   * 返回函数执行结果
   */
  const toggleSecret = async (apiId: string) => {
    if (revealedSecrets[apiId]) {
      setRevealedSecrets((current) => {
        const next = { ...current };
        delete next[apiId];
        return next;
      });
      return;
    }
    try {
      await ensureSecretLoaded(apiId);
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : String(error));
    }
  };

  /**
   * 函数 `copySecret`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - apiId: 参数 apiId
   *
   * # 返回
   * 返回函数执行结果
   */
  const copySecret = async (
    apiId: string,
    target: "key" | "username" | "password"
  ) => {
    try {
      const secret = await ensureSecretLoaded(apiId);
      const authType = String(secret.authType || "").trim().toLowerCase();
      const value =
        target === "username"
          ? secret.username
          : target === "password"
            ? secret.password
            : secret.key;
      if (authType === "userpass") {
        if (!value) {
          throw new Error(t("账号密码字段为空"));
        }
      } else if (!value) {
        throw new Error(t("密钥为空"));
      }
      await copyTextToClipboard(value);
      toast.success(t("已复制到剪贴板"));
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : String(error));
    }
  };

  const secretPreview = (secret: AggregateApiSecretResult) => {
    const authType = String(secret.authType || "").trim().toLowerCase();
    if (authType === "userpass") {
      return `${secret.username || ""}:${secret.password || ""}`;
    }
    return secret.key || "";
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      {!isServiceReady ? (
        <Card className="glass-card border-none shadow-sm">
          <CardContent className="pt-6 text-sm text-muted-foreground">
            {t("服务未连接")}
          </CardContent>
        </Card>
      ) : null}

      <div>
        <div>
          <p className="mt-1 text-sm text-muted-foreground">
            {t("管理上游聚合地址与密钥，并测试连通性")}
          </p>
        </div>
      </div>

      <div className="space-y-4">
        <Card className="glass-card border-none shadow-xl backdrop-blur-md">
          <CardContent className="px-4 ">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">{t("查询")}</span>
                <Select
                  value={providerFilter}
                  onValueChange={(value) => setProviderFilter(value || "all")}
                >
                  <SelectTrigger className="w-[160px]">
                    <SelectValue>
                      {(value) =>
                        t(
                          AGGREGATE_API_PROVIDER_FILTER_LABELS[
                            String(value || "")
                          ] || "全部类型",
                        )
                      }
                    </SelectValue>
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">{t("全部类型")}</SelectItem>
                    <SelectItem value="codex">Codex</SelectItem>
                    <SelectItem value="claude">Claude</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="flex items-center gap-3">
                <div className="text-xs text-muted-foreground">
                  {t("共")} {filteredAggregateApis.length} {t("条")}
                </div>
                <Button
                  className="h-10 gap-2 shadow-lg shadow-primary/20"
                  onClick={openCreateModal}
                  disabled={!isServiceReady}
                >
                  <Plus className="h-4 w-4" /> {t("新建聚合 API")}
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="glass-card overflow-hidden border-none py-0 shadow-xl backdrop-blur-md">
          <CardContent className="p-0">
            <Table className="w-full table-fixed">
              <TableHeader>
                <TableRow>
                  <TableHead className="max-w-[220px]">{t("供应商 / URL")}</TableHead>
                  <TableHead className="w-[84px] text-center">{t("类型")}</TableHead>
                  <TableHead className="w-[148px]">{t("密钥")}</TableHead>
                  <TableHead className="w-[64px] text-center">{t("顺序")}</TableHead>
                  <TableHead className="w-[130px]">{t("测试连通性")}</TableHead>
                  <TableHead className="w-[104px]">{t("状态")}</TableHead>
                  <TableHead className="text-center">{t("操作")}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  Array.from({ length: 3 }).map((_, index) => (
                    <TableRow key={index}>
                      <TableCell>
                        <Skeleton className="h-4 w-24" />
                      </TableCell>
                      <TableCell>
                        <Skeleton className="h-6 w-12 rounded-full" />
                      </TableCell>
                      <TableCell>
                        <Skeleton className="h-4 w-28" />
                      </TableCell>
                      <TableCell>
                        <Skeleton className="mx-auto h-4 w-12" />
                      </TableCell>
                      <TableCell>
                        <Skeleton className="h-6 w-20 rounded-full" />
                      </TableCell>
                      <TableCell>
                        <Skeleton className="h-6 w-16 rounded-full" />
                      </TableCell>
                      <TableCell className="text-center">
                        <Skeleton className="mx-auto h-8 w-8" />
                      </TableCell>
                    </TableRow>
                  ))
                ) : filteredAggregateApis.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="h-48 text-center">
                      <div className="flex flex-col items-center justify-center gap-2 text-muted-foreground">
                        <ShieldCheck className="h-8 w-8 opacity-20" />
                        <p>
                          {providerFilter === "all"
                            ? t("暂无聚合 API，点击右上角新建")
                            : t("暂无 {provider} 聚合 API", {
                                provider:
                                  AGGREGATE_API_PROVIDER_LABELS[
                                    providerFilter
                                  ] || providerFilter,
                              })}
                        </p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  filteredAggregateApis.map((api) => {
                    const revealed = revealedSecrets[api.id];
                    const isEnabled =
                      String(api.status || "")
                        .trim()
                        .toLowerCase() !== "disabled";
                    const createdTimeText = formatTsFromSeconds(
                      api.createdAt,
                      t("未知时间"),
                    );

                    return (
                      <TableRow key={api.id} className="group">
                        <TableCell className="overflow-hidden">
                          <Tooltip>
                            <TooltipTrigger
                              render={<div />}
                              className="block cursor-help text-left"
                            >
                              <div className="grid gap-0.5 overflow-hidden">
                                <span className="block truncate text-xs font-medium text-foreground">
                                  {api.supplierName || "-"}
                                </span>
                                <span className="block truncate font-mono text-[11px] text-muted-foreground">
                                  {api.url}
                                </span>
                              </div>
                            </TooltipTrigger>
                            <TooltipContent className="max-w-sm whitespace-pre-wrap break-words">
                              <div className="grid gap-1">
                                <div className="text-[11px] font-medium">
                                  {api.supplierName || "-"}
                                </div>
                                <div className="break-all text-xs">
                                  {api.url}
                                </div>
                                <div className="text-[11px] opacity-80">
                                  {t("创建时间")}: {createdTimeText}
                                </div>
                              </div>
                            </TooltipContent>
                          </Tooltip>
                        </TableCell>
                        <TableCell className="text-center">
                          <div className="flex justify-center">
                            <Badge
                              variant="secondary"
                              className="w-fit text-[10px] font-normal"
                            >
                              {AGGREGATE_API_PROVIDER_LABELS[
                                api.providerType
                              ] || api.providerType}
                            </Badge>
                          </div>
                        </TableCell>
                        <TableCell className="overflow-hidden">
                          <div className="flex min-w-0 items-center gap-1.5 overflow-hidden">
                            <Tooltip>
                              <TooltipTrigger
                                render={<div />}
                                className="block min-w-0 cursor-help"
                              >
                                <code className="block min-w-0 flex-1 truncate rounded border border-primary/5 bg-muted/50 px-2 py-1 font-mono text-[10px] leading-4 text-primary">
                                  {revealed
                                    ? secretPreview(revealed)
                                    : loadingSecretId === api.id
                                      ? t("读取中...")
                                      : api.id}
                                </code>
                              </TooltipTrigger>
                              <TooltipContent className="max-w-sm whitespace-pre-wrap break-words">
                                {revealed ? secretPreview(revealed) : api.id}
                              </TooltipContent>
                            </Tooltip>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7 text-muted-foreground hover:text-primary"
                              disabled={!isServiceReady}
                              onClick={() => void toggleSecret(api.id)}
                            >
                              {revealed ? (
                                <EyeOff className="h-3.5 w-3.5" />
                              ) : (
                                <Eye className="h-3.5 w-3.5" />
                              )}
                            </Button>
                            {String(api.authType || "")
                              .trim()
                              .toLowerCase() === "userpass" ? (
                              <DropdownMenu>
                                <DropdownMenuTrigger>
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    className="h-7 w-7 text-muted-foreground hover:text-primary"
                                    render={<span />}
                                    nativeButton={false}
                                    disabled={!isServiceReady}
                                  >
                                    <Copy className="h-3.5 w-3.5" />
                                  </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end">
                                  <DropdownMenuItem
                                    onClick={() => void copySecret(api.id, "username")}
                                  >
                                    {t("复制用户名")}
                                  </DropdownMenuItem>
                                  <DropdownMenuItem
                                    onClick={() => void copySecret(api.id, "password")}
                                  >
                                    {t("复制密码")}
                                  </DropdownMenuItem>
                                </DropdownMenuContent>
                              </DropdownMenu>
                            ) : (
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-7 w-7 text-muted-foreground hover:text-primary"
                                disabled={!isServiceReady}
                                onClick={() => void copySecret(api.id, "key")}
                              >
                                <Copy className="h-3.5 w-3.5" />
                              </Button>
                            )}
                          </div>
                        </TableCell>
                        <TableCell className="text-center font-mono text-xs text-muted-foreground">
                          {api.sort}
                        </TableCell>
                        <TableCell className="whitespace-nowrap align-middle">
                          <div className="flex flex-col items-start gap-1">
                            <div className="flex items-center gap-2">
                              {renderTestStatus(api)}
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-7 gap-1.5 px-2 text-xs"
                                disabled={
                                  !isServiceReady || testingApiId === api.id
                                }
                                onClick={() => testMutation.mutate(api.id)}
                              >
                                <RefreshCw
                                  className={
                                    testingApiId === api.id
                                      ? "h-3.5 w-3.5 animate-spin"
                                      : "h-3.5 w-3.5"
                                  }
                                />
                                {t("测试")}
                              </Button>
                            </div>
                          </div>
                          {api.lastTestAt ? (
                            <p className="mt-1 text-[10px] text-muted-foreground">
                              {formatTsFromSeconds(api.lastTestAt, t("未知时间"))}
                            </p>
                          ) : null}
                          {api.lastTestStatus === "failed" && api.lastTestError ? (
                            <Tooltip>
                              <TooltipTrigger
                                render={<div />}
                                className="mt-1 block max-w-full cursor-help text-left"
                              >
                                <p className="max-w-[220px] truncate text-[10px] text-red-500/90">
                                  {api.lastTestError}
                                </p>
                              </TooltipTrigger>
                              <TooltipContent className="max-w-sm whitespace-pre-wrap break-words">
                                {api.lastTestError}
                              </TooltipContent>
                            </Tooltip>
                          ) : null}
                        </TableCell>
                        <TableCell className="align-middle">
                          <div className="flex items-center gap-2">
                            <Switch
                              className="scale-75"
                              checked={isEnabled}
                              disabled={
                                !isServiceReady || togglingApiId === api.id
                              }
                              onCheckedChange={(enabled) =>
                                toggleStatusMutation.mutate({ api, enabled })
                              }
                            />
                            <span className="text-[10px] font-medium text-muted-foreground">
                              {isEnabled ? t("启用") : t("禁用")}
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
                              onClick={() => openEditModal(api.id)}
                              title={t("编辑配置")}
                            >
                              <Settings2 className="h-4 w-4" />
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
                                  disabled={!isServiceReady}
                                  onClick={() => openEditModal(api.id)}
                                >
                                  {t("编辑聚合 API")}
                                </DropdownMenuItem>
                                <DropdownMenuItem
                                  className="gap-2"
                                  disabled={
                                    !isServiceReady || prioritizeMutation.isPending
                                  }
                                  onClick={() => prioritizeMutation.mutate(api)}
                                >
                                  <ArrowUp className="h-4 w-4" /> {t("设为优先")}
                                </DropdownMenuItem>
                                <DropdownMenuItem
                                  className="gap-2 text-red-500"
                                  disabled={!isServiceReady}
                                  onClick={() => setDeleteId(api.id)}
                                >
                                  <Trash2 className="h-4 w-4" /> {t("删除聚合 API")}
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
      </div>

      <AggregateApiModal
        open={modalOpen}
        onOpenChange={setModalOpen}
        aggregateApi={editingApi}
        defaultSort={defaultCreateSort}
      />

      <ConfirmDialog
        open={Boolean(deleteId)}
        onOpenChange={(open) => !open && setDeleteId(null)}
        title={t("删除聚合 API")}
        description={t("删除聚合 API")}
        confirmText={t("删除")}
        cancelText={t("取消")}
        onConfirm={() => {
          if (!deleteId) return;
          deleteMutation.mutate(deleteId);
          setDeleteId(null);
        }}
      />
    </div>
  );
}
