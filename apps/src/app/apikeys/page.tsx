"use client";

import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  DollarSign,
  Copy,
  Eye,
  EyeOff,
  MoreVertical,
  Plus,
  RefreshCw,
  Settings2,
  Zap,
  Trash2,
} from "lucide-react";
import { toast } from "sonner";
import { ApiKeyModal } from "@/components/modals/api-key-modal";
import { ConfirmDialog } from "@/components/modals/confirm-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import { useApiKeys } from "@/hooks/useApiKeys";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { useDeferredDesktopActivation } from "@/hooks/useDeferredDesktopActivation";
import { usePageTransitionReady } from "@/hooks/usePageTransitionReady";
import { accountClient } from "@/lib/api/account-client";
import { serviceClient } from "@/lib/api/service-client";
import { useAppStore } from "@/lib/store/useAppStore";
import { copyTextToClipboard } from "@/lib/utils/clipboard";
import { formatCompactNumber } from "@/lib/utils/usage";

const ROTATION_STRATEGY_LABELS: Record<string, string> = {
  account_rotation: "账号轮转",
  aggregate_api_rotation: "聚合API轮转",
};

function formatUsd(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Math.max(0, value));
}

function sumRequestLogCost(items: Array<{ estimatedCostUsd: number | null }>): number {
  return items.reduce((sum, item) => sum + Math.max(0, item.estimatedCostUsd || 0), 0);
}

function ApiKeyStatCard({
  title,
  value,
  icon: Icon,
  color,
  sub,
}: {
  title: string;
  value: string;
  icon: typeof Zap;
  color: string;
  sub: string;
}) {
  return (
    <Card className="glass-card overflow-hidden border-none shadow-md backdrop-blur-md transition-all hover:scale-[1.02]">
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium">{title}</CardTitle>
        <Icon className={color} />
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{value}</div>
        <p className="mt-1 text-[10px] text-muted-foreground">{sub}</p>
      </CardContent>
    </Card>
  );
}

export default function ApiKeysPage() {
  const serviceAddr = useAppStore((state) => state.serviceStatus.addr);
  const {
    apiKeys,
    isLoading,
    isModelsLoading,
    isServiceReady,
    deleteApiKey,
    toggleApiKeyStatus,
    refreshModels,
    readApiKeySecret,
    isToggling,
    isRefreshingModels,
  } = useApiKeys();
  const isPageActive = useDesktopPageActive("/apikeys/");
  const isUsageQueryEnabled = useDeferredDesktopActivation(isServiceReady);
  usePageTransitionReady(
    "/apikeys/",
    !isServiceReady || (!isLoading && !isModelsLoading),
  );
  const [revealedSecrets, setRevealedSecrets] = useState<Record<string, string>>({});
  const [loadingSecretId, setLoadingSecretId] = useState<string | null>(null);
  const [apiKeyModalOpen, setApiKeyModalOpen] = useState(false);
  const [editingKeyId, setEditingKeyId] = useState<string | null>(null);
  const [deleteKeyId, setDeleteKeyId] = useState<string | null>(null);

  useEffect(() => {
    if (isPageActive) {
      return;
    }
    setApiKeyModalOpen(false);
    setEditingKeyId(null);
    setDeleteKeyId(null);
  }, [isPageActive]);

  const editingApiKey = useMemo(
    () => apiKeys.find((item) => item.id === editingKeyId) || null,
    [apiKeys, editingKeyId]
  );
  const { data: usageOverview, isPending: isUsageOverviewLoading } = useQuery({
    queryKey: ["apikey-usage-overview", serviceAddr || null],
    queryFn: async () => {
      const [stats, firstPage] = await Promise.all([
        accountClient.listApiKeyUsageStats(),
        serviceClient.listRequestLogs({ page: 1, pageSize: 500 }),
      ]);

      const usageByKey = stats.reduce<Record<string, number>>((result, item) => {
        const keyId = String(item.keyId || "").trim();
        if (!keyId) return result;
        result[keyId] = Math.max(0, item.totalTokens || 0);
        return result;
      }, {});

      let totalCostUsd = sumRequestLogCost(firstPage.items);
      const pageSize = Math.max(1, firstPage.pageSize || 500);
      const totalPages = Math.max(1, Math.ceil(firstPage.total / pageSize));
      for (let page = 2; page <= totalPages; page += 1) {
        const pageResult = await serviceClient.listRequestLogs({ page, pageSize });
        totalCostUsd += sumRequestLogCost(pageResult.items);
      }

      const totalTokens = Object.values(usageByKey).reduce((sum, value) => sum + value, 0);
      return {
        usageByKey,
        totalTokens,
        totalCostUsd,
      };
    },
    enabled: isUsageQueryEnabled && isPageActive,
    retry: 1,
  });
  const usageByKey = usageOverview?.usageByKey || {};
  const showOverviewLoading = isServiceReady && isPageActive && isUsageOverviewLoading;

  const openCreateModal = () => {
    setEditingKeyId(null);
    setApiKeyModalOpen(true);
  };

  const openEditModal = (id: string) => {
    setEditingKeyId(id);
    setApiKeyModalOpen(true);
  };

  const ensureSecretLoaded = async (id: string) => {
    if (revealedSecrets[id]) {
      return revealedSecrets[id];
    }
    setLoadingSecretId(id);
    try {
      const secret = await readApiKeySecret(id);
      if (!secret) {
        throw new Error("后端未返回密钥明文");
      }
      setRevealedSecrets((current) => ({ ...current, [id]: secret }));
      return secret;
    } finally {
      setLoadingSecretId(null);
    }
  };

  const toggleSecret = async (id: string) => {
    if (revealedSecrets[id]) {
      setRevealedSecrets((current) => {
        const nextState = { ...current };
        delete nextState[id];
        return nextState;
      });
      return;
    }

    try {
      await ensureSecretLoaded(id);
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : String(error));
    }
  };

  const copyToClipboard = async (id: string) => {
    try {
      const secret = await ensureSecretLoaded(id);
      await copyTextToClipboard(secret);
      toast.success("已复制到剪贴板");
    } catch (error: unknown) {
      toast.error(error instanceof Error ? error.message : String(error));
    }
  };

  const handleDelete = (id: string) => {
    setDeleteKeyId(id);
  };

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      {!isServiceReady ? (
        <Card className="glass-card border-none shadow-sm">
          <CardContent className="pt-6 text-sm text-muted-foreground">
            服务未连接，平台密钥与模型配置暂不可用；连接恢复后会自动继续加载。
          </CardContent>
        </Card>
      ) : null}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold tracking-tight">平台密钥</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            创建和管理网关调用所需的访问令牌
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            className="glass-card h-10 gap-2"
            onClick={() => refreshModels(true)}
            disabled={!isServiceReady || isRefreshingModels}
          >
            <RefreshCw className={isRefreshingModels ? "h-4 w-4 animate-spin" : "h-4 w-4"} />
            刷新模型
          </Button>
          <Button
            className="h-10 gap-2 shadow-lg shadow-primary/20"
            onClick={openCreateModal}
            disabled={!isServiceReady}
          >
            <Plus className="h-4 w-4" /> 创建密钥
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        {isLoading || showOverviewLoading ? (
          <>
            <Skeleton className="h-32 w-full rounded-2xl" />
            <Skeleton className="h-32 w-full rounded-2xl" />
          </>
        ) : (
          <>
            <ApiKeyStatCard
              title="总使用 Token"
              value={formatCompactNumber(usageOverview?.totalTokens || 0, "0")}
              icon={Zap}
              color="h-4 w-4 text-amber-500"
              sub="按全部平台密钥累计"
            />
            <ApiKeyStatCard
              title="总费用"
              value={formatUsd(usageOverview?.totalCostUsd || 0)}
              icon={DollarSign}
              color="h-4 w-4 text-emerald-500"
              sub="按请求日志累计估算"
            />
          </>
        )}
      </div>

      <Card className="glass-card overflow-hidden border-none py-0 shadow-xl backdrop-blur-md">
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>密钥 / ID</TableHead>
                <TableHead>名称</TableHead>
                <TableHead>协议</TableHead>
                <TableHead>轮转策略</TableHead>
                <TableHead>绑定模型</TableHead>
                <TableHead>总使用 Token</TableHead>
                <TableHead>状态</TableHead>
                <TableHead className="text-center">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                Array.from({ length: 3 }).map((_, index) => (
                    <TableRow key={index}>
                      <TableCell><Skeleton className="h-4 w-32" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-20" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-20" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-28" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-20" /></TableCell>
                      <TableCell><Skeleton className="h-6 w-16 rounded-full" /></TableCell>
                      <TableCell className="text-center"><Skeleton className="mx-auto h-8 w-8" /></TableCell>
                    </TableRow>
                ))
              ) : apiKeys.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="h-48 text-center">
                    <div className="flex flex-col items-center justify-center gap-2 text-muted-foreground">
                      <Plus className="h-8 w-8 opacity-20" />
                      <p>暂无平台密钥，点击右上角创建</p>
                    </div>
                  </TableCell>
                </TableRow>
              ) : (
                apiKeys.map((key) => {
                  const revealed = revealedSecrets[key.id];
                  const isEnabled = String(key.status).toLowerCase() !== "disabled";

                  return (
                    <TableRow key={key.id} className="group">
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <code
                            className="max-w-[240px] break-all whitespace-normal rounded border border-primary/5 bg-muted/50 px-2 py-1 font-mono text-[10px] leading-4 text-primary"
                            title={revealed || key.id}
                          >
                            {revealed
                              ? revealed
                              : loadingSecretId === key.id
                                ? "读取中..."
                                : key.id}
                          </code>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7 text-muted-foreground hover:text-primary"
                            disabled={!isServiceReady}
                            onClick={() => void toggleSecret(key.id)}
                          >
                            {revealed ? (
                              <EyeOff className="h-3.5 w-3.5" />
                            ) : (
                              <Eye className="h-3.5 w-3.5" />
                            )}
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7 text-muted-foreground hover:text-primary"
                            disabled={!isServiceReady}
                            onClick={() => void copyToClipboard(key.id)}
                          >
                            <Copy className="h-3.5 w-3.5" />
                          </Button>
                        </div>
                      </TableCell>
                      <TableCell className="text-sm font-semibold">{key.name || "未命名"}</TableCell>
                      <TableCell>
                        <Badge variant="outline" className="bg-accent/20 text-[10px] font-normal capitalize">
                          {key.protocol.replace(/_/g, " ")}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Badge variant="secondary" className="text-[10px] font-normal">
                          {ROTATION_STRATEGY_LABELS[key.rotationStrategy] ||
                            key.rotationStrategy}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs font-medium text-muted-foreground">
                        {key.model ? (
                          key.model
                        ) : (
                          <span title="跟随请求表示使用请求体里的实际 model；请求日志展示的是最终生效模型。">
                            跟随请求
                          </span>
                        )}
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {formatCompactNumber(usageByKey[key.id] ?? 0, "0")}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Switch
                            className="scale-75"
                            checked={isEnabled}
                            disabled={!isServiceReady || isToggling}
                            onCheckedChange={(enabled) =>
                              toggleApiKeyStatus({ id: key.id, enabled })
                            }
                          />
                          <span className="text-[10px] font-medium text-muted-foreground">
                            {isEnabled ? "启用" : "禁用"}
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
                            onClick={() => openEditModal(key.id)}
                            title="编辑配置"
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
                                onClick={() => openEditModal(key.id)}
                              >
                                设置模型与推理
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                className="gap-2 text-red-500"
                                disabled={!isServiceReady}
                                onClick={() => handleDelete(key.id)}
                              >
                                <Trash2 className="h-4 w-4" /> 删除密钥
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

      <ApiKeyModal
        open={apiKeyModalOpen}
        onOpenChange={setApiKeyModalOpen}
        apiKey={editingApiKey}
      />
      <ConfirmDialog
        open={Boolean(deleteKeyId)}
        onOpenChange={(open) => {
          if (!open) {
            setDeleteKeyId(null);
          }
        }}
        title="删除平台密钥"
        description={`确定删除平台密钥 ${apiKeys.find((item) => item.id === deleteKeyId)?.name || ""} 吗？删除后不可恢复。`}
        confirmText="删除"
        confirmVariant="destructive"
        onConfirm={() => {
          if (!deleteKeyId) return;
          deleteApiKey(deleteKeyId);
        }}
      />
    </div>
  );
}
