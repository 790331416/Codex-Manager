"use client";

import { useEffect, useRef } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { accountClient, ManagedModelPayload } from "@/lib/api/account-client";
import { serviceClient } from "@/lib/api/service-client";
import {
  buildCodexModelsCachePayload,
  serializeManagedModelCatalogForCodexCache,
} from "@/lib/api/model-catalog";
import { getAppErrorMessage } from "@/lib/api/transport";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { useDeferredDesktopActivation } from "@/hooks/useDeferredDesktopActivation";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { useI18n } from "@/lib/i18n/provider";
import { useAppStore } from "@/lib/store/useAppStore";
import { ManagedModelCatalog } from "@/types";

const MANAGED_MODEL_QUERY_KEY = ["managed-model-catalog"];

export function useManagedModels() {
  const queryClient = useQueryClient();
  const { t } = useI18n();
  const serviceStatus = useAppStore((state) => state.serviceStatus);
  const {
    canAccessManagementRpc,
    isDesktopRuntime,
    canUseBrowserDownloadExport,
  } = useRuntimeCapabilities();
  const isServiceReady = canAccessManagementRpc && serviceStatus.connected;
  const isPageActive = useDesktopPageActive("/models/");
  const isQueryEnabled = useDeferredDesktopActivation(isServiceReady && isPageActive);
  const codexUserAgentRef = useRef("");
  const syncedCatalogFingerprintRef = useRef("");

  const ensureServiceReady = (actionLabel: string): boolean => {
    if (isServiceReady) {
      return true;
    }
    toast.info(`${t("服务未连接，暂时无法")} ${t(actionLabel)}`);
    return false;
  };

  const resolveCodexUserAgent = async (): Promise<string> => {
    const cachedUserAgent = codexUserAgentRef.current.trim();
    if (cachedUserAgent.includes("codex_cli_rs/")) {
      return cachedUserAgent;
    }

    const initializeResult = await serviceClient.initialize(serviceStatus.addr);
    const userAgent = String(initializeResult.userAgent || "").trim();
    if (!userAgent.includes("codex_cli_rs/")) {
      throw new Error(t("当前服务未返回可用的 Codex CLI 标识"));
    }

    codexUserAgentRef.current = userAgent;
    return userAgent;
  };

  const triggerBrowserDownload = (fileName: string, content: string): void => {
    if (typeof document === "undefined") {
      throw new Error(t("当前环境不支持浏览器导出"));
    }

    const blob = new Blob([content], {
      type: "application/json;charset=utf-8",
    });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = fileName;
    anchor.style.display = "none";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 0);
  };

  const syncCatalogToCodexCache = async (
    catalog: ManagedModelCatalog | null | undefined,
    options?: { force?: boolean },
  ): Promise<string | null> => {
    if (!catalog) {
      return "模型目录为空";
    }

    if (!isDesktopRuntime) {
      return null;
    }

    if (!isServiceReady) {
      return "服务未连接";
    }

    const models = serializeManagedModelCatalogForCodexCache(catalog.items || []);
    if (models.length === 0) {
      return "模型目录为空";
    }

    const fingerprint = JSON.stringify(models);
    if (!options?.force && syncedCatalogFingerprintRef.current === fingerprint) {
      return null;
    }

    try {
      const userAgent = await resolveCodexUserAgent();
      await serviceClient.syncCodexModelsCache({
        userAgent,
        models,
      });
      syncedCatalogFingerprintRef.current = fingerprint;
      return null;
    } catch (error) {
      return getAppErrorMessage(error);
    }
  };

  const reloadManagedCatalog = async (): Promise<ManagedModelCatalog> => {
    const catalog = await accountClient.listManagedModels(false);
    queryClient.setQueryData(MANAGED_MODEL_QUERY_KEY, catalog);
    return catalog;
  };

  const query = useQuery({
    queryKey: MANAGED_MODEL_QUERY_KEY,
    queryFn: () => accountClient.listManagedModels(false),
    enabled: isQueryEnabled,
    retry: 1,
  });

  const invalidateAll = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: MANAGED_MODEL_QUERY_KEY }),
      queryClient.invalidateQueries({ queryKey: ["apikey-models"] }),
      queryClient.invalidateQueries({ queryKey: ["startup-snapshot"] }),
    ]);
  };

  const refreshMutation = useMutation({
    mutationFn: (refreshRemote: boolean) => accountClient.listManagedModels(refreshRemote),
    onSuccess: async (catalog) => {
      queryClient.setQueryData(MANAGED_MODEL_QUERY_KEY, catalog);
      const cacheSyncError = await syncCatalogToCodexCache(catalog);
      await invalidateAll();
      if (cacheSyncError) {
        toast.error(`${t("模型目录已刷新，但同步 Codex 模型缓存失败")}: ${cacheSyncError}`);
      } else {
        toast.success(t("模型目录已刷新"));
      }
    },
    onError: (error: unknown) => {
      toast.error(`${t("刷新模型失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const saveMutation = useMutation({
    mutationFn: (params: ManagedModelPayload) => accountClient.saveManagedModel(params),
    onSuccess: async () => {
      const catalog = await reloadManagedCatalog();
      const cacheSyncError = await syncCatalogToCodexCache(catalog);
      await invalidateAll();
      if (cacheSyncError) {
        toast.error(`${t("模型已保存，但同步 Codex 模型缓存失败")}: ${cacheSyncError}`);
      } else {
        toast.success(t("模型已保存"));
      }
    },
    onError: (error: unknown) => {
      toast.error(`${t("保存模型失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (slug: string) => accountClient.deleteManagedModel(slug),
    onSuccess: async () => {
      const catalog = await reloadManagedCatalog();
      const cacheSyncError = await syncCatalogToCodexCache(catalog);
      await invalidateAll();
      if (cacheSyncError) {
        toast.error(`${t("模型已删除，但同步 Codex 模型缓存失败")}: ${cacheSyncError}`);
      } else {
        toast.success(t("模型已删除"));
      }
    },
    onError: (error: unknown) => {
      toast.error(`${t("删除模型失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  const exportMutation = useMutation({
    mutationFn: async () => {
      if (!isServiceReady) {
        throw new Error(t("服务未连接"));
      }

      const catalog = query.data ?? (await reloadManagedCatalog());
      const models = catalog.items || [];
      if (!models.length) {
        throw new Error(t("模型目录为空"));
      }

      if (isDesktopRuntime) {
        const cacheSyncError = await syncCatalogToCodexCache(catalog, { force: true });
        if (cacheSyncError) {
          throw new Error(cacheSyncError);
        }
        return { mode: "desktop" as const };
      }

      if (!canUseBrowserDownloadExport) {
        throw new Error(t("当前环境不支持导出 Codex 缓存"));
      }

      const userAgent = await resolveCodexUserAgent();
      const payload = buildCodexModelsCachePayload(models, userAgent);
      triggerBrowserDownload("models_cache.json", `${JSON.stringify(payload, null, 2)}\n`);
      return { mode: "browser" as const };
    },
    onSuccess: (result) => {
      toast.success(
        result?.mode === "browser"
          ? t("Codex 缓存已下载，请保存到 `~/.codex/models_cache.json`")
          : t("已导出到本地 Codex 缓存")
      );
    },
    onError: (error) => {
      toast.error(`${t("导出失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  useEffect(() => {
    codexUserAgentRef.current = "";
    syncedCatalogFingerprintRef.current = "";
  }, [serviceStatus.addr]);

  useEffect(() => {
    if (!isDesktopRuntime || !isServiceReady || !query.data || query.dataUpdatedAt === 0) {
      return;
    }

    void syncCatalogToCodexCache(query.data).then((errorMessage) => {
      if (errorMessage) {
        console.warn("sync codex models cache failed", errorMessage);
      }
    });
  }, [
    isDesktopRuntime,
    isServiceReady,
    query.data,
    query.dataUpdatedAt,
  ]);

  return {
    models: query.data?.items || [],
    catalog: query.data || { items: [] },
    isLoading: isServiceReady && (!isQueryEnabled || query.isLoading),
    isServiceReady,
    refreshRemote: async () => {
      if (!ensureServiceReady("刷新模型")) return null;
      return refreshMutation.mutateAsync(true);
    },
    refreshLocal: async () => {
      if (!ensureServiceReady("读取模型")) return null;
      return refreshMutation.mutateAsync(false);
    },
    saveModel: async (params: ManagedModelPayload) => {
      if (!ensureServiceReady("保存模型")) return null;
      return saveMutation.mutateAsync(params);
    },
    deleteModel: async (slug: string) => {
      if (!ensureServiceReady("删除模型")) return false;
      await deleteMutation.mutateAsync(slug);
      return true;
    },
    exportCodexCache: async () => {
      if (!ensureServiceReady("导出模型目录")) return false;
      await exportMutation.mutateAsync();
      return true;
    },
    isRefreshing: refreshMutation.isPending,
    isSaving: saveMutation.isPending,
    isDeleting: deleteMutation.isPending,
    isExporting: exportMutation.isPending,
    canExportCodexCache:
      !isDesktopRuntime && isServiceReady && Boolean(query.data?.items?.length),
  };
}
