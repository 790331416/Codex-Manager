"use client";

import { useCallback, useEffect, useState, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { usePathname } from "next/navigation";
import { AlertCircle, Play, RefreshCw } from "lucide-react";
import { useTheme } from "next-themes";
import { toast } from "sonner";
import { useAppStore } from "@/lib/store/useAppStore";
import { serviceClient } from "@/lib/api/service-client";
import {
  buildStartupSnapshotQueryKey,
  STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
  STARTUP_SNAPSHOT_STALE_TIME,
} from "@/lib/api/startup-snapshot";
import { appClient } from "@/lib/api/app-client";
import { loadRuntimeCapabilities } from "@/lib/api/transport";
import { Button } from "@/components/ui/button";
import { CodexCliOnboardingDialog } from "@/components/layout/codex-cli-onboarding-dialog";
import { applyAppearancePreset } from "@/lib/appearance";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { useLocalDayRange } from "@/hooks/useLocalDayRange";
import {
  formatServiceError,
  isExpectedInitializeResult,
  normalizeServiceAddr,
} from "@/lib/utils/service";
import { useI18n } from "@/lib/i18n/provider";
import {
  getCanonicalStaticRouteUrl,
  normalizeRoutePath,
} from "@/lib/utils/static-routes";

const DEFAULT_SERVICE_ADDR = "localhost:48760";
const STARTUP_WARMUP_LABEL = "[startup warmup]";
/**
 * 函数 `sleep`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - ms: 参数 ms
 *
 * # 返回
 * 返回函数执行结果
 */
const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

/**
 * 函数 `AppBootstrap`
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
export function AppBootstrap({ children }: { children: React.ReactNode }) {
  const {
    setServiceStatus,
    setAppSettings,
    setRuntimeCapabilities,
    closeCodexCliGuide,
    serviceStatus,
    appSettings,
    isCodexCliGuideOpen,
    runtimeCapabilities,
  } = useAppStore();
  const { setTheme } = useTheme();
  const { t } = useI18n();
  const localDayRange = useLocalDayRange();
  const queryClient = useQueryClient();
  const pathname = usePathname();
  const { canManageService, isDesktopRuntime, isUnsupportedWebRuntime } =
    useRuntimeCapabilities();
  const [isInitializing, setIsInitializing] = useState(true);
  const hasInitializedOnce = useRef(false);
  const hasBootstrappedOnce = useRef(false);
  const serviceStatusRef = useRef(serviceStatus);
  const runtimeCapabilitiesRef = useRef(runtimeCapabilities);
  const [error, setError] = useState<string | null>(null);
  const [guideSessionDismissed, setGuideSessionDismissed] = useState(false);
  const supportsLocalServiceStart = canManageService;

  useEffect(() => {
    serviceStatusRef.current = serviceStatus;
  }, [serviceStatus]);

  useEffect(() => {
    runtimeCapabilitiesRef.current = runtimeCapabilities;
  }, [runtimeCapabilities]);

  /**
   * 函数 `applyLowTransparency`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - enabled: 参数 enabled
   *
   * # 返回
   * 返回函数执行结果
   */
  const applyLowTransparency = (enabled: boolean) => {
    if (enabled) {
      document.body.classList.add("low-transparency");
    } else {
      document.body.classList.remove("low-transparency");
    }
  };

  const initializeService = useCallback(async (addr: string, retries = 0) => {
    let lastError: unknown = null;

    for (let attempt = 0; attempt <= retries; attempt += 1) {
      try {
        const initializeResult = await serviceClient.initialize(addr);
        if (!isExpectedInitializeResult(initializeResult)) {
          throw new Error("Port is in use or unexpected service responded (invalid initialize response)");
        }
        return initializeResult;
      } catch (serviceError: unknown) {
        lastError = serviceError;
        if (attempt < retries) {
          await sleep(300);
        }
      }
    }

    throw lastError || new Error(t("服务初始化失败: {addr}", { addr }));
  }, [t]);

  const startAndInitializeService = useCallback(
    async (addr: string) => {
      await serviceClient.start(addr);
      return initializeService(addr, 2);
    },
    [initializeService]
  );

  const prefetchStartupSnapshot = useCallback(
    async (addr: string) => {
      await queryClient.prefetchQuery({
        queryKey: buildStartupSnapshotQueryKey(
          addr,
          STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
          localDayRange.dayStartTs,
        ),
        queryFn: () =>
          serviceClient.getStartupSnapshot({
            requestLogLimit: STARTUP_SNAPSHOT_REQUEST_LOG_LIMIT,
            dayStartTs: localDayRange.dayStartTs,
            dayEndTs: localDayRange.dayEndTs,
          }),
        staleTime: STARTUP_SNAPSHOT_STALE_TIME,
      });
    },
    [localDayRange.dayEndTs, localDayRange.dayStartTs, queryClient]
  );

  const shouldBlockOnInitialDashboardSnapshot = useCallback(
    (desktopRuntime: boolean) =>
      desktopRuntime &&
      !hasInitializedOnce.current &&
      normalizeRoutePath(pathname) === "/",
    [pathname],
  );

  const applyConnectedServiceState = useCallback(
    async (
      addr: string,
      version: string,
      lowTransparency: boolean,
      options?: { blockOnDashboardSnapshot?: boolean },
    ) => {
      if (options?.blockOnDashboardSnapshot) {
        try {
          await prefetchStartupSnapshot(addr);
        } catch (warmupError) {
          console.warn(
            `${STARTUP_WARMUP_LABEL} initial dashboard snapshot prefetch failed`,
            warmupError,
          );
        }
      }
      setServiceStatus({
        addr,
        connected: true,
        version,
      });
      applyLowTransparency(lowTransparency);
      setIsInitializing(false);
      hasInitializedOnce.current = true;
    },
    [prefetchStartupSnapshot, setServiceStatus],
  );

  const init = useCallback(async () => {
    // Only show full screen loading if we haven't initialized once
    if (!hasInitializedOnce.current) {
      setIsInitializing(true);
    }
    setError(null);

    try {
      const detectedRuntimeCapabilities = await loadRuntimeCapabilities(
        runtimeCapabilitiesRef.current?.mode === "unsupported-web"
      );
      setRuntimeCapabilities(detectedRuntimeCapabilities);
      const desktopRuntime = detectedRuntimeCapabilities.mode === "desktop-tauri";
      const shouldBlockOnDashboardSnapshot =
        shouldBlockOnInitialDashboardSnapshot(desktopRuntime);

      if (detectedRuntimeCapabilities.mode === "unsupported-web") {
        if (!hasInitializedOnce.current) {
          setServiceStatus({ connected: false, version: "" });
          setError(
            detectedRuntimeCapabilities.unsupportedReason ||
              t("当前 Web 运行方式不受支持")
          );
        }
        setIsInitializing(false);
        return;
      }

      const settings = await appClient.getSettings();
      const addr = normalizeServiceAddr(settings.serviceAddr || DEFAULT_SERVICE_ADDR);
      const currentServiceStatus = serviceStatusRef.current;
      
      const currentAppliedTheme = typeof document !== 'undefined' ? document.documentElement.getAttribute('data-theme') : null;
      if (settings.theme && settings.theme !== currentAppliedTheme) {
        setTheme(settings.theme);
      }
      applyAppearancePreset(settings.appearancePreset);
      
      setAppSettings(settings);
      
      // CRITICAL: Do not reset status to connected: false if we are already connected
      // This prevents the Header badge from flashing
      if (!currentServiceStatus.connected || currentServiceStatus.addr !== addr) {
        setServiceStatus({ addr, connected: false, version: "" });
      }

      try {
        try {
          await initializeService(addr, 1);
        } catch (initializeError) {
          if (!desktopRuntime) {
            throw initializeError;
          }
          await startAndInitializeService(addr);
        }
        await applyConnectedServiceState(
          addr,
          "",
          settings.lowTransparency,
          { blockOnDashboardSnapshot: shouldBlockOnDashboardSnapshot },
        );
      } catch (serviceError: unknown) {
        if (!hasInitializedOnce.current) {
           setServiceStatus({ addr, connected: false, version: "" });
           setError(formatServiceError(serviceError));
        }
        setIsInitializing(false);
      }
    } catch (appError: unknown) {
      if (!hasInitializedOnce.current) {
        setError(appError instanceof Error ? appError.message : String(appError));
      }
      setIsInitializing(false);
    }
    // 使用 ref 读取最新 serviceStatus，避免把初始化流程绑定到状态抖动上
  }, [
    applyConnectedServiceState,
    initializeService,
    setAppSettings,
    setRuntimeCapabilities,
    setServiceStatus,
    setTheme,
    startAndInitializeService,
    shouldBlockOnInitialDashboardSnapshot,
    t,
  ]);

  /**
   * 函数 `handleForceStart`
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
  const handleForceStart = async () => {
    if (!supportsLocalServiceStart) {
      void init();
      return;
    }

    setIsInitializing(true);
    setError(null);
    try {
      const addr = normalizeServiceAddr(serviceStatus.addr || DEFAULT_SERVICE_ADDR);
      const settings = await appClient.setSettings({ serviceAddr: addr });
      
      const currentAppliedTheme = typeof document !== 'undefined' ? document.documentElement.getAttribute('data-theme') : null;
      if (settings.theme && settings.theme !== currentAppliedTheme) {
        setTheme(settings.theme);
      }
      applyAppearancePreset(settings.appearancePreset);
      
      setAppSettings(settings);
      await startAndInitializeService(addr);
      await applyConnectedServiceState(
        addr,
        "",
        settings.lowTransparency,
        {
          blockOnDashboardSnapshot:
            shouldBlockOnInitialDashboardSnapshot(true),
        },
      );
      toast.success(t("服务已连接"));
    } catch (startError: unknown) {
      setServiceStatus({ connected: false, version: "" });
      setError(formatServiceError(startError));
      setIsInitializing(false);
    }
  };

  const handleGuideOpenChange = useCallback((open: boolean) => {
    if (open) {
      return;
    }
    if (isCodexCliGuideOpen) {
      closeCodexCliGuide();
      return;
    }
    setGuideSessionDismissed(true);
  }, [closeCodexCliGuide, isCodexCliGuideOpen]);

  const handleGuideAcknowledge = useCallback(
    async (dismissPermanently: boolean) => {
      if (dismissPermanently) {
        try {
          const settings = await appClient.setSettings({
            codexCliGuideDismissed: true,
          });
          setAppSettings(settings);
          toast.success(t("后续将不再显示这份引导"));
        } catch (guideError: unknown) {
          const message =
            guideError instanceof Error ? guideError.message : String(guideError);
          toast.error(t("保存引导状态失败: {message}", { message }));
          throw guideError;
        }
      }

      closeCodexCliGuide();
      setGuideSessionDismissed(true);
    },
    [closeCodexCliGuide, setAppSettings, t]
  );

  useEffect(() => {
    if (hasBootstrappedOnce.current) {
      return;
    }
    hasBootstrappedOnce.current = true;
    void init();
  }, [init]);

  useEffect(() => {
    if (isDesktopRuntime || typeof window === "undefined") {
      return;
    }

    const canonicalUrl = getCanonicalStaticRouteUrl();
    if (!canonicalUrl) {
      return;
    }

    window.history.replaceState(window.history.state, "", canonicalUrl);
  }, [isDesktopRuntime, pathname]);

  const showLoading = isInitializing && !hasInitializedOnce.current;
  const showError = !!error && !hasInitializedOnce.current;
  const showCodexGuide =
    isCodexCliGuideOpen ||
    serviceStatus.connected &&
    !showLoading &&
    !showError &&
    !isUnsupportedWebRuntime &&
    !guideSessionDismissed &&
    !appSettings.codexCliGuideDismissed;
  return (
    <>
      {/* Always keep children mounted to prevent Header/Sidebar remounting 'reload' feel */}
      {children}

      <CodexCliOnboardingDialog
        open={showCodexGuide}
        onOpenChange={handleGuideOpenChange}
        onAcknowledge={handleGuideAcknowledge}
      />

      {(showLoading || showError) && (
        <div className="fixed inset-0 z-50 flex flex-col items-center justify-center bg-background">
          <div className="flex w-full max-w-md flex-col items-center gap-6 rounded-3xl glass-card p-10 shadow-2xl animate-in fade-in zoom-in duration-500">
            {showLoading ? (
              <>
                <div className="h-14 w-14 animate-spin rounded-full border-4 border-primary border-t-transparent" />
                <div className="flex flex-col items-center gap-2">
                  <h2 className="text-2xl font-bold tracking-tight">{t("正在准备环境")}</h2>
                  <p className="px-4 text-center text-sm text-muted-foreground">
                    {t("正在同步本地配置，请稍候...")}
                  </p>
                </div>
              </>
            ) : (
              <>
                <div className="flex h-14 w-14 items-center justify-center rounded-full bg-destructive/10">
                  <AlertCircle className="h-8 w-8 text-destructive" />
                </div>
                <div className="flex flex-col items-center gap-2 text-center">
                  <h2 className="text-xl font-bold tracking-tight text-destructive">
                    {isUnsupportedWebRuntime
                      ? t("当前 Web 运行方式不受支持")
                      : t("无法同步核心服务状态")}
                  </h2>
                  {isUnsupportedWebRuntime ? (
                    <p className="px-4 text-center text-sm text-muted-foreground">
                      {t(
                        "请通过 `codexmanager-web` 打开页面，或在反向代理中同时提供 `/api/runtime` 与 `/api/rpc`。",
                      )}
                    </p>
                  ) : null}
                  <p className="max-h-32 overflow-y-auto break-all rounded-lg bg-muted/50 p-3 font-mono text-[10px] text-muted-foreground">
                    {error}
                  </p>
                </div>
                <div
                  className={`grid w-full gap-3 ${supportsLocalServiceStart ? "grid-cols-2" : "grid-cols-1"}`}
                >
                  <Button variant="outline" onClick={() => void init()} className="h-11 gap-2">
                    <RefreshCw className="h-4 w-4" /> {t("重试")}
                  </Button>
                  {supportsLocalServiceStart ? (
                    <Button onClick={handleForceStart} className="h-11 gap-2 bg-primary">
                      <Play className="h-4 w-4" /> {t("强制启动")}
                    </Button>
                  ) : null}
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </>
  );
}
