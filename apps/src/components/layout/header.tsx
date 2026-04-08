"use client";

import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { Settings as SettingsIcon } from "lucide-react";
import { toast } from "sonner";
import { useAppStore } from "@/lib/store/useAppStore";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { DisclaimerTicker } from "@/components/layout/disclaimer-ticker";
import { LanguageSwitcher } from "@/components/layout/language-switcher";
import { WebPasswordModal } from "../modals/web-password-modal";
import { serviceClient } from "@/lib/api/service-client";
import { appClient } from "@/lib/api/app-client";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { useI18n } from "@/lib/i18n/provider";
import {
  formatServiceError,
  isExpectedInitializeResult,
  normalizeServiceAddr,
} from "@/lib/utils/service";

const DEFAULT_SERVICE_ADDR = "localhost:48760";

/**
 * 函数 `Header`
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
export function Header() {
  const { serviceStatus, setServiceStatus, setAppSettings } = useAppStore();
  const pathname = usePathname();
  const { t } = useI18n();
  const [webPasswordModalOpen, setWebPasswordModalOpen] = useState(false);
  const [isToggling, setIsToggling] = useState(false);
  const [portInput, setPortInput] = useState("48760");
  const { canManageService } = useRuntimeCapabilities();

  useEffect(() => {
    const current = String(serviceStatus.addr || DEFAULT_SERVICE_ADDR);
    const [, port = current] = current.split(":");
    setPortInput(port || "48760");
  }, [serviceStatus.addr]);

  /**
   * 函数 `getPageTitle`
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
  const getPageTitle = () => {
      const normalizedPathname = pathname === "/" ? pathname : pathname.replace(/\/+$/, "");
      switch (normalizedPathname) {
      case "/":
        return t("仪表盘");
      case "/accounts":
        return t("账号管理");
      case "/apikeys":
        return t("平台密钥");
      case "/aggregate-api":
        return t("聚合API");
      case "/logs":
        return t("请求日志");
      case "/settings":
        return t("应用设置");
      default:
        return "CodexManager";
    }
  };

  /**
   * 函数 `persistServiceAddr`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - nextAddr: 参数 nextAddr
   *
   * # 返回
   * 返回函数执行结果
   */
  const persistServiceAddr = async (nextAddr: string) => {
    const normalized = normalizeServiceAddr(nextAddr);
    const settings = await appClient.setSettings({ serviceAddr: normalized });
    setAppSettings(settings);
    setServiceStatus({ addr: normalized });
    return normalized;
  };

  /**
   * 函数 `handleToggleService`
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
  const handleToggleService = async (enabled: boolean) => {
    setIsToggling(true);
    try {
      const nextAddr = await persistServiceAddr(serviceStatus.addr || `localhost:${portInput}`);
      if (enabled) {
        await serviceClient.start(nextAddr);
        const initResult = await serviceClient.initialize(nextAddr);
        if (!isExpectedInitializeResult(initResult)) {
          throw new Error("Port is in use or unexpected service responded (invalid initialize response)");
        }
        setServiceStatus({
          connected: true,
          version: "",
          addr: nextAddr,
        });
        toast.success("服务已启动");
      } else {
        await serviceClient.stop();
        setServiceStatus({ connected: false, version: "" });
        toast.info(t("服务已停止"));
      }
    } catch (error: unknown) {
      toast.error(`${t("操作失败")}: ${formatServiceError(error)}`);
    } finally {
      setIsToggling(false);
    }
  };

  /**
   * 函数 `handlePortBlur`
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
  const handlePortBlur = async () => {
    try {
      const nextAddr = await persistServiceAddr(`localhost:${portInput}`);
      setServiceStatus({ addr: nextAddr });
    } catch (error: unknown) {
      toast.error(`${t("保存")}失败: ${formatServiceError(error)}`);
    }
  };

  return (
    <>
      <header className="sticky top-0 z-30 flex h-16 items-center gap-4 glass-header px-6">
        <div className="flex min-w-0 shrink-0 items-center gap-4">
          <h1 className="text-lg font-semibold">{getPageTitle()}</h1>
          <Badge variant={serviceStatus.connected ? "default" : "secondary"} className="h-5">
            {serviceStatus.connected ? t("服务已连接") : t("服务未连接")}
          </Badge>
          {serviceStatus.version ? (
            <span className="text-xs text-muted-foreground">v{serviceStatus.version}</span>
          ) : null}
        </div>

        <div className="hidden min-w-0 flex-1 justify-center lg:flex">
          <DisclaimerTicker />
        </div>

        <div className="flex shrink-0 items-center gap-4">
          <LanguageSwitcher compact triggerClassName="w-[152px]" />

          {canManageService ? (
            <div className="flex items-center gap-2 rounded-lg border bg-card/30 px-3 py-1.5 shadow-sm">
              <span className="text-xs font-medium text-muted-foreground">{t("监听端口")}</span>
              <Input
                className="h-7 w-16 border-none bg-transparent p-0 text-xs font-mono focus-visible:ring-0"
                placeholder="48760"
                value={portInput}
                onChange={(event) => {
                  const nextPort = event.target.value.replace(/[^\d]/g, "");
                  setPortInput(nextPort);
                  if (nextPort) {
                    setServiceStatus({ addr: `localhost:${nextPort}` });
                  }
                }}
                onBlur={() => void handlePortBlur()}
              />
              <div className="mx-1 h-4 w-px bg-border" />
              <Switch
                checked={serviceStatus.connected}
                disabled={isToggling}
                onCheckedChange={handleToggleService}
                className="scale-90"
              />
            </div>
          ) : null}

          <Button
            variant="outline"
            size="sm"
            className="h-9 gap-2 px-3"
            onClick={() => setWebPasswordModalOpen(true)}
          >
            <SettingsIcon className="h-3.5 w-3.5" />
            <span className="text-xs">{t("密码")}</span>
          </Button>
        </div>
      </header>

      <WebPasswordModal
        open={webPasswordModalOpen}
        onOpenChange={setWebPasswordModalOpen}
      />
    </>
  );
}
