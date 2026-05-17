"use client";

import { useEffect, useState } from "react";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogFooter
} from "@/components/ui/dialog";
import { Button, buttonVariants } from "@/components/ui/button";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { useAppStore } from "@/lib/store/useAppStore";
import { appClient } from "@/lib/api/app-client";
import { toast } from "sonner";
import { ShieldAlert, ShieldCheck, KeyRound, Trash2, UsersRound, WalletCards } from "lucide-react";
import { useI18n } from "@/lib/i18n/provider";

interface WebPasswordModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

function billingModeLockReasonLabel(reason: string): string {
  switch (reason) {
    case "member_users":
      return "已存在成员账号";
    case "api_key_owners":
      return "已存在平台 Key 归属";
    case "wallet_balance":
      return "已存在成员钱包余额";
    case "wallet_ledger":
      return "已存在钱包流水";
    case "model_group_assignments":
      return "已存在模型组成员分配";
    case "request_charges":
      return "已存在请求扣费记录";
    default:
      return reason;
  }
}

/**
 * 函数 `WebPasswordModal`
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
export function WebPasswordModal({ open, onOpenChange }: WebPasswordModalProps) {
  const { t } = useI18n();
  const { appSettings, setAppSettings } = useAppStore();
  const { canAccessManagementRpc, isDesktopRuntime } = useRuntimeCapabilities();
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [webAuthMode, setWebAuthMode] = useState(appSettings.webAuthMode || "none");
  const [distributionEnabled, setDistributionEnabled] = useState(
    Boolean(appSettings.distributionEnabled)
  );
  const [isLoading, setIsLoading] = useState(false);
  const billingModeLock = appSettings.billingModeLock ?? {
    accountModeLocked: false,
    distributionLocked: false,
    reasons: [],
  };
  const accountModeLocked = Boolean(billingModeLock.accountModeLocked);
  const distributionLocked = Boolean(billingModeLock.distributionLocked);
  const distributionRequiresAccounts = webAuthMode !== "accounts";
  const distributionSwitchDisabled =
    !canAccessManagementRpc ||
    isLoading ||
    distributionRequiresAccounts ||
    (appSettings.distributionEnabled && distributionLocked);
  const lockReasonLabels = billingModeLock.reasons.map((reason) =>
    t(billingModeLockReasonLabel(reason))
  );

  const redirectToCurrentWebAuthBoundary = (nextMode: string) => {
    if (isDesktopRuntime || typeof window === "undefined") {
      return;
    }
    if (nextMode === "accounts" || nextMode === "password") {
      window.location.replace("/__login?force=1");
      return;
    }
    window.location.replace("/");
  };

  useEffect(() => {
    setWebAuthMode(appSettings.webAuthMode || "none");
    setDistributionEnabled(Boolean(appSettings.distributionEnabled));
  }, [appSettings.distributionEnabled, appSettings.webAuthMode]);

  useEffect(() => {
    if (!open) {
      setPassword("");
      setConfirmPassword("");
      return;
    }

    let cancelled = false;
    if (!canAccessManagementRpc) {
      return;
    }
    /**
     * 函数 `syncSettings`
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
    const syncSettings = async () => {
      try {
        const settings = await appClient.getSettings();
        if (!cancelled) {
          setAppSettings(settings);
        }
      } catch (err: unknown) {
        if (!cancelled) {
          toast.error(
            `${t("访问控制")} ${t("失败")}: ${err instanceof Error ? err.message : String(err)}`
          );
        }
      }
    };

    void syncSettings();

    return () => {
      cancelled = true;
    };
  }, [canAccessManagementRpc, open, setAppSettings]);

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
    if (!canAccessManagementRpc) {
      toast.info(t("访问密码"));
      return;
    }
    if (password && password !== confirmPassword) {
      toast.error(t("确认新密码"));
      return;
    }
    if (
      webAuthMode === "password" &&
      !appSettings.webAccessPasswordConfigured &&
      !password
    ) {
      toast.error(t("新密码"));
      return;
    }
    if (accountModeLocked && appSettings.webAuthMode === "accounts" && webAuthMode !== "accounts") {
      toast.error(t("已进入账号计费模式，不能从界面关闭账号系统。"));
      return;
    }
    if (distributionEnabled && webAuthMode !== "accounts") {
      toast.error(t("请先启用账号系统，再开启额度分发。"));
      return;
    }
    if (appSettings.distributionEnabled && distributionLocked && !distributionEnabled) {
      toast.error(t("已进入账号计费模式，不能从界面关闭额度分发。"));
      return;
    }

    setIsLoading(true);
    try {
      const previousMode = appSettings.webAuthMode || "none";
      const settings = await appClient.setSettings({
        webAuthMode,
        distributionEnabled,
        ...(password ? { webAccessPassword: password } : {}),
      });
      setAppSettings(settings);
      toast.success(t("保存"));
      onOpenChange(false);
      setPassword("");
      setConfirmPassword("");
      if (previousMode !== settings.webAuthMode) {
        redirectToCurrentWebAuthBoundary(settings.webAuthMode || "none");
      }
    } catch (err: unknown) {
      toast.error(`${t("保存")} ${t("失败")}: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * 函数 `handleClear`
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
  const handleClear = async () => {
    if (!canAccessManagementRpc) {
      toast.info(t("访问密码"));
      return;
    }
    setIsLoading(true);
    try {
      const settings = await appClient.setSettings({
        webAccessPassword: "",
        webAuthMode: webAuthMode === "password" ? "none" : webAuthMode,
      });
      setAppSettings(settings);
      toast.success(t("清除"));
      onOpenChange(false);
      setPassword("");
      setConfirmPassword("");
    } catch (err: unknown) {
      toast.error(`${t("清除")} ${t("失败")}: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="glass-card p-0 sm:max-w-[560px]">
        <DialogHeader className="border-b px-5 py-4">
          <div className="mb-1 flex items-center gap-3">
            <div className="rounded-xl bg-primary/10 p-2 text-primary">
              <ShieldCheck className="h-5 w-5" />
            </div>
            <div className="space-y-1">
              <DialogTitle>{t("访问控制")}</DialogTitle>
              <DialogDescription>
                {t("统一管理 Web 登录方式、访问密码和团队额度分发。")}
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        <div className="grid gap-4 px-5 py-4">
          {!canAccessManagementRpc ? (
            <Alert>
              <ShieldAlert />
              <AlertDescription>
                {t("当前运行环境暂不支持读取或保存访问密码。")}
              </AlertDescription>
            </Alert>
          ) : null}
          {webAuthMode === "accounts" ? (
            <Alert>
              <UsersRound />
              <AlertDescription>
                {appSettings.appUsersConfigured
                  ? t("账号系统已启用")
                  : t("账号系统未初始化，首次打开 Web 登录页时会创建管理员")}
              </AlertDescription>
            </Alert>
          ) : appSettings.webAccessPasswordConfigured ? (
            <Alert>
              <ShieldCheck />
              <AlertDescription>{t("当前已启用访问密码保护")}</AlertDescription>
            </Alert>
          ) : (
            <Alert>
              <ShieldAlert />
              <AlertDescription>
                {t("当前未设置访问密码，Web 管理页处于公开状态")}
              </AlertDescription>
            </Alert>
          )}

          <div className="grid gap-2">
            <Label htmlFor="web-auth-mode">{t("访问方式")}</Label>
            <Select
              value={webAuthMode}
              disabled={!canAccessManagementRpc || isLoading}
              onValueChange={(value) => {
                if (!value) return;
                if (accountModeLocked && value !== "accounts") return;
                setWebAuthMode(value);
                if (value !== "accounts") {
                  setDistributionEnabled(false);
                }
              }}
            >
              <SelectTrigger id="web-auth-mode" className="w-full">
                <SelectValue>
                  {(value) => {
                    const mode = String(value || "none");
                    if (mode === "accounts") return t("账号系统");
                    if (mode === "password") return t("访问密码");
                    return t("不启用");
                  }}
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                    <SelectGroup>
                <SelectItem value="none" disabled={accountModeLocked}>
                  {t("不启用")}
                </SelectItem>
                <SelectItem value="password" disabled={accountModeLocked}>
                  {t("访问密码")}
                </SelectItem>
                <SelectItem value="accounts">{t("账号系统")}</SelectItem>
                </SelectGroup>
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">
              {webAuthMode === "accounts"
                  ? t("适合多人使用：管理员维护成员账号，成员按归属钱包消费额度。")
                : webAuthMode === "password"
                  ? t("适合个人或小团队：所有访问者共用同一个访问密码。")
                  : t("公开访问不会拦截 Web 管理页，请只在本机可信环境使用。")}
              {accountModeLocked
                ? ` ${t("已进入账号计费模式，不能从界面关闭账号系统。")}`
                : ""}
            </p>
          </div>

          <Card size="sm">
            <CardContent className="flex items-center justify-between gap-4">
              <div className="flex items-center gap-3">
                <WalletCards className="h-4 w-4 text-muted-foreground" />
                <div>
                  <div className="text-sm font-medium">{t("额度分发")}</div>
                  <div className="text-xs text-muted-foreground">
                    {distributionRequiresAccounts
                      ? t("请先启用账号系统，再开启额度分发。")
                      : appSettings.distributionEnabled && distributionLocked
                        ? t("已进入账号计费模式，不能从界面关闭额度分发。")
                        : t("启用后平台 Key 需要归属到成员钱包")}
                  </div>
                </div>
              </div>
              <Switch
                checked={distributionEnabled}
                disabled={distributionSwitchDisabled}
                onCheckedChange={setDistributionEnabled}
              />
            </CardContent>
          </Card>

          {(accountModeLocked || distributionLocked) && lockReasonLabels.length > 0 ? (
            <Alert>
              <ShieldAlert />
              <AlertTitle>
                {t("已进入账号计费模式。为避免权限归属错乱和账务断层，不能从界面关闭账号系统或额度分发。")}
              </AlertTitle>
              <AlertDescription>
                {t("锁定原因")}: {lockReasonLabels.join("、")}
              </AlertDescription>
            </Alert>
          ) : null}

          {webAuthMode === "password" ? (
            <Card size="sm">
              <CardContent className="grid gap-3">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <KeyRound className="h-4 w-4 text-muted-foreground" />
                  {t("访问密码")}
                </div>
                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="grid gap-2">
                    <Label htmlFor="password">{t("新密码")}</Label>
                    <Input
                      id="password"
                      type="password"
                      placeholder={t("新密码")}
                      value={password}
                      disabled={!canAccessManagementRpc}
                      onChange={(e) => setPassword(e.target.value)}
                    />
                  </div>
                  <div className="grid gap-2">
                    <Label htmlFor="confirm">{t("确认新密码")}</Label>
                    <Input
                      id="confirm"
                      type="password"
                      placeholder={t("确认新密码")}
                      value={confirmPassword}
                      disabled={!canAccessManagementRpc}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                    />
                  </div>
                </div>
                <p className="text-xs text-muted-foreground">
                  {appSettings.webAccessPasswordConfigured
                    ? t("留空保存时会保留当前访问密码。")
                    : t("首次启用访问密码模式必须填写密码。")}
                </p>
              </CardContent>
            </Card>
          ) : null}
        </div>

        <DialogFooter className="m-0 rounded-b-xl border-t bg-muted/40 px-5 py-4">
          {appSettings.webAccessPasswordConfigured && (
            <Button variant="ghost" onClick={handleClear} disabled={!canAccessManagementRpc || isLoading} className="text-destructive hover:text-destructive hover:bg-destructive/10">
              <Trash2 className="h-4 w-4 mr-2" /> {t("清除")}
            </Button>
          )}
          <DialogClose
            className={buttonVariants({ variant: "outline" })}
            type="button"
          >
            {t("取消")}
          </DialogClose>
          <Button onClick={handleSave} disabled={!canAccessManagementRpc || isLoading}>
            {isLoading ? `${t("保存")}...` : t("保存访问控制")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
