import { invoke, invokeFirst } from "./transport";
import {
  AppSettings,
  CodexAccountSwitchResult,
  CodexLatestVersionInfo,
} from "../../types";
import { normalizeAppSettings } from "./normalize";
import {
  readUpdateActionResult,
  readUpdateCheckResult,
  readUpdatePrepareResult,
  readUpdateStatusResult,
  UpdateActionResult,
  UpdateCheckResult,
  UpdatePrepareResult,
  UpdateStatusResult,
} from "./app-updates";
import {
  GatewayConcurrencyRecommendation,
  readGatewayConcurrencyRecommendation,
} from "./gateway-settings";

export const appClient = {
  async getSettings(): Promise<AppSettings> {
    const result = await invoke<unknown>("app_settings_get");
    return normalizeAppSettings(result);
  },
  async setSettings(patch: Partial<AppSettings>): Promise<AppSettings> {
    const result = await invoke<unknown>("app_settings_set", { patch });
    return normalizeAppSettings(result);
  },
  async getGatewayConcurrencyRecommendation(): Promise<GatewayConcurrencyRecommendation> {
    const result = await invoke<unknown>("service_gateway_concurrency_recommend_get");
    return readGatewayConcurrencyRecommendation(result);
  },
  getCodexLatestVersion: () =>
    invoke<CodexLatestVersionInfo>("service_gateway_codex_latest_version_get"),

  getCloseToTray: () => invoke<boolean>("app_close_to_tray_on_close_get"),
  setCloseToTray: (enabled: boolean) =>
    invoke("app_close_to_tray_on_close_set", { enabled }),

  openInBrowser: (url: string) => invoke("open_in_browser", { url }),
  openInFileManager: (path: string) => invoke("open_in_file_manager", { path }),
  openUpdateLogsDir: (assetPath?: string) =>
    invoke("app_update_open_logs_dir", { assetPath: assetPath || null }),
  switchCodexAccount: (accountId: string) =>
    invoke<CodexAccountSwitchResult>("local_codex_switch", { accountId }),
  enableCodexLocalProxyMode: () =>
    invoke<CodexAccountSwitchResult>("enable_local_proxy_mode"),

  async checkUpdate(): Promise<UpdateCheckResult> {
    const result = await invokeFirst<unknown>(
      ["app_update_check", "update_check", "check_update"],
      {}
    );
    return readUpdateCheckResult(result);
  },
  async prepareUpdate(
    payload: Record<string, unknown> = {}
  ): Promise<UpdatePrepareResult> {
    const result = await invokeFirst<unknown>(
      ["app_update_prepare", "update_download", "download_update"],
      payload
    );
    return readUpdatePrepareResult(result);
  },
  async launchInstaller(
    payload: Record<string, unknown> = {}
  ): Promise<UpdateActionResult> {
    const result = await invokeFirst<unknown>(
      ["app_update_launch_installer", "update_install", "install_update"],
      payload
    );
    return readUpdateActionResult(result);
  },
  async applyUpdatePortable(
    payload: Record<string, unknown> = {}
  ): Promise<UpdateActionResult> {
    const result = await invokeFirst<unknown>(
      ["app_update_apply_portable", "update_restart", "restart_update"],
      payload
    );
    return readUpdateActionResult(result);
  },
  async getStatus(): Promise<UpdateStatusResult> {
    const result = await invokeFirst<unknown>(["app_update_status", "update_status"], {});
    return readUpdateStatusResult(result);
  },
};
