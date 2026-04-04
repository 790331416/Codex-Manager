import { invoke, withAddr } from "./transport";
import {
  normalizeAppSettings,
  normalizeGatewayErrorLogs,
  normalizeRequestLogFilterSummary,
  normalizeRequestLogListResult,
  normalizeStartupSnapshot,
  normalizeTodaySummary,
} from "./normalize";
import {
  BackgroundTaskSettings,
  GatewayErrorLog,
  RequestLogFilterSummary,
  RequestLogListResult,
  RequestLogTodaySummary,
  ServiceInitializationResult,
  StartupSnapshot,
} from "../../types";
import { readInitializeResult } from "@/lib/utils/service";

/**
 * 函数 `readStringField`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - payload: 参数 payload
 * - key: 参数 key
 *
 * # 返回
 * 返回函数执行结果
 */
function readStringField(payload: unknown, key: string): string {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return "";
  }
  /**
   * 函数 `value`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - payload as Record<string, unknown>: 参数 payload as Record<string, unknown>
   *
   * # 返回
   * 返回函数执行结果
   */
  const value = (payload as Record<string, unknown>)[key];
  return typeof value === "string" ? value.trim() : "";
}

export const serviceClient = {
  start: (addr?: string) => invoke("service_start", { addr }),
  stop: () => invoke("service_stop"),
  async initialize(addr?: string): Promise<ServiceInitializationResult> {
    const result = await invoke<unknown>(
      "service_initialize",
      addr ? { addr } : withAddr()
    );
    return readInitializeResult(result);
  },
  async getStartupSnapshot(
    params?: Record<string, unknown>
  ): Promise<StartupSnapshot> {
    const result = await invoke<unknown>(
      "service_startup_snapshot",
      withAddr(params)
    );
    return normalizeStartupSnapshot(result);
  },

  getGatewayTransport: () => invoke<unknown>("service_gateway_transport_get", withAddr()),
  setGatewayTransport: (settings: Record<string, unknown>) =>
    invoke("service_gateway_transport_set", withAddr(settings)),
  getUpstreamProxy: () =>
    invoke<string>("service_gateway_upstream_proxy_get", withAddr()),
  setUpstreamProxy: (proxyUrl: string) =>
    invoke("service_gateway_upstream_proxy_set", withAddr({ proxyUrl })),
  getRouteStrategy: () =>
    invoke<string>("service_gateway_route_strategy_get", withAddr()),
  setRouteStrategy: (strategy: string) =>
    invoke("service_gateway_route_strategy_set", withAddr({ strategy })),
  async getManualPreferredAccountId(): Promise<string> {
    const result = await invoke<unknown>("service_gateway_manual_account_get", withAddr());
    return readStringField(result, "accountId");
  },
  setManualPreferredAccount: (accountId: string) =>
    invoke("service_gateway_manual_account_set", withAddr({ accountId })),
  clearManualPreferredAccount: () =>
    invoke("service_gateway_manual_account_clear", withAddr()),

  getBackgroundTasks: () =>
    invoke<BackgroundTaskSettings>("service_gateway_background_tasks_get", withAddr()),
  setBackgroundTasks: (settings: BackgroundTaskSettings) =>
    invoke(
      "service_gateway_background_tasks_set",
      withAddr({ ...(settings as unknown as Record<string, unknown>) })
    ),
  getConcurrencyRecommendation: () =>
    invoke<unknown>("service_gateway_concurrency_recommend_get", withAddr()),

  async listRequestLogs(params?: {
    query?: string;
    statusFilter?: string;
    page?: number;
    pageSize?: number;
  }): Promise<RequestLogListResult> {
    const result = await invoke<unknown>(
      "service_requestlog_list",
      withAddr({
        query: params?.query || "",
        statusFilter: params?.statusFilter || "all",
        page: params?.page ?? 1,
        pageSize: params?.pageSize ?? 20,
      })
    );
    return normalizeRequestLogListResult(result);
  },
  async getRequestLogSummary(params?: {
    query?: string;
    statusFilter?: string;
  }): Promise<RequestLogFilterSummary> {
    const result = await invoke<unknown>(
      "service_requestlog_summary",
      withAddr({
        query: params?.query || "",
        statusFilter: params?.statusFilter || "all",
      })
    );
    return normalizeRequestLogFilterSummary(result);
  },
  async listGatewayErrorLogs(limit = 50): Promise<GatewayErrorLog[]> {
    const result = await invoke<unknown>(
      "service_requestlog_error_list",
      withAddr({ limit })
    );
    return normalizeGatewayErrorLogs(result);
  },
  clearGatewayErrorLogs: () =>
    invoke("service_requestlog_error_clear", withAddr()),
  clearRequestLogs: () => invoke("service_requestlog_clear", withAddr()),
  async getTodaySummary(): Promise<RequestLogTodaySummary> {
    const result = await invoke<unknown>(
      "service_requestlog_today_summary",
      withAddr()
    );
    return normalizeTodaySummary(result);
  },

  getListenConfig: () => invoke<unknown>("service_listen_config_get", withAddr()),
  setListenConfig: (mode: string) =>
    invoke("service_listen_config_set", withAddr({ mode })),

  getEnvOverrides: async () => {
    const result = await invoke<unknown>("app_settings_get");
    return normalizeAppSettings(result).envOverrides;
  },
};
