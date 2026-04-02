import { invoke as tauriInvoke, isTauri as tauriIsTauri } from "@tauri-apps/api/core";
import { fetchWithRetry, runWithControl, RequestOptions } from "../utils/request";
import {
  buildDesktopRuntimeCapabilities,
  buildUnsupportedWebCapabilities,
  buildWebGatewayRuntimeCapabilities,
  DEFAULT_UNSUPPORTED_WEB_REASON,
  normalizeRpcBaseUrl,
  normalizeRuntimeCapabilities,
} from "../runtime/runtime-capabilities";
import { useAppStore } from "../store/useAppStore";
import { RuntimeCapabilities } from "../../types";

type InvokeParams = Record<string, unknown>;

type WebCommandDescriptor = {
  rpcMethod?: string;
  mapParams?: (params?: InvokeParams) => InvokeParams;
  direct?: (params?: InvokeParams, options?: RequestOptions) => Promise<unknown>;
};

const DEFAULT_WEB_RPC_BASE_URL = "/api/rpc";
const DEFAULT_RUNTIME_PROBE_URL = "/api/runtime";
const CONFIGURED_WEB_RPC_BASE_URL = normalizeRpcBaseUrl(
  process.env.NEXT_PUBLIC_CODEXMANAGER_RPC_BASE_URL
);

let runtimeCapabilitiesCache: RuntimeCapabilities | null = null;
let runtimeCapabilitiesPromise: Promise<RuntimeCapabilities> | null = null;

const WEB_COMMAND_MAP: Record<string, WebCommandDescriptor> = {
  app_settings_get: { rpcMethod: "appSettings/get" },
  app_settings_set: {
    rpcMethod: "appSettings/set",
    mapParams: (params) => asRecord(asRecord(params)?.patch) ?? {},
  },
  service_initialize: { rpcMethod: "initialize" },
  service_startup_snapshot: { rpcMethod: "startup/snapshot" },
  service_account_list: { rpcMethod: "account/list" },
  service_account_delete: { rpcMethod: "account/delete" },
  service_account_delete_many: { rpcMethod: "account/deleteMany" },
  service_account_delete_unavailable_free: {
    rpcMethod: "account/deleteUnavailableFree",
  },
  service_account_update: { rpcMethod: "account/update" },
  service_account_import: { rpcMethod: "account/import" },
  service_account_import_by_file: {
    direct: () => pickImportFilesFromBrowser(false),
  },
  service_account_import_by_directory: {
    direct: () => pickImportFilesFromBrowser(true),
  },
  service_account_export_by_account_files: {
    direct: (params, options) => exportAccountsViaBrowser(asRecord(params), options),
  },
  service_usage_read: { rpcMethod: "account/usage/read" },
  service_usage_list: { rpcMethod: "account/usage/list" },
  service_usage_refresh: { rpcMethod: "account/usage/refresh" },
  service_usage_aggregate: { rpcMethod: "account/usage/aggregate" },
  service_aggregate_api_list: { rpcMethod: "aggregateApi/list" },
  service_aggregate_api_create: { rpcMethod: "aggregateApi/create" },
  service_aggregate_api_update: { rpcMethod: "aggregateApi/update" },
  service_aggregate_api_delete: { rpcMethod: "aggregateApi/delete" },
  service_aggregate_api_read_secret: { rpcMethod: "aggregateApi/readSecret" },
  service_aggregate_api_test_connection: {
    rpcMethod: "aggregateApi/testConnection",
  },
  service_login_start: {
    rpcMethod: "account/login/start",
    mapParams: (params) => ({
      ...(params ?? {}),
      type:
        typeof params?.loginType === "string" && params.loginType.trim()
          ? params.loginType
          : "chatgpt",
      openBrowser: false,
    }),
  },
  service_login_status: { rpcMethod: "account/login/status" },
  service_login_complete: { rpcMethod: "account/login/complete" },
  service_login_chatgpt_auth_tokens: {
    rpcMethod: "account/login/start",
    mapParams: (params) => ({
      ...(params ?? {}),
      type: "chatgptAuthTokens",
    }),
  },
  service_account_read: { rpcMethod: "account/read" },
  service_account_logout: { rpcMethod: "account/logout" },
  service_chatgpt_auth_tokens_refresh: {
    rpcMethod: "account/chatgptAuthTokens/refresh",
  },
  service_apikey_list: { rpcMethod: "apikey/list" },
  service_apikey_create: { rpcMethod: "apikey/create" },
  service_apikey_usage_stats: { rpcMethod: "apikey/usageStats" },
  service_apikey_delete: {
    rpcMethod: "apikey/delete",
    mapParams: mapKeyIdToId,
  },
  service_apikey_update_model: {
    rpcMethod: "apikey/updateModel",
    mapParams: mapKeyIdToId,
  },
  service_apikey_disable: {
    rpcMethod: "apikey/disable",
    mapParams: mapKeyIdToId,
  },
  service_apikey_enable: {
    rpcMethod: "apikey/enable",
    mapParams: mapKeyIdToId,
  },
  service_apikey_models: { rpcMethod: "apikey/models" },
  service_apikey_read_secret: {
    rpcMethod: "apikey/readSecret",
    mapParams: mapKeyIdToId,
  },
  service_gateway_transport_get: { rpcMethod: "gateway/transport/get" },
  service_gateway_transport_set: { rpcMethod: "gateway/transport/set" },
  service_gateway_upstream_proxy_get: { rpcMethod: "gateway/upstreamProxy/get" },
  service_gateway_upstream_proxy_set: { rpcMethod: "gateway/upstreamProxy/set" },
  service_gateway_route_strategy_get: { rpcMethod: "gateway/routeStrategy/get" },
  service_gateway_route_strategy_set: { rpcMethod: "gateway/routeStrategy/set" },
  service_gateway_manual_account_get: { rpcMethod: "gateway/manualAccount/get" },
  service_gateway_manual_account_set: { rpcMethod: "gateway/manualAccount/set" },
  service_gateway_manual_account_clear: {
    rpcMethod: "gateway/manualAccount/clear",
  },
  service_gateway_background_tasks_get: {
    rpcMethod: "gateway/backgroundTasks/get",
  },
  service_gateway_background_tasks_set: {
    rpcMethod: "gateway/backgroundTasks/set",
  },
  service_gateway_concurrency_recommend_get: {
    rpcMethod: "gateway/concurrencyRecommendation/get",
  },
  service_requestlog_list: { rpcMethod: "requestlog/list" },
  service_requestlog_summary: { rpcMethod: "requestlog/summary" },
  service_requestlog_clear: { rpcMethod: "requestlog/clear" },
  service_requestlog_today_summary: { rpcMethod: "requestlog/today_summary" },
  service_plugin_catalog_list: { rpcMethod: "plugin/catalog/list" },
  service_plugin_catalog_refresh: { rpcMethod: "plugin/catalog/refresh" },
  service_plugin_install: { rpcMethod: "plugin/install" },
  service_plugin_update: { rpcMethod: "plugin/update" },
  service_plugin_uninstall: { rpcMethod: "plugin/uninstall" },
  service_plugin_list: { rpcMethod: "plugin/list" },
  service_plugin_enable: { rpcMethod: "plugin/enable" },
  service_plugin_disable: { rpcMethod: "plugin/disable" },
  service_plugin_tasks_update: { rpcMethod: "plugin/tasks/update" },
  service_plugin_tasks_list: { rpcMethod: "plugin/tasks/list" },
  service_plugin_tasks_run: { rpcMethod: "plugin/tasks/run" },
  service_plugin_logs_list: { rpcMethod: "plugin/logs/list" },
  service_listen_config_get: { rpcMethod: "service/listenConfig/get" },
  service_listen_config_set: { rpcMethod: "service/listenConfig/set" },
  open_in_browser: {
    direct: async (params) => {
      const url = typeof params?.url === "string" ? params.url.trim() : "";
      if (!url) {
        throw new Error("缺少浏览器跳转地址");
      }
      if (typeof window === "undefined") {
        throw new Error("当前环境不支持打开浏览器");
      }
      window.open(url, "_blank", "noopener,noreferrer");
      return { ok: true };
    },
  },
  open_in_file_manager: {
    direct: async () => {
      throw new Error("当前环境不支持打开本地目录");
    },
  },
  app_update_open_logs_dir: {
    direct: async () => {
      throw new Error("当前环境不支持打开更新日志目录");
    },
  },
};

/**
 * 函数 `asRecord`
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
function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

/**
 * 函数 `cacheRuntimeCapabilities`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - runtimeCapabilities: 参数 runtimeCapabilities
 *
 * # 返回
 * 返回函数执行结果
 */
function cacheRuntimeCapabilities(
  runtimeCapabilities: RuntimeCapabilities
): RuntimeCapabilities {
  runtimeCapabilitiesCache = runtimeCapabilities;
  return runtimeCapabilities;
}

/**
 * 函数 `probeRuntimeCapabilities`
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
async function probeRuntimeCapabilities(): Promise<RuntimeCapabilities | null> {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    const response = await fetchWithRetry(
      DEFAULT_RUNTIME_PROBE_URL,
      {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      },
      {
        timeoutMs: 1500,
        retries: 0,
        shouldRetryStatus: () => false,
      }
    );
    if (!response.ok) {
      return null;
    }
    return normalizeRuntimeCapabilities(
      await response.json(),
      CONFIGURED_WEB_RPC_BASE_URL || DEFAULT_WEB_RPC_BASE_URL
    );
  } catch {
    return null;
  }
}

/**
 * 函数 `getCachedRuntimeCapabilities`
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
export function getCachedRuntimeCapabilities(): RuntimeCapabilities | null {
  if (isTauriRuntime()) {
    return runtimeCapabilitiesCache ?? buildDesktopRuntimeCapabilities();
  }
  return runtimeCapabilitiesCache;
}

/**
 * 函数 `loadRuntimeCapabilities`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - force: 参数 force
 *
 * # 返回
 * 返回函数执行结果
 */
export async function loadRuntimeCapabilities(
  force = false
): Promise<RuntimeCapabilities> {
  if (isTauriRuntime()) {
    return cacheRuntimeCapabilities(buildDesktopRuntimeCapabilities());
  }
  if (!force && runtimeCapabilitiesCache) {
    return runtimeCapabilitiesCache;
  }
  if (!force && runtimeCapabilitiesPromise) {
    return runtimeCapabilitiesPromise;
  }

  runtimeCapabilitiesPromise = (async () => {
    const probedRuntime = await probeRuntimeCapabilities();
    if (probedRuntime) {
      return cacheRuntimeCapabilities(probedRuntime);
    }
    if (CONFIGURED_WEB_RPC_BASE_URL) {
      return cacheRuntimeCapabilities(
        buildWebGatewayRuntimeCapabilities(CONFIGURED_WEB_RPC_BASE_URL)
      );
    }
    return cacheRuntimeCapabilities(
      buildUnsupportedWebCapabilities(
        DEFAULT_UNSUPPORTED_WEB_REASON,
        DEFAULT_WEB_RPC_BASE_URL
      )
    );
  })();

  try {
    return await runtimeCapabilitiesPromise;
  } finally {
    runtimeCapabilitiesPromise = null;
  }
}

/**
 * 函数 `getAppErrorMessage`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - error: 参数 error
 * - fallback: 参数 fallback
 *
 * # 返回
 * 返回函数执行结果
 */
export function getAppErrorMessage(
  error: unknown,
  fallback = "操作失败"
): string {
  if (error instanceof Error) {
    const nested = getAppErrorMessage(error.message, "");
    return nested || fallback;
  }

  const businessMessage = resolveBusinessErrorMessage(error);
  if (businessMessage) return businessMessage;

  const rpcMessage = resolveRpcErrorMessage(error).trim();
  if (!rpcMessage || rpcMessage === "null" || rpcMessage === "undefined") {
    return fallback;
  }
  return rpcMessage;
}

/**
 * 函数 `resolveRpcErrorMessage`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - error: 参数 error
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveRpcErrorMessage(error: unknown): string {
  if (typeof error === "string") return error;
  const record = asRecord(error);
  if (record?.message && typeof record.message === "string") {
    return record.message;
  }
  return error ? JSON.stringify(error) : "RPC 请求失败";
}

/**
 * 函数 `throwIfBusinessError`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - payload: 参数 payload
 *
 * # 返回
 * 返回函数执行结果
 */
function throwIfBusinessError(payload: unknown): void {
  const msg = resolveBusinessErrorMessage(payload);
  if (msg) throw new Error(msg);
}

/**
 * 函数 `invokeWebRpc`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - method: 参数 method
 * - params?: 参数 params?
 * - options: 参数 options
 *
 * # 返回
 * 返回函数执行结果
 */
async function invokeWebRpc<T>(
  method: string,
  params?: InvokeParams,
  options: RequestOptions = {}
): Promise<T> {
  const descriptor = WEB_COMMAND_MAP[method];
  if (!descriptor) {
    throw new Error("当前 Web / Docker 版暂不支持该操作");
  }
  if (descriptor.direct) {
    return (await descriptor.direct(params, options)) as T;
  }
  if (!descriptor.rpcMethod) {
    throw new Error("当前 Web / Docker 版暂不支持该操作");
  }
  return postWebRpc<T>(
    descriptor.rpcMethod,
    descriptor.mapParams ? descriptor.mapParams(params) : params ?? {},
    options
  );
}

/**
 * 函数 `postWebRpc`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - rpcMethod: 参数 rpcMethod
 * - params?: 参数 params?
 * - options: 参数 options
 *
 * # 返回
 * 返回函数执行结果
 */
async function postWebRpc<T>(
  rpcMethod: string,
  params?: InvokeParams,
  options: RequestOptions = {}
): Promise<T> {
  const runtimeCapabilities = await loadRuntimeCapabilities();
  if (runtimeCapabilities.mode === "unsupported-web") {
    throw new Error(
      runtimeCapabilities.unsupportedReason || DEFAULT_UNSUPPORTED_WEB_REASON
    );
  }

  const response = await fetchWithRetry(
    runtimeCapabilities.rpcBaseUrl,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: Date.now(),
        method: rpcMethod,
        params: params ?? {},
      }),
    },
    options
  );

  if (!response.ok) throw new Error(`RPC 请求失败（HTTP ${response.status}）`);

  /**
   * 函数 `payload`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - await response.json(): 参数 await response.json()
   *
   * # 返回
   * 返回函数执行结果
   */
  const payload = (await response.json()) as unknown;
  const responseRecord = asRecord(payload);
  if (responseRecord && "error" in responseRecord) {
    throw new Error(resolveRpcErrorMessage(responseRecord.error));
  }
  if (responseRecord && "result" in responseRecord) {
    const result = responseRecord.result as T;
    throwIfBusinessError(result);
    return result;
  }

  throwIfBusinessError(payload);
  return payload as T;
}

/**
 * 函数 `isTauriRuntime`
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
export function isTauriRuntime(): boolean {
  if (typeof window === "undefined") {
    return false;
  }

  const runtime = globalThis as typeof globalThis & {
    __TAURI__?: unknown;
    __TAURI_INTERNALS__?: { invoke?: unknown };
  };

  return (
    tauriIsTauri() ||
    Boolean(runtime.__TAURI_INTERNALS__?.invoke) ||
    Boolean(runtime.__TAURI__)
  );
}

/**
 * 函数 `withAddr`
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
export function withAddr(
  params: Record<string, unknown> = {}
): Record<string, unknown> {
  const addr = useAppStore.getState().serviceStatus.addr;
  return {
    addr: addr || null,
    ...params,
  };
}

/**
 * 函数 `isCommandMissingError`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - err: 参数 err
 *
 * # 返回
 * 返回函数执行结果
 */
export function isCommandMissingError(err: unknown): boolean {
  const msg = getAppErrorMessage(err, "").toLowerCase();
  return (
    msg.includes("unknown command") ||
    msg.includes("not found") ||
    msg.includes("is not a registered")
  );
}

/**
 * 函数 `invokeFirst`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - methods: 参数 methods
 * - params?: 参数 params?
 * - options: 参数 options
 *
 * # 返回
 * 返回函数执行结果
 */
export async function invokeFirst<T>(
  methods: string[],
  params?: Record<string, unknown>,
  options: RequestOptions = {}
): Promise<T> {
  let lastErr: unknown;
  for (const method of methods) {
    try {
      return await invoke<T>(method, params, options);
    } catch (err) {
      lastErr = err;
      if (!isCommandMissingError(err)) {
        throw err;
      }
    }
  }
  throw lastErr || new Error("未配置可用命令");
}

/**
 * 函数 `invoke`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - method: 参数 method
 * - params?: 参数 params?
 * - options: 参数 options
 *
 * # 返回
 * 返回函数执行结果
 */
export async function invoke<T>(
  method: string,
  params?: InvokeParams,
  options: RequestOptions = {}
): Promise<T> {
  if (!isTauriRuntime()) {
    return invokeWebRpc(method, params, options);
  }

  const response = await runWithControl<unknown>(
    () => tauriInvoke(method, params || {}),
    options
  );

  const responseRecord = asRecord(response);
  if (responseRecord && "error" in responseRecord) {
    const error = responseRecord.error;
    throw new Error(
      typeof error === "string"
        ? error
        : asRecord(error)?.message
          ? String(asRecord(error)?.message)
          : JSON.stringify(error)
    );
  }

  if (responseRecord && "result" in responseRecord) {
    const payload = responseRecord.result as T;
    throwIfBusinessError(payload);
    return payload;
  }
  
  throwIfBusinessError(response);
  return response as T;
}

/**
 * 函数 `resolveBusinessErrorMessage`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - payload: 参数 payload
 *
 * # 返回
 * 返回函数执行结果
 */
function resolveBusinessErrorMessage(payload: unknown): string {
  const source = asRecord(payload);
  if (!source) return "";
  const error = source.error;
  if (source.ok === false) {
    return typeof error === "string"
      ? error
      : asRecord(error)?.message
        ? String(asRecord(error)?.message)
        : "操作失败";
  }
  if (error) {
    return typeof error === "string"
      ? error
      : asRecord(error)?.message
        ? String(asRecord(error)?.message)
        : "";
  }
  return "";
}

/**
 * 函数 `mapKeyIdToId`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - params?: 参数 params?
 *
 * # 返回
 * 返回函数执行结果
 */
function mapKeyIdToId(params?: InvokeParams): InvokeParams {
  const source = params ?? {};
  const keyId =
    typeof source.keyId === "string" && source.keyId.trim()
      ? source.keyId.trim()
      : undefined;
  if (!keyId) {
    return source;
  }
  return {
    ...source,
    id: keyId,
  };
}

/**
 * 函数 `isSupportedBrowserImportFile`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - file: 参数 file
 *
 * # 返回
 * 返回函数执行结果
 */
function isSupportedBrowserImportFile(file: File): boolean {
  const normalizedName = String(file.name || "").trim().toLowerCase();
  return normalizedName.endsWith(".json") || normalizedName.endsWith(".txt");
}

/**
 * 函数 `pickImportFilesFromBrowser`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - directory: 参数 directory
 *
 * # 返回
 * 返回函数执行结果
 */
async function pickImportFilesFromBrowser(directory: boolean): Promise<unknown> {
  if (typeof document === "undefined") {
    throw new Error("当前环境不支持浏览器文件选择");
  }

  const input = document.createElement("input");
  input.type = "file";
  input.accept = ".json,.txt,application/json,text/plain";
  input.multiple = true;
  if (directory) {
    const directoryInput = input as HTMLInputElement & {
      directory?: boolean;
      webkitdirectory?: boolean;
    };
    directoryInput.directory = true;
    directoryInput.webkitdirectory = true;
  }
  input.style.display = "none";
  document.body.appendChild(input);

  return await new Promise<unknown>((resolve, reject) => {
    let finished = false;

    /**
     * 函数 `cleanup`
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
    const cleanup = () => {
      input.removeEventListener("change", handleChange);
      input.removeEventListener("cancel", handleCancel as EventListener);
      input.remove();
    };

    /**
     * 函数 `finish`
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
    const finish = (value: unknown) => {
      if (finished) return;
      finished = true;
      cleanup();
      resolve(value);
    };

    /**
     * 函数 `fail`
     *
     * 作者: gaohongshun
     *
     * 时间: 2026-04-02
     *
     * # 参数
     * - error: 参数 error
     *
     * # 返回
     * 返回函数执行结果
     */
    const fail = (error: unknown) => {
      if (finished) return;
      finished = true;
      cleanup();
      reject(error);
    };

    /**
     * 函数 `handleCancel`
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
    const handleCancel = () => {
      finish({
        ok: true,
        canceled: true,
      });
    };

    /**
     * 函数 `handleChange`
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
    const handleChange = async () => {
      try {
        const files = Array.from(input.files ?? []);
        if (!files.length) {
          handleCancel();
          return;
        }

        const importableFiles = files.filter(isSupportedBrowserImportFile);
        if (!importableFiles.length) {
          fail(
            new Error(
              directory
                ? "所选目录中没有可导入的 .json 或 .txt 文件"
                : "请选择 .json 或 .txt 文件"
            )
          );
          return;
        }

        const fileEntries = await Promise.all(
          importableFiles.map(async (file) => {
            const content = await file.text();
            const relativePath =
              (file as File & { webkitRelativePath?: string }).webkitRelativePath ||
              file.name;
            return {
              content,
              path: relativePath || file.name,
            };
          })
        );
        const nonEmptyEntries = fileEntries.filter(
          (entry) => entry.content.trim().length > 0
        );
        if (!nonEmptyEntries.length) {
          fail(new Error("未在所选文件中找到可导入内容"));
          return;
        }

        const filePaths = nonEmptyEntries.map((entry) => entry.path);
        const contents = nonEmptyEntries.map((entry) => entry.content);
        const directorySourcePath = filePaths[0] || fileEntries[0]?.path || "";
        const directoryPath = directory
          ? directorySourcePath.split("/")[0] || directorySourcePath.split("\\")[0] || ""
          : "";

        finish({
          ok: true,
          canceled: false,
          directoryPath,
          fileCount: importableFiles.length,
          filePaths,
          contents,
        });
      } catch (error) {
        fail(error);
      }
    };

    input.addEventListener("change", handleChange);
    input.addEventListener("cancel", handleCancel as EventListener);
    input.click();
  });
}

/**
 * 函数 `exportAccountsViaBrowser`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - options: 参数 options
 *
 * # 返回
 * 返回函数执行结果
 */
async function exportAccountsViaBrowser(
  params: Record<string, unknown> | null = null,
  options: RequestOptions = {}
): Promise<unknown> {
  if (typeof document === "undefined") {
    throw new Error("当前环境不支持浏览器导出");
  }

  const selectedAccountIds = Array.isArray(params?.selectedAccountIds)
    ? params.selectedAccountIds
        .map((item) => String(item || "").trim())
        .filter(Boolean)
    : [];
  const exportMode =
    typeof params?.exportMode === "string" && params.exportMode.trim()
      ? params.exportMode.trim()
      : "multiple";
  const payload =
    asRecord(
      await postWebRpc<unknown>(
        "account/exportData",
        {
          selectedAccountIds,
          exportMode,
        },
        options
      )
    ) ?? {};
  const files = Array.isArray(payload.files)
    ? payload.files
        .map((item) => asRecord(item))
        .filter((item): item is Record<string, unknown> => item !== null)
    : [];

  for (const item of files) {
    const fileName =
      typeof item.fileName === "string" && item.fileName.trim()
        ? item.fileName.trim()
        : "account.json";
    const content = typeof item.content === "string" ? item.content : "";
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
  }

  return {
    ok: true,
    canceled: false,
    exported:
      typeof payload.exported === "number" ? payload.exported : files.length,
    outputDir: "browser-download",
  };
}

/**
 * 函数 `requestlogListViaHttpRpc`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * - params: 参数 params
 * - addr: 参数 addr
 * - options: 参数 options
 *
 * # 返回
 * 返回函数执行结果
 */
export async function requestlogListViaHttpRpc<T>(
  params: {
    query?: string;
    statusFilter?: string;
    page?: number;
    pageSize?: number;
  },
  addr: string,
  options: RequestOptions = {}
): Promise<T> {
  // Desktop environment should use Tauri invoke for reliability
  if (isTauriRuntime()) {
    return invoke<T>(
      "service_requestlog_list",
      {
        query: params.query || "",
        statusFilter: params.statusFilter || "all",
        page: params.page ?? 1,
        pageSize: params.pageSize ?? 20,
        addr,
      },
      options
    );
  }

  // Fallback for web mode if needed (though not primary for this app)
  const body = JSON.stringify({
    jsonrpc: "2.0",
    id: Date.now(),
    method: "requestlog/list",
    params: {
      query: params.query || "",
      statusFilter: params.statusFilter || "all",
      page: params.page ?? 1,
      pageSize: params.pageSize ?? 20,
    },
  });

  const response = await fetchWithRetry(
    `http://${addr}/rpc`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    },
    options
  );

  if (!response.ok) throw new Error(`RPC 请求失败（HTTP ${response.status}）`);
  /**
   * 函数 `payload`
   *
   * 作者: gaohongshun
   *
   * 时间: 2026-04-02
   *
   * # 参数
   * - await response.json(): 参数 await response.json()
   *
   * # 返回
   * 返回函数执行结果
   */
  const payload = (await response.json()) as Record<string, unknown>;
  return ((payload.result ?? payload) as T);
}
