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
import {
  getAppErrorMessage,
  isCommandMissingError,
  unwrapRpcPayload,
} from "./transport-errors";
export { getAppErrorMessage, isCommandMissingError } from "./transport-errors";
import { createWebCommandMap } from "./transport-web-commands";
import type { InvokeParams, WebCommandDescriptor } from "./transport-web-commands";
import { postJsonRpc } from "./rpc-http";

const DEFAULT_WEB_RPC_BASE_URL = "/api/rpc";
const DEFAULT_RUNTIME_PROBE_URL = "/api/runtime";
const CONFIGURED_WEB_RPC_BASE_URL = normalizeRpcBaseUrl(
  process.env.NEXT_PUBLIC_CODEXMANAGER_RPC_BASE_URL
);

let runtimeCapabilitiesCache: RuntimeCapabilities | null = null;
let runtimeCapabilitiesPromise: Promise<RuntimeCapabilities> | null = null;

const WEB_COMMAND_MAP: Record<string, WebCommandDescriptor> =
  createWebCommandMap(postWebRpc);

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

  return postJsonRpc<T>(
    fetchWithRetry,
    runtimeCapabilities.rpcBaseUrl,
    rpcMethod,
    params ?? {},
    options
  );
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
  return unwrapRpcPayload<T>(response);
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
  return postJsonRpc<T>(
    fetchWithRetry,
    `http://${addr}/rpc`,
    "requestlog/list",
    {
      query: params.query || "",
      statusFilter: params.statusFilter || "all",
      page: params.page ?? 1,
      pageSize: params.pageSize ?? 20,
    },
    options
  );
}
