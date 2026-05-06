function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function readStringField(payload: unknown, key: string, fallback = ""): string {
  const source = asRecord(payload);
  const value = source?.[key];
  return typeof value === "string" ? value.trim() : fallback;
}

function readBooleanField(
  payload: unknown,
  key: string,
  fallback = false
): boolean {
  const source = asRecord(payload);
  const value = source?.[key];
  return typeof value === "boolean" ? value : fallback;
}

function readNumberField(payload: unknown, key: string, fallback = 0): number {
  const source = asRecord(payload);
  const value = source?.[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function readStringArrayField(payload: unknown, key: string): string[] {
  const source = asRecord(payload);
  const value = source?.[key];
  return Array.isArray(value)
    ? value
        .map((item) => (typeof item === "string" ? item.trim() : ""))
        .filter(Boolean)
    : [];
}

export interface GatewayTransportSettings {
  sseKeepaliveIntervalMs: number;
  upstreamStreamTimeoutMs: number;
  upstreamTotalTimeoutMs: number;
  envKeys: string[];
  requiresRestart: boolean;
}

export interface GatewayUpstreamProxySettings {
  proxyUrl: string;
  envKey: string;
  requiresRestart: boolean;
}

export interface GatewayRouteStrategySettings {
  strategy: string;
  options: string[];
  manualPreferredAccountId: string;
}

export interface GatewayConcurrencyRecommendation {
  cpuCores: number;
  memoryMib: number;
  usageRefreshWorkers: number;
  httpWorkerFactor: number;
  httpWorkerMin: number;
  httpStreamWorkerFactor: number;
  httpStreamWorkerMin: number;
  accountMaxInflight: number;
  queueWaitTimeoutMs: number;
}

export interface ServiceListenConfig {
  mode: string;
  options: string[];
  requiresRestart: boolean;
}

const DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION: GatewayConcurrencyRecommendation = {
  cpuCores: 1,
  memoryMib: 1,
  usageRefreshWorkers: 2,
  httpWorkerFactor: 2,
  httpWorkerMin: 4,
  httpStreamWorkerFactor: 1,
  httpStreamWorkerMin: 1,
  accountMaxInflight: 1,
  queueWaitTimeoutMs: 100,
};

const DEFAULT_SERVICE_LISTEN_OPTIONS = ["loopback", "all_interfaces"];

export function readGatewayTransportSettings(
  payload: unknown
): GatewayTransportSettings {
  return {
    sseKeepaliveIntervalMs: readNumberField(payload, "sseKeepaliveIntervalMs", 15_000),
    upstreamStreamTimeoutMs: readNumberField(
      payload,
      "upstreamStreamTimeoutMs",
      300_000
    ),
    upstreamTotalTimeoutMs: readNumberField(payload, "upstreamTotalTimeoutMs", 0),
    envKeys:
      readStringArrayField(payload, "envKeys").length > 0
        ? readStringArrayField(payload, "envKeys")
        : [
            "CODEXMANAGER_SSE_KEEPALIVE_INTERVAL_MS",
            "CODEXMANAGER_UPSTREAM_STREAM_TIMEOUT_MS",
            "CODEXMANAGER_UPSTREAM_TOTAL_TIMEOUT_MS",
          ],
    requiresRestart: readBooleanField(payload, "requiresRestart", false),
  };
}

export function readGatewayUpstreamProxySettings(
  payload: unknown
): GatewayUpstreamProxySettings {
  return {
    proxyUrl: readStringField(payload, "proxyUrl"),
    envKey: readStringField(
      payload,
      "envKey",
      "CODEXMANAGER_UPSTREAM_PROXY_URL"
    ),
    requiresRestart: readBooleanField(payload, "requiresRestart", false),
  };
}

export function readGatewayRouteStrategySettings(
  payload: unknown
): GatewayRouteStrategySettings {
  return {
    strategy: readStringField(payload, "strategy", "ordered"),
    options:
      readStringArrayField(payload, "options").length > 0
        ? readStringArrayField(payload, "options")
        : ["ordered", "balanced"],
    manualPreferredAccountId: readStringField(payload, "manualPreferredAccountId"),
  };
}

export function readGatewayManualAccountId(payload: unknown): string {
  return readStringField(payload, "accountId");
}

export function readGatewayConcurrencyRecommendation(
  payload: unknown
): GatewayConcurrencyRecommendation {
  return {
    cpuCores: readNumberField(
      payload,
      "cpuCores",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.cpuCores
    ),
    memoryMib: readNumberField(
      payload,
      "memoryMib",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.memoryMib
    ),
    usageRefreshWorkers: readNumberField(
      payload,
      "usageRefreshWorkers",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.usageRefreshWorkers
    ),
    httpWorkerFactor: readNumberField(
      payload,
      "httpWorkerFactor",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.httpWorkerFactor
    ),
    httpWorkerMin: readNumberField(
      payload,
      "httpWorkerMin",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.httpWorkerMin
    ),
    httpStreamWorkerFactor: readNumberField(
      payload,
      "httpStreamWorkerFactor",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.httpStreamWorkerFactor
    ),
    httpStreamWorkerMin: readNumberField(
      payload,
      "httpStreamWorkerMin",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.httpStreamWorkerMin
    ),
    accountMaxInflight: readNumberField(
      payload,
      "accountMaxInflight",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.accountMaxInflight
    ),
    queueWaitTimeoutMs: readNumberField(
      payload,
      "queueWaitTimeoutMs",
      DEFAULT_GATEWAY_CONCURRENCY_RECOMMENDATION.queueWaitTimeoutMs
    ),
  };
}

export function readServiceListenConfig(payload: unknown): ServiceListenConfig {
  return {
    mode: readStringField(payload, "mode", "loopback"),
    options:
      readStringArrayField(payload, "options").length > 0
        ? readStringArrayField(payload, "options")
        : DEFAULT_SERVICE_LISTEN_OPTIONS,
    requiresRestart: readBooleanField(payload, "requiresRestart", true),
  };
}
