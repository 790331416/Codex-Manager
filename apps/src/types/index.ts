export type AvailabilityLevel = "ok" | "warn" | "bad" | "unknown";

export type RuntimeMode = "desktop-tauri" | "web-gateway" | "unsupported-web";

export interface RuntimeCapabilities {
  mode: RuntimeMode;
  rpcBaseUrl: string;
  canManageService: boolean;
  canSelfUpdate: boolean;
  canCloseToTray: boolean;
  canOpenLocalDir: boolean;
  canUseBrowserFileImport: boolean;
  canUseBrowserDownloadExport: boolean;
  unsupportedReason?: string | null;
}

export interface ServiceStatus {
  connected: boolean;
  version: string;
  uptime: number;
  addr: string;
}

export interface AccountUsage {
  accountId: string;
  availabilityStatus: string;
  usedPercent: number | null;
  windowMinutes: number | null;
  resetsAt: number | null;
  secondaryUsedPercent: number | null;
  secondaryWindowMinutes: number | null;
  secondaryResetsAt: number | null;
  creditsJson: string | null;
  capturedAt: number | null;
}

export interface Account {
  id: string;
  name: string;
  group: string;
  priority: number;
  preferred: boolean;
  label: string;
  groupName: string;
  sort: number;
  status: string;
  statusReason: string;
  planType: string | null;
  planTypeRaw: string | null;
  note: string | null;
  tags: string[];
  isAvailable: boolean;
  isLowQuota: boolean;
  lastRefreshAt: number | null;
  availabilityText: string;
  availabilityLevel: AvailabilityLevel;
  primaryRemainPercent: number | null;
  secondaryRemainPercent: number | null;
  usage: AccountUsage | null;
}

export interface AccountListResult {
  items: Account[];
  total: number;
  page: number;
  pageSize: number;
}

export interface UsageAggregateSummary {
  primaryBucketCount: number;
  primaryKnownCount: number;
  primaryUnknownCount: number;
  primaryRemainPercent: number | null;
  secondaryBucketCount: number;
  secondaryKnownCount: number;
  secondaryUnknownCount: number;
  secondaryRemainPercent: number | null;
}

export interface ApiKey {
  id: string;
  name: string;
  model: string;
  modelSlug: string;
  reasoningEffort: string;
  serviceTier: string;
  rotationStrategy: string;
  aggregateApiId: string | null;
  accountPlanFilter: string | null;
  aggregateApiUrl: string | null;
  protocol: string;
  clientType: string;
  authScheme: string;
  upstreamBaseUrl: string;
  staticHeadersJson: string;
  status: string;
  createdAt: number | null;
  lastUsedAt: number | null;
}

export interface ApiKeyCreateResult {
  id: string;
  key: string;
}

export interface AggregateApi {
  id: string;
  providerType: string;
  supplierName: string | null;
  sort: number;
  url: string;
  authType: string;
  authParams: Record<string, unknown> | null;
  action: string | null;
  status: string;
  createdAt: number | null;
  updatedAt: number | null;
  lastTestAt: number | null;
  lastTestStatus: string | null;
  lastTestError: string | null;
}

export interface AggregateApiCreateResult {
  id: string;
  key: string;
}

export interface AggregateApiSecretResult {
  id: string;
  key: string;
  authType: string;
  username: string | null;
  password: string | null;
}

export interface AggregateApiTestResult {
  id: string;
  ok: boolean;
  statusCode: number | null;
  message: string | null;
  testedAt: number;
  latencyMs: number;
}

export interface ApiKeyUsageStat {
  keyId: string;
  totalTokens: number;
  estimatedCostUsd: number;
}

export interface PluginCatalogTask {
  id: string;
  name: string;
  description: string | null;
  entrypoint: string;
  scheduleKind: string;
  intervalSeconds: number | null;
  enabled: boolean;
}

export interface PluginCatalogEntry {
  id: string;
  name: string;
  version: string;
  description: string | null;
  author: string | null;
  homepageUrl: string | null;
  scriptUrl: string | null;
  scriptBody: string | null;
  permissions: string[];
  tasks: PluginCatalogTask[];
  manifestVersion: string;
  category: string | null;
  runtimeKind: string;
  tags: string[];
  sourceUrl: string | null;
}

export interface InstalledPluginSummary {
  pluginId: string;
  sourceUrl: string | null;
  name: string;
  version: string;
  description: string | null;
  author: string | null;
  homepageUrl: string | null;
  scriptUrl: string | null;
  permissions: string[];
  status: string;
  installedAt: number;
  updatedAt: number;
  lastRunAt: number | null;
  lastError: string | null;
  taskCount: number;
  enabledTaskCount: number;
  manifestVersion: string;
  category: string | null;
  runtimeKind: string;
  tags: string[];
}

export interface PluginTaskSummary {
  id: string;
  pluginId: string;
  pluginName: string;
  name: string;
  description: string | null;
  entrypoint: string;
  scheduleKind: string;
  intervalSeconds: number | null;
  enabled: boolean;
  nextRunAt: number | null;
  lastRunAt: number | null;
  lastStatus: string | null;
  lastError: string | null;
}

export interface PluginRunLogSummary {
  id: number;
  pluginId: string;
  pluginName: string | null;
  taskId: string | null;
  taskName: string | null;
  runType: string;
  status: string;
  startedAt: number;
  finishedAt: number | null;
  durationMs: number | null;
  output: unknown | null;
  error: string | null;
}

export interface PluginCatalogResult {
  sourceUrl: string;
  items: PluginCatalogEntry[];
}

export interface ModelReasoningLevel {
  effort: string;
  description: string;
  [key: string]: unknown;
}

export interface ModelTruncationPolicy {
  mode: string;
  limit: number;
  [key: string]: unknown;
}

export interface ModelInfo {
  slug: string;
  displayName: string;
  description: string | null;
  defaultReasoningLevel: string | null;
  supportedReasoningLevels: ModelReasoningLevel[];
  shellType: string | null;
  visibility: string | null;
  supportedInApi: boolean;
  priority: number;
  additionalSpeedTiers: string[];
  availabilityNux: Record<string, unknown> | null;
  upgrade: Record<string, unknown> | null;
  baseInstructions: string | null;
  modelMessages: Record<string, unknown> | null;
  supportsReasoningSummaries: boolean | null;
  defaultReasoningSummary: string | null;
  supportVerbosity: boolean | null;
  defaultVerbosity: unknown | null;
  applyPatchToolType: string | null;
  webSearchToolType: string | null;
  truncationPolicy: ModelTruncationPolicy | null;
  supportsParallelToolCalls: boolean | null;
  supportsImageDetailOriginal: boolean | null;
  contextWindow: number | null;
  autoCompactTokenLimit: number | null;
  effectiveContextWindowPercent: number | null;
  experimentalSupportedTools: string[];
  inputModalities: string[];
  minimalClientVersion: unknown | null;
  supportsSearchTool: boolean | null;
  availableInPlans: string[];
  [key: string]: unknown;
}

export interface ModelCatalog {
  models: ModelInfo[];
  [key: string]: unknown;
}

export interface RequestLog {
  id: string;
  traceId: string;
  keyId: string;
  accountId: string;
  initialAccountId: string;
  attemptedAccountIds: string[];
  initialAggregateApiId: string;
  attemptedAggregateApiIds: string[];
  requestPath: string;
  originalPath: string;
  adaptedPath: string;
  method: string;
  requestType: string;
  path: string;
  model: string;
  reasoningEffort: string;
  serviceTier: string;
  effectiveServiceTier: string;
  responseAdapter: string;
  upstreamUrl: string;
  aggregateApiSupplierName: string | null;
  aggregateApiUrl: string | null;
  statusCode: number | null;
  inputTokens: number | null;
  cachedInputTokens: number | null;
  outputTokens: number | null;
  totalTokens: number | null;
  reasoningOutputTokens: number | null;
  estimatedCostUsd: number | null;
  durationMs: number | null;
  error: string;
  createdAt: number | null;
}

export interface RequestLogListResult {
  items: RequestLog[];
  total: number;
  page: number;
  pageSize: number;
}

export interface GatewayErrorLog {
  traceId: string;
  keyId: string;
  accountId: string;
  requestPath: string;
  method: string;
  stage: string;
  errorKind: string;
  upstreamUrl: string;
  cfRay: string;
  statusCode: number | null;
  compressionEnabled: boolean;
  compressionRetryAttempted: boolean;
  message: string;
  createdAt: number | null;
}

export interface GatewayErrorLogListResult {
  items: GatewayErrorLog[];
  total: number;
  page: number;
  pageSize: number;
  stages: string[];
}

export interface RequestLogFilterSummary {
  totalCount: number;
  filteredCount: number;
  successCount: number;
  errorCount: number;
  totalTokens: number;
  totalCostUsd: number;
}

export interface LoginStatusResult {
  status: string;
  error: string;
}

export interface RequestLogTodaySummary {
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  reasoningOutputTokens: number;
  todayTokens: number;
  estimatedCost: number;
}

export interface DeviceAuthInfo {
  userCodeUrl: string;
  tokenUrl: string;
  verificationUrl: string;
  redirectUri: string;
}

export interface LoginStartResult {
  type: string;
  authUrl?: string | null;
  loginId: string;
  verificationUrl?: string | null;
  userCode?: string | null;
}

export interface CurrentAccessTokenAccount {
  type: string;
  accountId: string;
  email: string;
  planType: string;
  planTypeRaw?: string | null;
  chatgptAccountId: string | null;
  workspaceId: string | null;
  status: string;
}

export interface CurrentAccessTokenAccountReadResult {
  account: CurrentAccessTokenAccount | null;
  requiresOpenaiAuth: boolean;
}

export interface ChatgptAuthTokensRefreshResult {
  accessToken: string;
  chatgptAccountId: string;
  chatgptPlanType: string | null;
}

export interface EnvOverrideCatalogItem {
  key: string;
  label: string;
  defaultValue: string;
  scope: string;
  applyMode: string;
}

export interface BackgroundTaskSettings {
  usagePollingEnabled: boolean;
  usagePollIntervalSecs: number;
  gatewayKeepaliveEnabled: boolean;
  gatewayKeepaliveIntervalSecs: number;
  tokenRefreshPollingEnabled: boolean;
  tokenRefreshPollIntervalSecs: number;
  usageRefreshWorkers: number;
  httpWorkerFactor: number;
  httpWorkerMin: number;
  httpStreamWorkerFactor: number;
  httpStreamWorkerMin: number;
}

export interface AppSettings {
  updateAutoCheck: boolean;
  closeToTrayOnClose: boolean;
  closeToTraySupported: boolean;
  lowTransparency: boolean;
  lightweightModeOnCloseToTray: boolean;
  codexCliGuideDismissed: boolean;
  webAccessPasswordConfigured: boolean;
  locale: string;
  localeOptions: string[];
  serviceAddr: string;
  serviceListenMode: string;
  serviceListenModeOptions: string[];
  routeStrategy: string;
  routeStrategyOptions: string[];
  freeAccountMaxModel: string;
  freeAccountMaxModelOptions: string[];
  modelForwardRules: string;
  accountMaxInflight: number;
  gatewayOriginator: string;
  gatewayOriginatorDefault: string;
  gatewayUserAgentVersion: string;
  gatewayUserAgentVersionDefault: string;
  gatewayResidencyRequirement: string;
  gatewayResidencyRequirementOptions: string[];
  pluginMarketMode: string;
  pluginMarketSourceUrl: string;
  upstreamProxyUrl: string;
  upstreamStreamTimeoutMs: number;
  sseKeepaliveIntervalMs: number;
  backgroundTasks: BackgroundTaskSettings;
  envOverrides: Record<string, string>;
  envOverrideCatalog: EnvOverrideCatalogItem[];
  envOverrideReservedKeys: string[];
  envOverrideUnsupportedKeys: string[];
  theme: string;
  appearancePreset: string;
  [key: string]: unknown;
}

export interface CodexLatestVersionInfo {
  packageName: string;
  version: string;
  distTag: string;
  registryUrl: string;
}

export interface ServiceInitializationResult {
  userAgent: string;
  codexHome: string;
  platformFamily: string;
  platformOs: string;
}

export interface StartupSnapshot {
  accounts: Account[];
  usageSnapshots: AccountUsage[];
  usageAggregateSummary: UsageAggregateSummary;
  apiKeys: ApiKey[];
  apiModels: ModelCatalog;
  manualPreferredAccountId: string;
  requestLogTodaySummary: RequestLogTodaySummary;
  requestLogs: RequestLog[];
}
