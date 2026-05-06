export interface EnvOverrideCatalogItem {
  key: string;
  label: string;
  defaultValue: string;
  scope: string;
  applyMode: string;
  riskLevel: string;
  effectScope: string;
  safetyNote: string;
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
  upstreamTotalTimeoutMs: number;
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
