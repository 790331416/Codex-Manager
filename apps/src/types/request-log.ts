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
  upstreamModel: string;
  actualSourceKind: string;
  actualSourceId: string;
  reasoningEffort: string;
  serviceTier: string;
  effectiveServiceTier: string;
  responseAdapter: string;
  canonicalSource: string;
  sizeRejectStage: string;
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
  firstResponseMs: number | null;
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

export interface RequestLogTodaySummary {
  inputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  reasoningOutputTokens: number;
  todayTokens: number;
  estimatedCost: number;
}
