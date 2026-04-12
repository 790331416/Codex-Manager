import { ManagedModelInfo, ModelInfo, ModelReasoningLevel, ModelTruncationPolicy } from "@/types";

const KNOWN_MODEL_FIELD_KEYS = new Set([
  "slug",
  "displayName",
  "display_name",
  "description",
  "defaultReasoningLevel",
  "default_reasoning_level",
  "supportedReasoningLevels",
  "supported_reasoning_levels",
  "shellType",
  "shell_type",
  "visibility",
  "supportedInApi",
  "supported_in_api",
  "priority",
  "additionalSpeedTiers",
  "additional_speed_tiers",
  "availabilityNux",
  "availability_nux",
  "upgrade",
  "baseInstructions",
  "base_instructions",
  "modelMessages",
  "model_messages",
  "supportsReasoningSummaries",
  "supports_reasoning_summaries",
  "defaultReasoningSummary",
  "default_reasoning_summary",
  "supportVerbosity",
  "support_verbosity",
  "defaultVerbosity",
  "default_verbosity",
  "applyPatchToolType",
  "apply_patch_tool_type",
  "webSearchToolType",
  "web_search_tool_type",
  "truncationPolicy",
  "truncation_policy",
  "supportsParallelToolCalls",
  "supports_parallel_tool_calls",
  "supportsImageDetailOriginal",
  "supports_image_detail_original",
  "contextWindow",
  "context_window",
  "autoCompactTokenLimit",
  "auto_compact_token_limit",
  "effectiveContextWindowPercent",
  "effective_context_window_percent",
  "experimentalSupportedTools",
  "experimental_supported_tools",
  "inputModalities",
  "input_modalities",
  "minimalClientVersion",
  "minimal_client_version",
  "supportsSearchTool",
  "supports_search_tool",
  "availableInPlans",
  "available_in_plans",
  "sourceKind",
  "source_kind",
  "userEdited",
  "user_edited",
  "sortIndex",
  "sort_index",
  "updatedAt",
  "updated_at",
]);

function normalizeNullableString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function serializeReasoningLevels(
  levels: ModelReasoningLevel[]
): Array<Record<string, unknown>> {
  return levels.map((level) => {
    const source = level as Record<string, unknown>;
    const extra = Object.fromEntries(
      Object.entries(source).filter(([key]) => key !== "effort" && key !== "description")
    );
    return {
      ...extra,
      effort: String(level.effort || "").trim(),
      description: typeof level.description === "string" ? level.description : "",
    };
  });
}

function serializeTruncationPolicy(
  policy: ModelTruncationPolicy | null
): Record<string, unknown> | null {
  if (!policy) return null;
  const source = policy as Record<string, unknown>;
  const extra = Object.fromEntries(
    Object.entries(source).filter(([key]) => key !== "mode" && key !== "limit")
  );
  return {
    ...extra,
    mode: String(policy.mode || "").trim(),
    limit: Number.isFinite(policy.limit) ? policy.limit : 0,
  };
}

export function extractManagedModelExtraFields(
  model: Partial<ManagedModelInfo> | Partial<ModelInfo> | null | undefined
): Record<string, unknown> {
  if (!model || typeof model !== "object") {
    return {};
  }
  return Object.fromEntries(
    Object.entries(model).filter(([key]) => !KNOWN_MODEL_FIELD_KEYS.has(key))
  );
}

export function serializeManagedModelForRpc(
  model: ManagedModelInfo | ModelInfo
): Record<string, unknown> {
  const extra = extractManagedModelExtraFields(model);
  const slug = String(model.slug || "").trim();
  const displayName = String(model.displayName || "").trim() || slug;

  return {
    ...extra,
    slug,
    display_name: displayName,
    description: normalizeNullableString(model.description),
    default_reasoning_level: normalizeNullableString(model.defaultReasoningLevel),
    supported_reasoning_levels: serializeReasoningLevels(model.supportedReasoningLevels),
    shell_type: normalizeNullableString(model.shellType),
    visibility: normalizeNullableString(model.visibility),
    supported_in_api: Boolean(model.supportedInApi),
    priority: Number.isFinite(model.priority) ? model.priority : 0,
    additional_speed_tiers: model.additionalSpeedTiers,
    availability_nux: model.availabilityNux,
    upgrade: model.upgrade,
    base_instructions: normalizeNullableString(model.baseInstructions),
    model_messages: model.modelMessages,
    supports_reasoning_summaries: model.supportsReasoningSummaries,
    default_reasoning_summary: normalizeNullableString(model.defaultReasoningSummary),
    support_verbosity: model.supportVerbosity,
    default_verbosity: model.defaultVerbosity,
    apply_patch_tool_type: normalizeNullableString(model.applyPatchToolType),
    web_search_tool_type: normalizeNullableString(model.webSearchToolType),
    truncation_policy: serializeTruncationPolicy(model.truncationPolicy),
    supports_parallel_tool_calls: model.supportsParallelToolCalls,
    supports_image_detail_original: model.supportsImageDetailOriginal,
    context_window: model.contextWindow,
    auto_compact_token_limit: model.autoCompactTokenLimit,
    effective_context_window_percent: model.effectiveContextWindowPercent,
    experimental_supported_tools: model.experimentalSupportedTools,
    input_modalities: model.inputModalities,
    minimal_client_version: model.minimalClientVersion,
    supports_search_tool: model.supportsSearchTool,
    available_in_plans: model.availableInPlans,
  };
}

function omitNullishEntries(source: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(source).filter(([, value]) => value !== null && value !== undefined)
  );
}

function parseCodexCliVersion(userAgent: string): string {
  const match = String(userAgent || "").match(/codex_cli_rs\/([^\s]+)/);
  return match?.[1]?.trim() || "";
}

function serializeModelForCodexCache(
  model: ManagedModelInfo | ModelInfo
): Record<string, unknown> {
  const serialized = omitNullishEntries(serializeManagedModelForRpc(model));

  return {
    ...serialized,
    display_name:
      typeof serialized.display_name === "string" && serialized.display_name.trim()
        ? serialized.display_name
        : model.slug,
    supported_reasoning_levels: Array.isArray(serialized.supported_reasoning_levels)
      ? serialized.supported_reasoning_levels
      : [],
    shell_type:
      typeof serialized.shell_type === "string" && serialized.shell_type.trim()
        ? serialized.shell_type
        : "shell_command",
    visibility:
      typeof serialized.visibility === "string" && serialized.visibility.trim()
        ? serialized.visibility
        : "list",
    supported_in_api:
      typeof serialized.supported_in_api === "boolean" ? serialized.supported_in_api : true,
    priority:
      typeof serialized.priority === "number" && Number.isFinite(serialized.priority)
        ? serialized.priority
        : 0,
    additional_speed_tiers: Array.isArray(serialized.additional_speed_tiers)
      ? serialized.additional_speed_tiers
      : [],
    base_instructions:
      typeof serialized.base_instructions === "string" ? serialized.base_instructions : "",
    supports_reasoning_summaries:
      typeof serialized.supports_reasoning_summaries === "boolean"
        ? serialized.supports_reasoning_summaries
        : false,
    default_reasoning_summary:
      typeof serialized.default_reasoning_summary === "string" &&
      serialized.default_reasoning_summary.trim()
        ? serialized.default_reasoning_summary
        : "auto",
    support_verbosity:
      typeof serialized.support_verbosity === "boolean" ? serialized.support_verbosity : false,
    web_search_tool_type:
      typeof serialized.web_search_tool_type === "string" &&
      serialized.web_search_tool_type.trim()
        ? serialized.web_search_tool_type
        : "text",
    truncation_policy:
      serialized.truncation_policy && typeof serialized.truncation_policy === "object"
        ? serialized.truncation_policy
        : { mode: "tokens", limit: 10000 },
    supports_parallel_tool_calls:
      typeof serialized.supports_parallel_tool_calls === "boolean"
        ? serialized.supports_parallel_tool_calls
        : false,
    effective_context_window_percent:
      typeof serialized.effective_context_window_percent === "number" &&
      Number.isFinite(serialized.effective_context_window_percent)
        ? serialized.effective_context_window_percent
        : 95,
    experimental_supported_tools: Array.isArray(serialized.experimental_supported_tools)
      ? serialized.experimental_supported_tools
      : [],
    input_modalities:
      Array.isArray(serialized.input_modalities) && serialized.input_modalities.length > 0
        ? serialized.input_modalities
        : ["text", "image"],
    supports_search_tool:
      typeof serialized.supports_search_tool === "boolean"
        ? serialized.supports_search_tool
        : false,
  };
}

function readModelSortIndex(model: ManagedModelInfo | ModelInfo): number {
  if ("sortIndex" in model && typeof model.sortIndex === "number") {
    return model.sortIndex;
  }
  return Number.MAX_SAFE_INTEGER;
}

export function serializeManagedModelCatalogForCodexCache(
  models: Array<ManagedModelInfo | ModelInfo>
): Array<Record<string, unknown>> {
  return [...models]
    .sort((left, right) => {
      if (left.priority !== right.priority) {
        return left.priority - right.priority;
      }

      const sortIndexDelta = readModelSortIndex(left) - readModelSortIndex(right);
      if (sortIndexDelta !== 0) {
        return sortIndexDelta;
      }

      return left.slug.localeCompare(right.slug);
    })
    .map((model) => serializeModelForCodexCache(model));
}

export function buildCodexModelsCachePayload(
  models: Array<ManagedModelInfo | ModelInfo>,
  userAgent: string,
  options?: {
    etag?: string | null;
    fetchedAt?: string;
  }
): Record<string, unknown> {
  const clientVersion = parseCodexCliVersion(userAgent);
  if (!clientVersion) {
    throw new Error("无法从 userAgent 解析 Codex CLI 版本");
  }

  return {
    fetched_at: options?.fetchedAt || new Date().toISOString(),
    etag: options?.etag ?? null,
    client_version: clientVersion,
    models: serializeManagedModelCatalogForCodexCache(models),
  };
}
