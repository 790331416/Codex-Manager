use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    Integer(i64),
}

impl fmt::Display for RequestId {
    /// 函数 `fmt`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - f: 参数 f
    ///
    /// # 返回
    /// 返回函数执行结果
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(value) => f.write_str(value),
            Self::Integer(value) => write!(f, "{value}"),
        }
    }
}

impl From<i64> for RequestId {
    /// 函数 `from`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - value: 参数 value
    ///
    /// # 返回
    /// 返回函数执行结果
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<i32> for RequestId {
    /// 函数 `from`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - value: 参数 value
    ///
    /// # 返回
    /// 返回函数执行结果
    fn from(value: i32) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<u64> for RequestId {
    /// 函数 `from`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - value: 参数 value
    ///
    /// # 返回
    /// 返回函数执行结果
    fn from(value: u64) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<u32> for RequestId {
    /// 函数 `from`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - value: 参数 value
    ///
    /// # 返回
    /// 返回函数执行结果
    fn from(value: u32) -> Self {
        Self::Integer(value as i64)
    }
}

impl From<usize> for RequestId {
    /// 函数 `from`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - value: 参数 value
    ///
    /// # 返回
    /// 返回函数执行结果
    fn from(value: usize) -> Self {
        Self::Integer(value as i64)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcMessage {
    Request(JsonRpcRequest),
    Notification(JsonRpcNotification),
    Response(JsonRpcResponse),
    Error(JsonRpcError),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub id: RequestId,
    pub method: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub method: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub id: RequestId,
    pub result: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub error: JsonRpcErrorObject,
    pub id: RequestId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcErrorObject {
    pub code: i64,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub version: String,
    pub user_agent: String,
    pub codex_home: String,
    pub platform_family: String,
    pub platform_os: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountSummary {
    pub id: String,
    pub label: String,
    pub group_name: Option<String>,
    pub preferred: bool,
    pub sort: i64,
    pub status: String,
    pub status_reason: Option<String>,
    pub plan_type: Option<String>,
    pub plan_type_raw: Option<String>,
    pub has_subscription: Option<bool>,
    pub subscription_plan: Option<String>,
    pub subscription_expires_at: Option<i64>,
    pub subscription_renews_at: Option<i64>,
    pub note: Option<String>,
    pub tags: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AccountListParams {
    pub page: i64,
    pub page_size: i64,
    pub query: Option<String>,
    pub filter: Option<String>,
    pub group_filter: Option<String>,
}

impl Default for AccountListParams {
    /// 函数 `default`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// 无
    ///
    /// # 返回
    /// 返回函数执行结果
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 5,
            query: None,
            filter: None,
            group_filter: None,
        }
    }
}

impl AccountListParams {
    /// 函数 `normalized`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn normalized(self) -> Self {
        // 中文注释：分页参数小于 1 时回退到默认值，避免出现负偏移或零页大小。
        Self {
            page: if self.page < 1 { 1 } else { self.page },
            page_size: if self.page_size < 1 {
                5
            } else {
                self.page_size
            },
            query: self.query,
            filter: self.filter,
            group_filter: self.group_filter,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountListResult {
    pub items: Vec<AccountSummary>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceAuthInfo {
    pub user_code_url: String,
    pub token_url: String,
    pub verification_url: String,
    pub redirect_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LoginStartResult {
    #[serde(rename = "apiKey", rename_all = "camelCase")]
    ApiKey {},
    #[serde(rename = "chatgpt", rename_all = "camelCase")]
    Chatgpt { login_id: String, auth_url: String },
    #[serde(rename = "chatgptDeviceCode", rename_all = "camelCase")]
    ChatgptDeviceCode {
        login_id: String,
        verification_url: String,
        user_code: String,
    },
    #[serde(rename = "chatgptAuthTokens", rename_all = "camelCase")]
    ChatgptAuthTokens {},
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSnapshotResult {
    pub account_id: Option<String>,
    pub availability_status: Option<String>,
    pub used_percent: Option<f64>,
    pub window_minutes: Option<i64>,
    pub resets_at: Option<i64>,
    pub secondary_used_percent: Option<f64>,
    pub secondary_window_minutes: Option<i64>,
    pub secondary_resets_at: Option<i64>,
    pub credits_json: Option<String>,
    pub captured_at: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UsageReadResult {
    pub snapshot: Option<UsageSnapshotResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitWindowResult {
    pub used_percent: i64,
    pub window_duration_mins: Option<i64>,
    pub resets_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitSnapshotResult {
    pub limit_id: Option<String>,
    pub limit_name: Option<String>,
    pub primary: Option<RateLimitWindowResult>,
    pub secondary: Option<RateLimitWindowResult>,
    pub credits: Option<serde_json::Value>,
    pub plan_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountRateLimitsReadResult {
    pub rate_limits: RateLimitSnapshotResult,
    pub rate_limits_by_limit_id:
        Option<std::collections::BTreeMap<String, RateLimitSnapshotResult>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UsageListResult {
    pub items: Vec<UsageSnapshotResult>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageAggregateSummaryResult {
    pub primary_bucket_count: i64,
    pub primary_known_count: i64,
    pub primary_unknown_count: i64,
    pub primary_remain_percent: Option<i64>,
    pub secondary_bucket_count: i64,
    pub secondary_known_count: i64,
    pub secondary_unknown_count: i64,
    pub secondary_remain_percent: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeySummary {
    pub id: String,
    pub name: Option<String>,
    pub model_slug: Option<String>,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub rotation_strategy: String,
    pub aggregate_api_id: Option<String>,
    pub account_plan_filter: Option<String>,
    pub aggregate_api_url: Option<String>,
    pub client_type: String,
    pub protocol_type: String,
    pub auth_scheme: String,
    pub upstream_base_url: Option<String>,
    pub static_headers_json: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiKeyListResult {
    pub items: Vec<ApiKeySummary>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyUsageStatSummary {
    pub key_id: String,
    pub total_tokens: i64,
    pub estimated_cost_usd: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiKeyUsageStatListResult {
    pub items: Vec<ApiKeyUsageStatSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyCreateResult {
    pub id: String,
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeySecretResult {
    pub id: String,
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateApiSummary {
    pub id: String,
    pub provider_type: String,
    pub supplier_name: Option<String>,
    pub sort: i64,
    pub url: String,
    pub auth_type: String,
    pub auth_params: Option<serde_json::Value>,
    pub action: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_test_at: Option<i64>,
    pub last_test_status: Option<String>,
    pub last_test_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginCatalogEntry {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub author: Option<String>,
    pub homepage_url: Option<String>,
    pub script_url: Option<String>,
    pub script_body: Option<String>,
    pub permissions: Vec<String>,
    pub tasks: Vec<PluginCatalogTask>,
    pub manifest_version: String,
    pub category: Option<String>,
    pub runtime_kind: String,
    pub tags: Vec<String>,
    pub source_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginCatalogTask {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub entrypoint: String,
    pub schedule_kind: String,
    pub interval_seconds: Option<i64>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstalledPluginSummary {
    pub plugin_id: String,
    pub source_url: Option<String>,
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub author: Option<String>,
    pub homepage_url: Option<String>,
    pub script_url: Option<String>,
    pub permissions: Vec<String>,
    pub status: String,
    pub installed_at: i64,
    pub updated_at: i64,
    pub last_run_at: Option<i64>,
    pub last_error: Option<String>,
    pub task_count: i64,
    pub enabled_task_count: i64,
    pub manifest_version: String,
    pub category: Option<String>,
    pub runtime_kind: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginTaskSummary {
    pub id: String,
    pub plugin_id: String,
    pub plugin_name: String,
    pub name: String,
    pub description: Option<String>,
    pub entrypoint: String,
    pub schedule_kind: String,
    pub interval_seconds: Option<i64>,
    pub enabled: bool,
    pub next_run_at: Option<i64>,
    pub last_run_at: Option<i64>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRunLogSummary {
    pub id: i64,
    pub plugin_id: String,
    pub plugin_name: Option<String>,
    pub task_id: Option<String>,
    pub task_name: Option<String>,
    pub run_type: String,
    pub status: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub duration_ms: Option<i64>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregateApiListResult {
    pub items: Vec<AggregateApiSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateApiCreateResult {
    pub id: String,
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateApiSecretResult {
    pub id: String,
    pub key: String,
    pub auth_type: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateApiTestResult {
    pub id: String,
    pub ok: bool,
    pub status_code: Option<i64>,
    pub message: Option<String>,
    pub tested_at: i64,
    pub latency_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelsResponse {
    #[serde(default)]
    pub models: Vec<ModelInfo>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl ModelsResponse {
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedModelCatalogEntry {
    #[serde(flatten)]
    pub model: ModelInfo,
    #[serde(default = "default_model_source_kind")]
    pub source_kind: String,
    #[serde(default)]
    pub user_edited: bool,
    #[serde(default)]
    pub sort_index: i64,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedModelCatalogResult {
    #[serde(default)]
    pub items: Vec<ManagedModelCatalogEntry>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedModelCatalogUpsertParams {
    #[serde(default)]
    pub previous_slug: Option<String>,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub user_edited: Option<bool>,
    #[serde(default)]
    pub sort_index: Option<i64>,
    #[serde(flatten)]
    pub model: ModelInfo,
}

fn default_model_source_kind() -> String {
    "remote".to_string()
}

fn default_supported_in_api() -> bool {
    true
}

fn default_input_modalities() -> Vec<String> {
    vec!["text".to_string(), "image".to_string()]
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelInfo {
    pub slug: String,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_reasoning_level: Option<String>,
    #[serde(default)]
    pub supported_reasoning_levels: Vec<ModelReasoningLevel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    #[serde(default = "default_supported_in_api")]
    pub supported_in_api: bool,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub additional_speed_tiers: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub availability_nux: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upgrade: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_messages: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_reasoning_summaries: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_reasoning_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub support_verbosity: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_verbosity: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply_patch_tool_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_search_tool_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncation_policy: Option<ModelTruncationPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_parallel_tool_calls: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_image_detail_original: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_compact_token_limit: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_context_window_percent: Option<i64>,
    #[serde(default)]
    pub experimental_supported_tools: Vec<String>,
    #[serde(default = "default_input_modalities")]
    pub input_modalities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimal_client_version: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_search_tool: Option<bool>,
    #[serde(default)]
    pub available_in_plans: Vec<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelReasoningLevel {
    pub effort: String,
    pub description: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelTruncationPolicy {
    pub mode: String,
    pub limit: i64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestLogSummary {
    pub trace_id: Option<String>,
    pub key_id: Option<String>,
    pub account_id: Option<String>,
    pub initial_account_id: Option<String>,
    #[serde(default)]
    pub attempted_account_ids: Vec<String>,
    pub initial_aggregate_api_id: Option<String>,
    #[serde(default)]
    pub attempted_aggregate_api_ids: Vec<String>,
    pub request_path: String,
    pub original_path: Option<String>,
    pub adapted_path: Option<String>,
    pub method: String,
    pub request_type: Option<String>,
    pub gateway_mode: Option<String>,
    pub transparent_mode: Option<bool>,
    pub enhanced_mode: Option<bool>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub effective_service_tier: Option<String>,
    pub response_adapter: Option<String>,
    pub canonical_source: Option<String>,
    pub size_reject_stage: Option<String>,
    pub upstream_url: Option<String>,
    pub aggregate_api_supplier_name: Option<String>,
    pub aggregate_api_url: Option<String>,
    pub status_code: Option<i64>,
    pub duration_ms: Option<i64>,
    pub first_response_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub reasoning_output_tokens: Option<i64>,
    pub estimated_cost_usd: Option<f64>,
    pub error: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RequestLogListParams {
    pub page: i64,
    pub page_size: i64,
    pub query: Option<String>,
    pub status_filter: Option<String>,
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
}

impl Default for RequestLogListParams {
    /// 函数 `default`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// 无
    ///
    /// # 返回
    /// 返回函数执行结果
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 20,
            query: None,
            status_filter: None,
            start_ts: None,
            end_ts: None,
        }
    }
}

impl RequestLogListParams {
    /// 函数 `normalized`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn normalized(self) -> Self {
        Self {
            page: if self.page < 1 { 1 } else { self.page },
            page_size: if self.page_size < 1 {
                20
            } else {
                self.page_size
            },
            query: self.query,
            status_filter: self.status_filter,
            start_ts: self.start_ts.filter(|value| *value > 0),
            end_ts: self.end_ts.filter(|value| *value > 0),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestLogListResult {
    pub items: Vec<RequestLogSummary>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayErrorLogSummary {
    pub trace_id: Option<String>,
    pub key_id: Option<String>,
    pub account_id: Option<String>,
    pub request_path: String,
    pub method: String,
    pub stage: String,
    pub error_kind: Option<String>,
    pub upstream_url: Option<String>,
    pub cf_ray: Option<String>,
    pub status_code: Option<i64>,
    pub compression_enabled: bool,
    pub compression_retry_attempted: bool,
    pub message: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GatewayErrorLogListParams {
    pub page: i64,
    pub page_size: i64,
    pub stage_filter: Option<String>,
}

impl Default for GatewayErrorLogListParams {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 10,
            stage_filter: None,
        }
    }
}

impl GatewayErrorLogListParams {
    pub fn normalized(self) -> Self {
        Self {
            page: if self.page < 1 { 1 } else { self.page },
            page_size: if self.page_size < 1 {
                10
            } else {
                self.page_size
            },
            stage_filter: self.stage_filter,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayErrorLogListResult {
    pub items: Vec<GatewayErrorLogSummary>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
    #[serde(default)]
    pub stages: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestLogFilterSummaryResult {
    pub total_count: i64,
    pub filtered_count: i64,
    pub success_count: i64,
    pub error_count: i64,
    pub total_tokens: i64,
    pub total_cost_usd: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestLogTodaySummaryResult {
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_output_tokens: i64,
    pub today_tokens: i64,
    pub estimated_cost: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartupSnapshotResult {
    pub accounts: Vec<AccountSummary>,
    pub usage_snapshots: Vec<UsageSnapshotResult>,
    #[serde(default)]
    pub usage_aggregate_summary: UsageAggregateSummaryResult,
    pub api_keys: Vec<ApiKeySummary>,
    pub api_models: ModelsResponse,
    pub manual_preferred_account_id: Option<String>,
    pub request_log_today_summary: RequestLogTodaySummaryResult,
    pub request_logs: Vec<RequestLogSummary>,
}

#[cfg(test)]
#[path = "tests/types_tests.rs"]
mod tests;
