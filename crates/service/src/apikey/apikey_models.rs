use std::collections::{BTreeMap, HashSet};

use codexmanager_core::rpc::types::{
    ModelInfo, ModelReasoningLevel, ModelTruncationPolicy, ModelsResponse,
};
use codexmanager_core::storage::{
    now_ts, ModelCatalogModelRecord, ModelCatalogReasoningLevelRecord, ModelCatalogScopeRecord,
    ModelCatalogStringItemRecord, Storage,
};
use serde_json::Value;

use crate::gateway;
use crate::storage_helpers;

const MODEL_CACHE_SCOPE_DEFAULT: &str = "default";

/// 函数 `read_model_options`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - refresh_remote: 参数 refresh_remote
///
/// # 返回
/// 返回函数执行结果
pub(crate) fn read_model_options(refresh_remote: bool) -> Result<ModelsResponse, String> {
    let cached = read_cached_model_options()?;
    if !refresh_remote && !cached.is_empty() {
        return Ok(cached);
    }

    match gateway::fetch_models_for_picker() {
        Ok(models) => {
            let merged = merge_models_response(cached.clone(), models);
            if !merged.is_empty() {
                let _ = save_model_options_cache(&merged);
            }
            Ok(merged)
        }
        Err(err) => {
            if !cached.is_empty() {
                return Ok(cached);
            }
            if refresh_remote {
                Err(err)
            } else {
                Ok(ModelsResponse::default())
            }
        }
    }
}

/// 函数 `save_model_options_cache`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - models: 参数 models
///
/// # 返回
/// 返回函数执行结果
fn save_model_options_cache(models: &ModelsResponse) -> Result<(), String> {
    let storage =
        storage_helpers::open_storage().ok_or_else(|| "storage unavailable".to_string())?;
    save_model_options_with_storage(&storage, models)
}

pub(crate) fn save_model_options_with_storage(
    storage: &Storage,
    models: &ModelsResponse,
) -> Result<(), String> {
    let normalized = normalize_models_response(models.clone());
    let updated_at = now_ts();
    save_model_catalog_rows(storage, &normalized, updated_at)
}

pub(crate) fn deserialize_models_response(raw: &str) -> ModelsResponse {
    normalize_models_response(crate::gateway::parse_models_response(raw.as_bytes()))
}

/// 函数 `read_cached_model_options`
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
fn read_cached_model_options() -> Result<ModelsResponse, String> {
    let storage =
        storage_helpers::open_storage().ok_or_else(|| "storage unavailable".to_string())?;
    read_model_options_from_storage(&storage)
}

pub(crate) fn read_model_options_from_storage(storage: &Storage) -> Result<ModelsResponse, String> {
    let rows = storage
        .list_model_catalog_models(MODEL_CACHE_SCOPE_DEFAULT)
        .map_err(|e| e.to_string())?;
    let scope_record = storage
        .get_model_catalog_scope(MODEL_CACHE_SCOPE_DEFAULT)
        .map_err(|e| e.to_string())?;
    let legacy_cache = storage
        .get_model_options_cache(MODEL_CACHE_SCOPE_DEFAULT)
        .map_err(|e| e.to_string())?;

    if !rows.is_empty() {
        let reasoning_levels = storage
            .list_model_catalog_reasoning_levels(MODEL_CACHE_SCOPE_DEFAULT)
            .map_err(|e| e.to_string())?;
        let additional_speed_tiers = storage
            .list_model_catalog_additional_speed_tiers(MODEL_CACHE_SCOPE_DEFAULT)
            .map_err(|e| e.to_string())?;
        let experimental_supported_tools = storage
            .list_model_catalog_experimental_supported_tools(MODEL_CACHE_SCOPE_DEFAULT)
            .map_err(|e| e.to_string())?;
        let input_modalities = storage
            .list_model_catalog_input_modalities(MODEL_CACHE_SCOPE_DEFAULT)
            .map_err(|e| e.to_string())?;
        let available_in_plans = storage
            .list_model_catalog_available_in_plans(MODEL_CACHE_SCOPE_DEFAULT)
            .map_err(|e| e.to_string())?;

        let mut reasoning_by_slug = group_reasoning_levels_by_slug(reasoning_levels);
        let mut speed_tiers_by_slug = group_string_items_by_slug(additional_speed_tiers);
        let mut tools_by_slug = group_string_items_by_slug(experimental_supported_tools);
        let mut modalities_by_slug = group_string_items_by_slug(input_modalities);
        let mut plans_by_slug = group_string_items_by_slug(available_in_plans);

        let legacy_extra = legacy_cache
            .as_ref()
            .map(|cache| deserialize_models_response(&cache.items_json).extra)
            .unwrap_or_default();
        let response_extra = scope_record
            .as_ref()
            .and_then(|record| parse_extra_json_map(Some(record.extra_json.as_str())))
            .unwrap_or(legacy_extra);

        let mut rebuilt_models = Vec::new();
        for row in rows.iter().cloned() {
            let slug = row.slug.clone();
            if let Some(model) = model_info_from_row(
                row,
                reasoning_by_slug.remove(&slug),
                speed_tiers_by_slug.remove(&slug),
                tools_by_slug.remove(&slug),
                modalities_by_slug.remove(&slug),
                plans_by_slug.remove(&slug),
            ) {
                rebuilt_models.push(model);
            }
        }

        if !rebuilt_models.is_empty() {
            let updated_at = rows
                .iter()
                .map(|row| row.updated_at)
                .max()
                .unwrap_or_else(now_ts);
            let response = normalize_models_response(ModelsResponse {
                models: rebuilt_models,
                extra: response_extra,
            });
            if needs_structured_backfill(&rows, scope_record.is_none()) {
                let _ = save_model_catalog_rows(storage, &response, updated_at);
            }
            return Ok(response);
        }
    }

    let Some(cache) = legacy_cache else {
        return Ok(ModelsResponse::default());
    };
    let models = deserialize_models_response(&cache.items_json);
    if !models.is_empty() {
        let _ = save_model_catalog_rows(storage, &models, cache.updated_at);
    }
    Ok(models)
}

pub(crate) fn normalize_models_response(response: ModelsResponse) -> ModelsResponse {
    let mut models = Vec::new();
    let mut seen = HashSet::new();
    for model in response.models {
        if let Some(normalized) = normalize_model_info(model) {
            if seen.insert(normalized.slug.clone()) {
                models.push(normalized);
            }
        }
    }

    ModelsResponse {
        models,
        extra: response.extra,
    }
}

pub(crate) fn merge_models_response(
    cached: ModelsResponse,
    incoming: ModelsResponse,
) -> ModelsResponse {
    let cached = normalize_models_response(cached);
    let incoming = normalize_models_response(incoming);
    if cached.is_empty() {
        return incoming;
    }
    if incoming.is_empty() {
        return cached;
    }

    let cached_models = cached.models;
    let incoming_models = incoming.models;
    let mut cached_by_slug = BTreeMap::new();
    for model in &cached_models {
        cached_by_slug.insert(model.slug.clone(), model.clone());
    }

    let mut merged_models = Vec::new();
    let mut seen = HashSet::new();
    for incoming_model in incoming_models {
        let slug = incoming_model.slug.clone();
        let merged_model = match cached_by_slug.get(&slug) {
            Some(cached_model) => merge_model_info(cached_model.clone(), incoming_model),
            None => incoming_model,
        };
        seen.insert(slug);
        merged_models.push(merged_model);
    }

    for cached_model in cached_models {
        if seen.insert(cached_model.slug.clone()) {
            merged_models.push(cached_model);
        }
    }

    ModelsResponse {
        models: merged_models,
        extra: merge_extra_maps(cached.extra, incoming.extra),
    }
}

fn normalize_model_info(mut model: ModelInfo) -> Option<ModelInfo> {
    let slug = model.slug.trim().to_string();
    if slug.is_empty() {
        return None;
    }

    model.slug = slug;
    if model.display_name.trim().is_empty() {
        model.display_name = model.slug.clone();
    }
    if model.input_modalities.is_empty() {
        model.input_modalities = default_input_modalities();
    }
    Some(model)
}

fn model_info_from_row(
    row: ModelCatalogModelRecord,
    reasoning_levels: Option<Vec<ModelReasoningLevel>>,
    additional_speed_tiers: Option<Vec<String>>,
    experimental_supported_tools: Option<Vec<String>>,
    input_modalities: Option<Vec<String>>,
    available_in_plans: Option<Vec<String>>,
) -> Option<ModelInfo> {
    let mut model = ModelInfo {
        slug: row.slug.clone(),
        display_name: row.display_name.clone(),
        extra: parse_extra_json_map(Some(row.extra_json.as_str())).unwrap_or_default(),
        ..Default::default()
    };

    model.slug = row.slug.clone();
    if !row.display_name.trim().is_empty() {
        model.display_name = row.display_name.clone();
    }
    if let Some(description) = row.description {
        model.description = Some(description);
    }
    if let Some(default_reasoning_level) = row.default_reasoning_level {
        model.default_reasoning_level = Some(default_reasoning_level);
    }
    if let Some(shell_type) = row.shell_type {
        model.shell_type = Some(shell_type);
    }
    if let Some(visibility) = row.visibility {
        model.visibility = Some(visibility);
    }
    if let Some(supported_in_api) = row.supported_in_api {
        model.supported_in_api = supported_in_api;
    }
    if let Some(priority) = row.priority {
        model.priority = priority;
    }
    if let Some(availability_nux) = parse_json_value(row.availability_nux_json.as_deref()) {
        model.availability_nux = Some(availability_nux);
    }
    if let Some(upgrade) = parse_json_value(row.upgrade_json.as_deref()) {
        model.upgrade = Some(upgrade);
    }
    if let Some(base_instructions) = row.base_instructions {
        model.base_instructions = Some(base_instructions);
    }
    if let Some(model_messages) = parse_json_value(row.model_messages_json.as_deref()) {
        model.model_messages = Some(model_messages);
    }
    if let Some(supports_reasoning_summaries) = row.supports_reasoning_summaries {
        model.supports_reasoning_summaries = Some(supports_reasoning_summaries);
    }
    if let Some(default_reasoning_summary) = row.default_reasoning_summary {
        model.default_reasoning_summary = Some(default_reasoning_summary);
    }
    if let Some(support_verbosity) = row.support_verbosity {
        model.support_verbosity = Some(support_verbosity);
    }
    if let Some(default_verbosity) = parse_json_value(row.default_verbosity_json.as_deref()) {
        model.default_verbosity = Some(default_verbosity);
    }
    if let Some(apply_patch_tool_type) = row.apply_patch_tool_type {
        model.apply_patch_tool_type = Some(apply_patch_tool_type);
    }
    if let Some(web_search_tool_type) = row.web_search_tool_type {
        model.web_search_tool_type = Some(web_search_tool_type);
    }
    if let Some(truncation_policy) = build_truncation_policy(
        row.truncation_mode.as_deref(),
        row.truncation_limit,
        row.truncation_extra_json.as_deref(),
        model.truncation_policy.take(),
    ) {
        model.truncation_policy = Some(truncation_policy);
    }
    if let Some(supports_parallel_tool_calls) = row.supports_parallel_tool_calls {
        model.supports_parallel_tool_calls = Some(supports_parallel_tool_calls);
    }
    if let Some(supports_image_detail_original) = row.supports_image_detail_original {
        model.supports_image_detail_original = Some(supports_image_detail_original);
    }
    if let Some(context_window) = row.context_window {
        model.context_window = Some(context_window);
    }
    if let Some(auto_compact_token_limit) = row.auto_compact_token_limit {
        model.auto_compact_token_limit = Some(auto_compact_token_limit);
    }
    if let Some(effective_context_window_percent) = row.effective_context_window_percent {
        model.effective_context_window_percent = Some(effective_context_window_percent);
    }
    if let Some(minimal_client_version) =
        parse_json_value(row.minimal_client_version_json.as_deref())
    {
        model.minimal_client_version = Some(minimal_client_version);
    }
    if let Some(supports_search_tool) = row.supports_search_tool {
        model.supports_search_tool = Some(supports_search_tool);
    }
    if let Some(levels) = reasoning_levels {
        model.supported_reasoning_levels = levels;
    }
    if let Some(speed_tiers) = additional_speed_tiers {
        model.additional_speed_tiers = speed_tiers;
    }
    if let Some(tools) = experimental_supported_tools {
        model.experimental_supported_tools = tools;
    }
    if let Some(modalities) = input_modalities {
        model.input_modalities = modalities;
    }
    if let Some(plans) = available_in_plans {
        model.available_in_plans = plans;
    }

    normalize_model_info(model)
}

fn save_model_catalog_rows(
    storage: &Storage,
    models: &ModelsResponse,
    updated_at: i64,
) -> Result<(), String> {
    let scope_record = ModelCatalogScopeRecord {
        scope: MODEL_CACHE_SCOPE_DEFAULT.to_string(),
        extra_json: serialize_extra_map(&models.extra)?,
        updated_at,
    };
    storage
        .upsert_model_catalog_scope(&scope_record)
        .map_err(|e| e.to_string())?;

    let mut model_rows = Vec::new();
    let mut reasoning_rows = Vec::new();
    let mut additional_speed_tiers = Vec::new();
    let mut experimental_supported_tools = Vec::new();
    let mut input_modalities = Vec::new();
    let mut available_in_plans = Vec::new();

    for (index, model) in models.models.iter().enumerate() {
        model_rows.push(model_record_from_model(model, index as i64, updated_at)?);
        reasoning_rows.extend(reasoning_records_from_model(model, updated_at)?);
        additional_speed_tiers.extend(string_records_from_model(
            &model.slug,
            &model.additional_speed_tiers,
            updated_at,
        ));
        experimental_supported_tools.extend(string_records_from_model(
            &model.slug,
            &model.experimental_supported_tools,
            updated_at,
        ));
        input_modalities.extend(string_records_from_model(
            &model.slug,
            &model.input_modalities,
            updated_at,
        ));
        available_in_plans.extend(string_records_from_model(
            &model.slug,
            &model.available_in_plans,
            updated_at,
        ));
    }

    storage
        .upsert_model_catalog_models(&model_rows)
        .map_err(|e| e.to_string())?;
    storage
        .upsert_model_catalog_reasoning_levels(&reasoning_rows)
        .map_err(|e| e.to_string())?;
    storage
        .upsert_model_catalog_additional_speed_tiers(&additional_speed_tiers)
        .map_err(|e| e.to_string())?;
    storage
        .upsert_model_catalog_experimental_supported_tools(&experimental_supported_tools)
        .map_err(|e| e.to_string())?;
    storage
        .upsert_model_catalog_input_modalities(&input_modalities)
        .map_err(|e| e.to_string())?;
    storage
        .upsert_model_catalog_available_in_plans(&available_in_plans)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn model_record_from_model(
    model: &ModelInfo,
    sort_index: i64,
    updated_at: i64,
) -> Result<ModelCatalogModelRecord, String> {
    let truncation_extra_json = model
        .truncation_policy
        .as_ref()
        .map(|policy| serialize_extra_map(&policy.extra))
        .transpose()?;
    Ok(ModelCatalogModelRecord {
        scope: MODEL_CACHE_SCOPE_DEFAULT.to_string(),
        slug: model.slug.clone(),
        display_name: model.display_name.clone(),
        description: model.description.clone(),
        default_reasoning_level: model.default_reasoning_level.clone(),
        shell_type: model.shell_type.clone(),
        visibility: model.visibility.clone(),
        supported_in_api: Some(model.supported_in_api),
        priority: Some(model.priority),
        availability_nux_json: serialize_json_option(&model.availability_nux)?,
        upgrade_json: serialize_json_option(&model.upgrade)?,
        base_instructions: model.base_instructions.clone(),
        model_messages_json: serialize_json_option(&model.model_messages)?,
        supports_reasoning_summaries: model.supports_reasoning_summaries,
        default_reasoning_summary: model.default_reasoning_summary.clone(),
        support_verbosity: model.support_verbosity,
        default_verbosity_json: serialize_json_option(&model.default_verbosity)?,
        apply_patch_tool_type: model.apply_patch_tool_type.clone(),
        web_search_tool_type: model.web_search_tool_type.clone(),
        truncation_mode: model
            .truncation_policy
            .as_ref()
            .map(|policy| policy.mode.clone()),
        truncation_limit: model.truncation_policy.as_ref().map(|policy| policy.limit),
        truncation_extra_json,
        supports_parallel_tool_calls: model.supports_parallel_tool_calls,
        supports_image_detail_original: model.supports_image_detail_original,
        context_window: model.context_window,
        auto_compact_token_limit: model.auto_compact_token_limit,
        effective_context_window_percent: model.effective_context_window_percent,
        minimal_client_version_json: serialize_json_option(&model.minimal_client_version)?,
        supports_search_tool: model.supports_search_tool,
        extra_json: serialize_extra_map(&model.extra)?,
        sort_index,
        updated_at,
    })
}

fn reasoning_records_from_model(
    model: &ModelInfo,
    updated_at: i64,
) -> Result<Vec<ModelCatalogReasoningLevelRecord>, String> {
    let mut records = Vec::new();
    for (index, level) in model.supported_reasoning_levels.iter().enumerate() {
        records.push(ModelCatalogReasoningLevelRecord {
            scope: MODEL_CACHE_SCOPE_DEFAULT.to_string(),
            slug: model.slug.clone(),
            effort: level.effort.clone(),
            description: level.description.clone(),
            extra_json: serialize_extra_map(&level.extra)?,
            sort_index: index as i64,
            updated_at,
        });
    }
    Ok(records)
}

fn string_records_from_model(
    slug: &str,
    values: &[String],
    updated_at: i64,
) -> Vec<ModelCatalogStringItemRecord> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| ModelCatalogStringItemRecord {
            scope: MODEL_CACHE_SCOPE_DEFAULT.to_string(),
            slug: slug.to_string(),
            value: value.clone(),
            sort_index: index as i64,
            updated_at,
        })
        .collect()
}

fn merge_model_info(mut cached: ModelInfo, incoming: ModelInfo) -> ModelInfo {
    cached.slug = incoming.slug;
    cached.display_name = merge_string(cached.display_name, incoming.display_name);
    cached.description = merge_option_string(cached.description, incoming.description);
    cached.default_reasoning_level = merge_option_string(
        cached.default_reasoning_level,
        incoming.default_reasoning_level,
    );
    cached.supported_reasoning_levels = merge_reasoning_levels(
        cached.supported_reasoning_levels,
        incoming.supported_reasoning_levels,
    );
    cached.shell_type = merge_option_string(cached.shell_type, incoming.shell_type);
    cached.visibility = merge_option_string(cached.visibility, incoming.visibility);
    cached.supported_in_api = cached.supported_in_api || incoming.supported_in_api;
    cached.priority = merge_number(cached.priority, incoming.priority);
    cached.additional_speed_tiers = merge_string_vec(
        cached.additional_speed_tiers,
        incoming.additional_speed_tiers,
    );
    cached.availability_nux = incoming.availability_nux.or(cached.availability_nux);
    cached.upgrade = incoming.upgrade.or(cached.upgrade);
    cached.base_instructions =
        merge_option_string(cached.base_instructions, incoming.base_instructions);
    cached.model_messages = incoming.model_messages.or(cached.model_messages);
    cached.supports_reasoning_summaries = incoming
        .supports_reasoning_summaries
        .or(cached.supports_reasoning_summaries);
    cached.default_reasoning_summary = merge_option_string(
        cached.default_reasoning_summary,
        incoming.default_reasoning_summary,
    );
    cached.support_verbosity = incoming.support_verbosity.or(cached.support_verbosity);
    cached.default_verbosity = incoming.default_verbosity.or(cached.default_verbosity);
    cached.apply_patch_tool_type =
        merge_option_string(cached.apply_patch_tool_type, incoming.apply_patch_tool_type);
    cached.web_search_tool_type =
        merge_option_string(cached.web_search_tool_type, incoming.web_search_tool_type);
    cached.truncation_policy = incoming.truncation_policy.or(cached.truncation_policy);
    cached.supports_parallel_tool_calls = incoming
        .supports_parallel_tool_calls
        .or(cached.supports_parallel_tool_calls);
    cached.supports_image_detail_original = incoming
        .supports_image_detail_original
        .or(cached.supports_image_detail_original);
    cached.context_window = incoming.context_window.or(cached.context_window);
    cached.auto_compact_token_limit = incoming
        .auto_compact_token_limit
        .or(cached.auto_compact_token_limit);
    cached.effective_context_window_percent = incoming
        .effective_context_window_percent
        .or(cached.effective_context_window_percent);
    cached.experimental_supported_tools = merge_string_vec(
        cached.experimental_supported_tools,
        incoming.experimental_supported_tools,
    );
    cached.input_modalities = merge_string_vec(cached.input_modalities, incoming.input_modalities);
    cached.minimal_client_version = incoming
        .minimal_client_version
        .or(cached.minimal_client_version);
    cached.supports_search_tool = incoming
        .supports_search_tool
        .or(cached.supports_search_tool);
    cached.available_in_plans =
        merge_string_vec(cached.available_in_plans, incoming.available_in_plans);
    cached.extra = merge_extra_maps(cached.extra, incoming.extra);
    normalize_model_info(cached).unwrap_or_default()
}

fn merge_string(cached: String, incoming: String) -> String {
    if incoming.trim().is_empty() {
        cached
    } else {
        incoming
    }
}

fn merge_option_string(cached: Option<String>, incoming: Option<String>) -> Option<String> {
    match incoming {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => cached,
    }
}

fn merge_number(cached: i64, incoming: i64) -> i64 {
    if incoming == 0 {
        cached
    } else {
        incoming
    }
}

fn merge_reasoning_levels(
    cached: Vec<ModelReasoningLevel>,
    incoming: Vec<ModelReasoningLevel>,
) -> Vec<ModelReasoningLevel> {
    if incoming.is_empty() {
        cached
    } else {
        let mut cached_by_effort = BTreeMap::new();
        for level in cached {
            cached_by_effort.insert(level.effort.clone(), level);
        }

        let mut merged = Vec::new();
        let mut seen = HashSet::new();
        for level in incoming {
            let effort = level.effort.clone();
            let merged_level = match cached_by_effort.get(&effort) {
                Some(cached_level) => ModelReasoningLevel {
                    effort: effort.clone(),
                    description: merge_string(cached_level.description.clone(), level.description),
                    extra: merge_extra_maps(cached_level.extra.clone(), level.extra),
                },
                None => level,
            };
            seen.insert(effort);
            merged.push(merged_level);
        }

        for (effort, level) in cached_by_effort {
            if seen.insert(effort) {
                merged.push(level);
            }
        }
        merged
    }
}

fn merge_string_vec(cached: Vec<String>, incoming: Vec<String>) -> Vec<String> {
    if incoming.is_empty() {
        return cached;
    }

    let mut merged = Vec::new();
    let mut seen = HashSet::new();
    for value in incoming.into_iter().chain(cached) {
        let normalized = value.trim().to_string();
        if normalized.is_empty() || !seen.insert(normalized.clone()) {
            continue;
        }
        merged.push(normalized);
    }
    merged
}

fn merge_extra_maps(
    mut cached: BTreeMap<String, Value>,
    incoming: BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    cached.extend(incoming);
    cached
}

fn default_input_modalities() -> Vec<String> {
    vec!["text".to_string(), "image".to_string()]
}

fn serialize_json_option(value: &Option<Value>) -> Result<Option<String>, String> {
    value
        .as_ref()
        .map(|item| serde_json::to_string(item).map_err(|e| e.to_string()))
        .transpose()
}

fn serialize_extra_map(extra: &BTreeMap<String, Value>) -> Result<String, String> {
    serde_json::to_string(extra).map_err(|e| e.to_string())
}

fn parse_json_value(raw: Option<&str>) -> Option<Value> {
    raw.and_then(|item| serde_json::from_str::<Value>(item).ok())
}

fn parse_extra_json_map(raw: Option<&str>) -> Option<BTreeMap<String, Value>> {
    raw.and_then(|item| serde_json::from_str::<BTreeMap<String, Value>>(item).ok())
}

fn build_truncation_policy(
    mode: Option<&str>,
    limit: Option<i64>,
    extra_json: Option<&str>,
    existing: Option<ModelTruncationPolicy>,
) -> Option<ModelTruncationPolicy> {
    let has_row_value = mode.is_some() || limit.is_some() || extra_json.is_some();
    if !has_row_value {
        return existing;
    }

    let mut policy = existing.unwrap_or_default();
    if let Some(mode) = mode {
        policy.mode = mode.to_string();
    }
    if let Some(limit) = limit {
        policy.limit = limit;
    }
    if let Some(extra) = parse_extra_json_map(extra_json) {
        policy.extra = extra;
    }
    Some(policy)
}

fn group_reasoning_levels_by_slug(
    records: Vec<ModelCatalogReasoningLevelRecord>,
) -> BTreeMap<String, Vec<ModelReasoningLevel>> {
    let mut grouped = BTreeMap::new();
    for record in records {
        grouped
            .entry(record.slug)
            .or_insert_with(Vec::new)
            .push(ModelReasoningLevel {
                effort: record.effort,
                description: record.description,
                extra: parse_extra_json_map(Some(record.extra_json.as_str())).unwrap_or_default(),
            });
    }
    grouped
}

fn group_string_items_by_slug(
    records: Vec<ModelCatalogStringItemRecord>,
) -> BTreeMap<String, Vec<String>> {
    let mut grouped = BTreeMap::new();
    for record in records {
        grouped
            .entry(record.slug)
            .or_insert_with(Vec::new)
            .push(record.value);
    }
    grouped
}

fn needs_structured_backfill(rows: &[ModelCatalogModelRecord], missing_scope_row: bool) -> bool {
    missing_scope_row
        || rows.iter().any(|row| {
            row.supported_in_api.is_none()
                && row.priority.is_none()
                && row.visibility.is_none()
                && row.minimal_client_version_json.is_none()
                && row.context_window.is_none()
                && row.extra_json.trim().is_empty()
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use codexmanager_core::storage::{now_ts, Storage};
    use serde_json::{json, Value};

    use super::{
        merge_models_response, normalize_models_response, read_model_options_from_storage,
    };
    use codexmanager_core::rpc::types::{ModelInfo, ModelsResponse};

    #[test]
    fn normalize_models_response_keeps_full_model_metadata() {
        let response = ModelsResponse {
            models: vec![
                serde_json::from_value(json!({
                    "slug": "gpt-5",
                    "display_name": "GPT-5",
                    "supported_in_api": true,
                    "visibility": "list",
                    "supported_reasoning_levels": [
                        { "effort": "medium", "description": "balanced" }
                    ]
                }))
                .expect("parse model"),
                ModelInfo {
                    slug: " ".to_string(),
                    display_name: String::new(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let normalized = normalize_models_response(response);
        assert_eq!(normalized.models.len(), 1);
        assert_eq!(normalized.models[0].slug, "gpt-5");
        assert_eq!(normalized.models[0].display_name, "GPT-5");
        assert!(normalized.models[0].supported_in_api);
        assert_eq!(normalized.models[0].supported_reasoning_levels.len(), 1);
    }

    #[test]
    fn merge_models_response_updates_existing_without_removing_cached_fields() {
        let cached = ModelsResponse {
            models: vec![
                ModelInfo {
                    slug: "gpt-5".to_string(),
                    display_name: "GPT-5".to_string(),
                    description: Some("cached description".to_string()),
                    supported_in_api: true,
                    priority: 200,
                    input_modalities: vec!["text".to_string(), "image".to_string()],
                    additional_speed_tiers: vec!["fast".to_string()],
                    supported_reasoning_levels: vec![serde_json::from_value(json!({
                        "effort": "medium",
                        "description": "balanced"
                    }))
                    .expect("reasoning preset")],
                    ..Default::default()
                },
                ModelInfo {
                    slug: "gpt-legacy".to_string(),
                    display_name: "GPT Legacy".to_string(),
                    supported_in_api: true,
                    ..Default::default()
                },
            ],
            extra: BTreeMap::from([("etag".to_string(), json!("cached"))]),
        };
        let incoming = ModelsResponse {
            models: vec![
                ModelInfo {
                    slug: "gpt-5".to_string(),
                    display_name: "GPT-5 New".to_string(),
                    supported_in_api: false,
                    supported_reasoning_levels: vec![serde_json::from_value(json!({
                        "effort": "high",
                        "description": "deeper"
                    }))
                    .expect("reasoning preset")],
                    visibility: Some("list".to_string()),
                    additional_speed_tiers: vec!["turbo".to_string()],
                    ..Default::default()
                },
                ModelInfo {
                    slug: "gpt-new".to_string(),
                    display_name: "GPT New".to_string(),
                    supported_in_api: true,
                    ..Default::default()
                },
            ],
            extra: BTreeMap::from([("etag".to_string(), json!("fresh"))]),
        };

        let merged = merge_models_response(cached, incoming);
        assert_eq!(
            merged
                .models
                .iter()
                .map(|model| model.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["gpt-5", "gpt-new", "gpt-legacy"]
        );
        assert_eq!(merged.models[0].display_name, "GPT-5 New");
        assert_eq!(
            merged.models[0].description.as_deref(),
            Some("cached description")
        );
        assert!(merged.models[0].supported_in_api);
        assert_eq!(merged.models[0].priority, 200);
        assert_eq!(
            merged.models[0].input_modalities,
            vec!["text".to_string(), "image".to_string()]
        );
        assert_eq!(
            merged.models[0].additional_speed_tiers,
            vec!["turbo".to_string(), "fast".to_string()]
        );
        assert_eq!(merged.models[0].supported_reasoning_levels.len(), 2);
        assert_eq!(
            merged.extra.get("etag").and_then(Value::as_str),
            Some("fresh")
        );
    }

    #[test]
    fn read_model_options_from_storage_backfills_structured_tables_from_legacy_cache() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();
        let payload = ModelsResponse {
            models: vec![serde_json::from_value(json!({
                "slug": "gpt-5.4",
                "display_name": "GPT-5.4",
                "description": "Latest frontier model",
                "supported_in_api": true,
                "supported_reasoning_levels": [
                    { "effort": "medium", "description": "balanced" }
                ],
                "input_modalities": ["text", "image"],
                "available_in_plans": ["pro", "team"]
            }))
            .expect("parse model")],
            extra: BTreeMap::from([("etag".to_string(), json!("legacy"))]),
        };
        let payload_json = serde_json::to_string(&payload).expect("serialize payload");
        storage
            .upsert_model_options_cache("default", &payload_json, now)
            .expect("seed legacy cache");

        let response = read_model_options_from_storage(&storage).expect("read models");
        assert_eq!(response.models.len(), 1);
        assert_eq!(response.models[0].slug, "gpt-5.4");
        assert_eq!(
            response.extra.get("etag").and_then(Value::as_str),
            Some("legacy")
        );

        let scope = storage
            .get_model_catalog_scope("default")
            .expect("read scope")
            .expect("scope exists");
        assert_eq!(
            serde_json::from_str::<BTreeMap<String, Value>>(&scope.extra_json)
                .expect("parse scope extra")
                .get("etag")
                .and_then(Value::as_str),
            Some("legacy")
        );
        let models = storage
            .list_model_catalog_models("default")
            .expect("list model rows");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].display_name, "GPT-5.4");
        assert_eq!(
            models[0].description.as_deref(),
            Some("Latest frontier model")
        );
        let reasoning_levels = storage
            .list_model_catalog_reasoning_levels("default")
            .expect("list reasoning levels");
        assert_eq!(reasoning_levels.len(), 1);
        assert_eq!(reasoning_levels[0].effort, "medium");
        let plans = storage
            .list_model_catalog_available_in_plans("default")
            .expect("list plans");
        assert_eq!(
            plans
                .iter()
                .map(|item| item.value.as_str())
                .collect::<Vec<_>>(),
            vec!["pro", "team"]
        );
    }
}
