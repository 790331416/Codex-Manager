use codexmanager_core::rpc::types::ModelsResponse;
const MODEL_CACHE_SCOPE_DEFAULT: &str = "default";

/// 函数 `serialize_models_response`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-12
///
/// # 参数
/// - models: 参数 models
///
/// # 返回
/// 返回函数执行结果
fn serialize_models_response(models: &ModelsResponse) -> String {
    serde_json::to_string(models).unwrap_or_else(|_| "{\"models\":[]}".to_string())
}

fn should_hide_model_descriptions_for_request(request: &tiny_http::Request) -> bool {
    request.headers().iter().any(|header| {
        header.field.equiv("User-Agent")
            && header
                .value
                .as_str()
                .to_ascii_lowercase()
                .contains("codex_cli_rs")
    })
}

fn response_models_for_client(models: &ModelsResponse, hide_descriptions: bool) -> ModelsResponse {
    if !hide_descriptions {
        return models.clone();
    }

    let mut response = models.clone();
    for model in &mut response.models {
        model.description = None;
    }
    response
}

/// 函数 `read_cached_models_response`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-12
///
/// # 参数
/// - storage: 参数 storage
///
/// # 返回
/// 返回函数执行结果
fn read_cached_models_response(
    storage: &codexmanager_core::storage::Storage,
) -> Result<ModelsResponse, String> {
    crate::apikey_models::read_model_options_from_storage(storage)
}

/// 函数 `maybe_respond_local_models`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 返回函数执行结果
pub(super) fn maybe_respond_local_models(
    request: tiny_http::Request,
    trace_id: &str,
    key_id: &str,
    protocol_type: &str,
    original_path: &str,
    path: &str,
    response_adapter: super::ResponseAdapter,
    request_method: &str,
    model_for_log: Option<&str>,
    reasoning_for_log: Option<&str>,
    storage: &codexmanager_core::storage::Storage,
) -> Result<Option<tiny_http::Request>, String> {
    let is_models_list = request_method.eq_ignore_ascii_case("GET")
        && (path == "/v1/models" || path.starts_with("/v1/models?"));
    if !is_models_list {
        return Ok(Some(request));
    }
    let context = super::local_response::LocalResponseContext {
        trace_id,
        key_id,
        protocol_type,
        original_path,
        path,
        response_adapter,
        request_method,
        model_for_log,
        reasoning_for_log,
        storage,
    };
    let hide_descriptions = should_hide_model_descriptions_for_request(&request);

    let cached = match read_cached_models_response(storage) {
        Ok(models) => models,
        Err(err) => {
            let message = crate::gateway::bilingual_error(
                "读取模型缓存失败",
                format!("model options cache read failed: {err}"),
            );
            super::local_response::respond_local_terminal_error(request, &context, 503, message)?;
            return Ok(None);
        }
    };

    let models = if !cached.is_empty() {
        cached
    } else {
        match super::fetch_models_for_picker() {
            Ok(fetched) if !fetched.is_empty() => {
                let merged = crate::apikey_models::merge_models_response(cached.clone(), fetched);
                if let Err(err) =
                    crate::apikey_models::save_model_options_with_storage(storage, &merged)
                {
                    log::warn!(
                        "event=gateway_model_catalog_upsert_failed scope={} err={}",
                        MODEL_CACHE_SCOPE_DEFAULT,
                        err
                    );
                }
                merged
            }
            Ok(_) => {
                let message = crate::gateway::bilingual_error(
                    "模型刷新后返回空目录",
                    "models refresh returned empty catalog",
                );
                super::local_response::respond_local_terminal_error(request, &context, 503, message)?;
                return Ok(None);
            }
            Err(err) => {
                let message = crate::gateway::bilingual_error(
                    "模型刷新失败",
                    format!("models refresh failed: {err}"),
                );
                super::local_response::respond_local_terminal_error(request, &context, 503, message)?;
                return Ok(None);
            }
        }
    };

    let response_models = response_models_for_client(&models, hide_descriptions);
    let output = serialize_models_response(&response_models);
    super::local_response::respond_local_json(
        request,
        &context,
        output,
        super::request_log::RequestLogUsage::default(),
    )?;
    Ok(None)
}

#[cfg(test)]
#[path = "tests/local_models_tests.rs"]
mod tests;
