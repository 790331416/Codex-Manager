use super::*;
use codexmanager_core::rpc::types::{ModelInfo, ModelsResponse};
use serde_json::Value;

/// 函数 `serialize_models_response_outputs_official_shape`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn serialize_models_response_outputs_official_shape() {
    let items = ModelsResponse {
        models: vec![
            ModelInfo {
                slug: "gpt-5.3-codex".to_string(),
                display_name: "GPT-5.3 Codex".to_string(),
                supported_in_api: true,
                visibility: Some("list".to_string()),
                ..Default::default()
            },
            ModelInfo {
                slug: "gpt-4o".to_string(),
                display_name: "GPT-4o".to_string(),
                supported_in_api: true,
                visibility: Some("list".to_string()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let output = serialize_models_response(&items);
    let value: Value = serde_json::from_str(&output).expect("valid json");
    let models = value
        .get("models")
        .and_then(Value::as_array)
        .expect("models array");
    assert_eq!(models.len(), 2);
    assert_eq!(
        models[0].get("slug").and_then(Value::as_str),
        Some("gpt-5.3-codex")
    );
    assert_eq!(
        models[1].get("slug").and_then(Value::as_str),
        Some("gpt-4o")
    );
    assert_eq!(
        models[0].get("display_name").and_then(Value::as_str),
        Some("GPT-5.3 Codex")
    );
    assert_eq!(
        models[1].get("visibility").and_then(Value::as_str),
        Some("list")
    );
}

#[test]
fn response_models_for_client_can_hide_descriptions_without_touching_metadata() {
    let items = ModelsResponse {
        models: vec![ModelInfo {
            slug: "gpt-5.3-codex".to_string(),
            display_name: "GPT-5.3 Codex".to_string(),
            description: Some("Latest frontier agentic coding model.".to_string()),
            supported_in_api: true,
            visibility: Some("list".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let response = response_models_for_client(&items, true);
    assert_eq!(response.models.len(), 1);
    assert_eq!(response.models[0].slug, "gpt-5.3-codex");
    assert_eq!(response.models[0].display_name, "GPT-5.3 Codex");
    assert_eq!(response.models[0].description, None);
    assert!(response.models[0].supported_in_api);
    assert_eq!(response.models[0].visibility.as_deref(), Some("list"));

    assert_eq!(
        items.models[0].description.as_deref(),
        Some("Latest frontier agentic coding model.")
    );
}
