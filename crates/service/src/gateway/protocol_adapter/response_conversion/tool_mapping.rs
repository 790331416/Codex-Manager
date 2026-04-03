use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

use super::{stream_event_created, stream_event_model, stream_event_response_id};

/// 函数 `is_openai_chat_tool_item_type`
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
pub(super) fn is_openai_chat_tool_item_type(item_type: &str) -> bool {
    matches!(item_type, "function_call" | "custom_tool_call")
}

// 中文注释：请求侧可能把超长工具名缩短，这里在响应映射时按 restore_map 还原原始名称。
pub(super) fn restore_openai_tool_name(
    name: &str,
    tool_name_restore_map: Option<&super::ToolNameRestoreMap>,
) -> String {
    tool_name_restore_map
        .and_then(|map| map.get(name))
        .cloned()
        .unwrap_or_else(|| name.to_string())
}

/// 函数 `restore_openai_tool_name_in_tool_call`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - tool_call: 参数 tool_call
/// - tool_name_restore_map: 参数 tool_name_restore_map
///
/// # 返回
/// 无
fn restore_openai_tool_name_in_tool_call(
    tool_call: &mut Value,
    tool_name_restore_map: Option<&super::ToolNameRestoreMap>,
) {
    let Some(function_obj) = tool_call.get_mut("function").and_then(Value::as_object_mut) else {
        return;
    };
    let Some(name) = function_obj.get("name").and_then(Value::as_str) else {
        return;
    };
    let restored_name = restore_openai_tool_name(name, tool_name_restore_map);
    function_obj.insert("name".to_string(), Value::String(restored_name));
}

/// 函数 `restore_openai_tool_name_in_chat_choice`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 无
pub(super) fn restore_openai_tool_name_in_chat_choice(
    choice: &mut Value,
    tool_name_restore_map: Option<&super::ToolNameRestoreMap>,
) {
    if let Some(tool_calls) = choice
        .get_mut("message")
        .and_then(|message| message.get_mut("tool_calls"))
        .and_then(Value::as_array_mut)
    {
        for tool_call in tool_calls {
            restore_openai_tool_name_in_tool_call(tool_call, tool_name_restore_map);
        }
    }
    if let Some(tool_calls) = choice
        .get_mut("delta")
        .and_then(|delta| delta.get_mut("tool_calls"))
        .and_then(Value::as_array_mut)
    {
        for tool_call in tool_calls {
            restore_openai_tool_name_in_tool_call(tool_call, tool_name_restore_map);
        }
    }
}

/// 函数 `map_response_event_to_openai_chat_tool_chunk`
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
pub(super) fn map_response_event_to_openai_chat_tool_chunk(
    value: &Value,
    tool_name_restore_map: Option<&super::ToolNameRestoreMap>,
) -> Option<Value> {
    let chunk_type = value.get("type").and_then(Value::as_str)?;
    let tool_call = match chunk_type {
        "response.output_item.added" | "response.output_item.done" => {
            let item = value
                .get("item")
                .or_else(|| value.get("output_item"))
                .and_then(Value::as_object)?;
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
            if !is_openai_chat_tool_item_type(item_type) {
                return None;
            }
            let output_index = value
                .get("output_index")
                .and_then(Value::as_i64)
                .or_else(|| item.get("index").and_then(Value::as_i64))
                .unwrap_or(0);
            let call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .map(|raw| restore_openai_tool_name(raw, tool_name_restore_map))
                .unwrap_or_default();
            let arguments = if chunk_type == "response.output_item.added" {
                String::new()
            } else {
                item.get("arguments")
                    .or_else(|| item.get("input"))
                    .map(|arguments| {
                        arguments.as_str().map(str::to_string).unwrap_or_else(|| {
                            serde_json::to_string(arguments).unwrap_or_else(|_| "{}".to_string())
                        })
                    })
                    .unwrap_or_default()
            };
            let mut tool_call = json!({
                "index": output_index,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": arguments
                }
            });
            if !call_id.is_empty() {
                tool_call["id"] = Value::String(call_id);
            }
            tool_call
        }
        "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
            let arguments = value
                .get("delta")
                .and_then(Value::as_str)
                .or_else(|| value.get("arguments").and_then(Value::as_str))
                .unwrap_or_default()
                .to_string();
            if arguments.is_empty() {
                return None;
            }
            let output_index = value
                .get("output_index")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let mut tool_call = json!({
                "index": output_index,
                "function": {
                    "arguments": arguments
                }
            });
            if let Some(call_id) = value
                .get("call_id")
                .or_else(|| value.get("item_id"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .filter(|id| !id.is_empty())
            {
                tool_call["id"] = Value::String(call_id);
            }
            tool_call
        }
        _ => return None,
    };

    let include_role = chunk_type == "response.output_item.added";
    let mut chunk = json!({
        "id": stream_event_response_id(value),
        "object": "chat.completion.chunk",
        "created": stream_event_created(value),
        "model": stream_event_model(value),
        "choices": [{
            "index": 0,
            "delta": {
                "tool_calls": []
            },
            "finish_reason": Value::Null
        }]
    });
    if include_role {
        chunk["choices"][0]["delta"]["role"] = Value::String("assistant".to_string());
    }
    chunk["choices"][0]["delta"]["tool_calls"] = Value::Array(vec![tool_call]);
    Some(chunk)
}

#[derive(Default)]
pub(super) struct AggregatedChatToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

/// 函数 `merge_tool_call_arguments`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - existing: 参数 existing
/// - fragment: 参数 fragment
///
/// # 返回
/// 无
fn merge_tool_call_arguments(existing: &mut String, fragment: &str) {
    if fragment.is_empty() {
        return;
    }
    if existing.is_empty() {
        existing.push_str(fragment);
        return;
    }
    if existing == fragment || existing.ends_with(fragment) || existing.starts_with(fragment) {
        return;
    }
    if fragment.starts_with(existing.as_str()) {
        *existing = fragment.to_string();
        return;
    }
    existing.push_str(fragment);
}

/// 函数 `merge_chat_tool_call_object`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - tool_obj: 参数 tool_obj
/// - default_index: 参数 default_index
/// - tool_calls: 参数 tool_calls
///
/// # 返回
/// 无
fn merge_chat_tool_call_object(
    tool_obj: &Map<String, Value>,
    default_index: usize,
    tool_calls: &mut BTreeMap<usize, AggregatedChatToolCall>,
) {
    let index = tool_obj
        .get("index")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(default_index);
    let entry = tool_calls.entry(index).or_default();
    if let Some(id) = tool_obj
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        entry.id = Some(id.to_string());
    }
    if let Some(name) = tool_obj
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        entry.name = Some(name.to_string());
    }
    let Some(function_obj) = tool_obj.get("function").and_then(Value::as_object) else {
        return;
    };
    if let Some(name) = function_obj
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        entry.name = Some(name.to_string());
    }
    if let Some(arguments) = function_obj.get("arguments") {
        if let Some(raw) = arguments.as_str() {
            merge_tool_call_arguments(&mut entry.arguments, raw);
        } else if let Ok(serialized) = serde_json::to_string(arguments) {
            merge_tool_call_arguments(&mut entry.arguments, serialized.as_str());
        }
    }
}

/// 函数 `collect_chat_tool_calls_from_delta`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 无
pub(super) fn collect_chat_tool_calls_from_delta(
    delta: &Map<String, Value>,
    tool_calls: &mut BTreeMap<usize, AggregatedChatToolCall>,
) {
    let Some(items) = delta.get("tool_calls").and_then(Value::as_array) else {
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(tool_obj) = item.as_object() else {
            continue;
        };
        merge_chat_tool_call_object(tool_obj, index, tool_calls);
    }
}

/// 函数 `collect_chat_tool_calls_from_message`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - super: 参数 super
///
/// # 返回
/// 无
#[allow(dead_code)]
pub(super) fn collect_chat_tool_calls_from_message(
    message: &Map<String, Value>,
    tool_calls: &mut BTreeMap<usize, AggregatedChatToolCall>,
) {
    let Some(items) = message.get("tool_calls").and_then(Value::as_array) else {
        return;
    };
    for (index, item) in items.iter().enumerate() {
        let Some(tool_obj) = item.as_object() else {
            continue;
        };
        merge_chat_tool_call_object(tool_obj, index, tool_calls);
    }
}

/// 粗略衡量「可执行编辑」信息量：用于避免仅用字符串长度选用 `completed`，
/// 从而用更长但 `edits:[]` 的快照覆盖流式拼出的有效 `edit` 参数（OpenClaw 报 `edits:[]`）。
fn tool_arguments_edit_payload_score(v: &Value) -> usize {
    if let Some(edits) = v.get("edits").and_then(Value::as_array) {
        let mut n = 0;
        for e in edits {
            let Some(obj) = e.as_object() else {
                continue;
            };
            let has_old = obj
                .get("oldText")
                .or_else(|| obj.get("old_text"))
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty());
            let has_new = obj
                .get("newText")
                .or_else(|| obj.get("new_text"))
                .and_then(Value::as_str)
                .is_some();
            if has_old || has_new {
                n += 1;
            }
        }
        if n > 0 {
            return n;
        }
    }
    let has_path = v
        .get("path")
        .or_else(|| v.get("file"))
        .or_else(|| v.get("filePath"))
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty());
    let has_old = v
        .get("oldText")
        .or_else(|| v.get("old_text"))
        .and_then(Value::as_str)
        .is_some();
    if has_path && has_old {
        return 1;
    }
    0
}

fn streamed_tool_arguments_should_be_kept_over_completed(streamed: &str, completed: &str) -> bool {
    let Ok(sv) = serde_json::from_str::<Value>(streamed) else {
        return false;
    };
    let Ok(cv) = serde_json::from_str::<Value>(completed) else {
        return false;
    };
    let s = tool_arguments_edit_payload_score(&sv);
    let c = tool_arguments_edit_payload_score(&cv);
    s > c
}

/// `convert_openai_sse_to_chat_completions_json` 在流式阶段用 `merge_tool_call_arguments` 聚合；
/// `response.completed` 里的 `message.tool_calls` 可能更完整。若仅当 `content.is_empty()` 才合并，
/// 则助手先输出任意文字时永远不会合并，最终 `arguments` 为空、`build_openai_chat_tool_calls` 退化为 `"{}"`。
/// 此处按索引合并：**优先更长**的 `function.arguments` 字符串，并忽略占位 `"{}"`。
/// 若更长快照的「可执行编辑」信息量更低（典型：`edits:[]`），则保留流式累积结果。
pub(super) fn merge_tool_calls_from_completed_message_prefer_longer(
    aggregated: &mut BTreeMap<usize, AggregatedChatToolCall>,
    message: &Map<String, Value>,
) {
    let Some(items) = message.get("tool_calls").and_then(Value::as_array) else {
        return;
    };
    for (fallback_index, item) in items.iter().enumerate() {
        let Some(tool_obj) = item.as_object() else {
            continue;
        };
        let index = tool_obj
            .get("index")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(fallback_index);
        let entry = aggregated.entry(index).or_default();
        if let Some(id) = tool_obj
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if entry.id.is_none() {
                entry.id = Some(id.to_string());
            }
        }
        let Some(function_obj) = tool_obj.get("function").and_then(Value::as_object) else {
            continue;
        };
        if let Some(name) = function_obj
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if entry.name.is_none() {
                entry.name = Some(name.to_string());
            }
        }
        let Some(completed_args) = function_obj.get("arguments").and_then(Value::as_str) else {
            continue;
        };
        let trimmed = completed_args.trim();
        if trimmed.is_empty() || matches!(trimmed, "{}" | "[]" | "null") {
            continue;
        }
        if entry.arguments.is_empty() {
            entry.arguments = completed_args.to_string();
            continue;
        }
        if completed_args.len() > entry.arguments.len() {
            if streamed_tool_arguments_should_be_kept_over_completed(
                &entry.arguments,
                completed_args,
            ) {
                continue;
            }
            entry.arguments = completed_args.to_string();
        }
    }
}

#[cfg(test)]
mod merge_completed_tests {
    use super::*;

    #[test]
    fn merge_keeps_streamed_when_completed_longer_but_empty_edits() {
        let mut aggregated = BTreeMap::new();
        let streamed = r#"{"path":"/tmp/a.ts","edits":[{"oldText":"x","newText":"y"}]}"#;
        aggregated.insert(
            0,
            AggregatedChatToolCall {
                id: Some("call_1".to_string()),
                name: Some("edit".to_string()),
                arguments: streamed.to_string(),
            },
        );
        let completed = r#"{"path":"/tmp/a.ts","edits":[],"_pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#;
        assert!(completed.len() > streamed.len());
        let message = serde_json::json!({
            "tool_calls": [{
                "index": 0,
                "function": {
                    "name": "edit",
                    "arguments": completed
                }
            }]
        });
        let message = message.as_object().unwrap().clone();
        merge_tool_calls_from_completed_message_prefer_longer(&mut aggregated, &message);
        assert_eq!(aggregated[&0].arguments, streamed);
    }

    #[test]
    fn merge_takes_completed_when_strongly_better() {
        let mut aggregated = BTreeMap::new();
        let streamed = r#"{"path":"/tmp/a.ts","edits":[]}"#;
        aggregated.insert(
            0,
            AggregatedChatToolCall {
                id: Some("call_1".to_string()),
                name: Some("edit".to_string()),
                arguments: streamed.to_string(),
            },
        );
        let completed =
            r#"{"path":"/tmp/a.ts","edits":[{"oldText":"full","newText":"patch"}],"extra":true}"#;
        assert!(completed.len() > streamed.len());
        let message = serde_json::json!({
            "tool_calls": [{
                "index": 0,
                "function": {
                    "name": "edit",
                    "arguments": completed
                }
            }]
        });
        let message = message.as_object().unwrap().clone();
        merge_tool_calls_from_completed_message_prefer_longer(&mut aggregated, &message);
        assert_eq!(aggregated[&0].arguments, completed);
    }
}

/// 函数 `build_openai_chat_tool_calls`
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
pub(super) fn build_openai_chat_tool_calls(
    tool_calls: &BTreeMap<usize, AggregatedChatToolCall>,
) -> Vec<Value> {
    let mut out = Vec::new();
    for (index, call) in tool_calls {
        let id = call.id.clone().unwrap_or_else(|| format!("call_{index}"));
        let name = call.name.clone().unwrap_or_else(|| "tool".to_string());
        let arguments = if call.arguments.is_empty() {
            "{}".to_string()
        } else {
            call.arguments.clone()
        };
        out.push(json!({
            "id": id,
            "type": "function",
            "function": {
                "name": name,
                "arguments": arguments
            }
        }));
    }
    out
}
