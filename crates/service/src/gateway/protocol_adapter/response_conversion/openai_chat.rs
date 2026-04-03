use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

use super::json_conversion::{
    extract_function_call_arguments_raw, summarize_special_response_item_text,
};
use super::tool_mapping::{
    build_openai_chat_tool_calls, collect_chat_tool_calls_from_delta,
    is_openai_chat_tool_item_type, map_response_event_to_openai_chat_tool_chunk,
    merge_tool_calls_from_completed_message_prefer_longer, restore_openai_tool_name,
    restore_openai_tool_name_in_chat_choice, AggregatedChatToolCall,
};
use super::{is_response_completed_event_type, parse_openai_sse_event_value, ToolNameRestoreMap};

/// 函数 `extract_chat_content_text`
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
pub(super) fn extract_chat_content_text(content: Option<&Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    match content {
        Value::String(text) => text.clone(),
        Value::Array(items) => {
            let mut parts = Vec::new();
            for item in items {
                if let Some(text) = item.as_str() {
                    if !text.is_empty() {
                        parts.push(text.to_string());
                    }
                    continue;
                }
                if let Some(obj) = item.as_object() {
                    let item_type = obj.get("type").and_then(Value::as_str).unwrap_or_default();
                    if matches!(item_type, "text" | "input_text" | "output_text") {
                        if let Some(text) = obj.get("text").and_then(Value::as_str) {
                            if !text.is_empty() {
                                parts.push(text.to_string());
                            }
                        }
                    }
                }
            }
            parts.join("")
        }
        _ => String::new(),
    }
}

/// 函数 `collect_text_from_response_content`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - content: 参数 content
/// - out: 参数 out
///
/// # 返回
/// 无
fn collect_text_from_response_content(content: &Value, out: &mut String) {
    match content {
        Value::String(text) => out.push_str(text),
        Value::Array(items) => {
            for item in items {
                let Some(item_obj) = item.as_object() else {
                    if let Some(text) = item.as_str() {
                        out.push_str(text);
                    }
                    continue;
                };
                let item_type = item_obj
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if matches!(item_type, "text" | "input_text" | "output_text") {
                    if let Some(text) = item_obj.get("text").and_then(Value::as_str) {
                        out.push_str(text);
                    }
                }
            }
        }
        Value::Object(map) => {
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                out.push_str(text);
            }
            if let Some(content) = map.get("content") {
                collect_text_from_response_content(content, out);
            }
        }
        _ => {}
    }
}

/// 函数 `stream_event_response_id`
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
pub(super) fn stream_event_response_id(value: &Value) -> String {
    value
        .get("response_id")
        .and_then(Value::as_str)
        .or_else(|| value.get("id").and_then(Value::as_str))
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
        })
        .unwrap_or("")
        .to_string()
}

/// 函数 `stream_event_model`
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
pub(super) fn stream_event_model(value: &Value) -> String {
    value
        .get("model")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("model"))
                .and_then(Value::as_str)
        })
        .unwrap_or("")
        .to_string()
}

/// 函数 `stream_event_created`
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
pub(super) fn stream_event_created(value: &Value) -> i64 {
    value
        .get("created")
        .and_then(Value::as_i64)
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("created"))
                .and_then(Value::as_i64)
        })
        .unwrap_or(0)
}

/// 函数 `extract_stream_event_text`
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
pub(super) fn extract_stream_event_text(value: &Value) -> String {
    if let Some(delta) = value.get("delta") {
        if let Some(text) = delta.as_str() {
            return text.to_string();
        }
        if let Some(text) = delta.get("text").and_then(Value::as_str) {
            return text.to_string();
        }
    }
    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return text.to_string();
    }

    let mut out = String::new();
    if let Some(part) = value.get("part").or_else(|| value.get("content_part")) {
        collect_text_from_response_content(part, &mut out);
        if !out.is_empty() {
            return out;
        }
    }

    if let Some(item) = value.get("item") {
        if let Some(content) = item.get("content") {
            collect_text_from_response_content(content, &mut out);
        } else {
            collect_text_from_response_content(item, &mut out);
        }
    }
    out
}

/// 函数 `build_openai_chat_text_chunk`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - value: 参数 value
/// - text: 参数 text
///
/// # 返回
/// 返回函数执行结果
fn build_openai_chat_text_chunk(value: &Value, text: &str) -> Value {
    json!({
        "id": stream_event_response_id(value),
        "object": "chat.completion.chunk",
        "created": stream_event_created(value),
        "model": stream_event_model(value),
        "choices": [{
            "index": 0,
            "delta": {
                "role": "assistant",
                "content": text
            },
            "finish_reason": Value::Null
        }]
    })
}

/// 函数 `append_text_from_response_output_item`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - item_obj: 参数 item_obj
/// - out: 参数 out
///
/// # 返回
/// 无
fn append_text_from_response_output_item(item_obj: &Map<String, Value>, out: &mut String) {
    if let Some(content) = item_obj.get("content") {
        collect_text_from_response_content(content, out);
    }
    if let Some(text) = item_obj.get("text").and_then(Value::as_str) {
        out.push_str(text);
    }
    if let Some(delta) = item_obj.get("delta").and_then(Value::as_str) {
        out.push_str(delta);
    }
    if let Some(part) = item_obj
        .get("part")
        .or_else(|| item_obj.get("content_part"))
    {
        collect_text_from_response_content(part, out);
    }
}

/// 函数 `extract_text_from_response_output_payload`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - item_obj: 参数 item_obj
///
/// # 返回
/// 返回函数执行结果
fn extract_text_from_response_output_payload(item_obj: &Map<String, Value>) -> String {
    let mut out = String::new();
    if let Some(output) = item_obj.get("output") {
        collect_text_from_response_content(output, &mut out);
    }
    out
}

/// 函数 `map_openai_response_to_chat_completion`
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
pub(super) fn map_openai_response_to_chat_completion(
    value: &Value,
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
) -> Value {
    if value
        .get("object")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "chat.completion")
    {
        let mut cloned = value.clone();
        if let Some(choices) = cloned.get_mut("choices").and_then(Value::as_array_mut) {
            for choice in choices {
                restore_openai_tool_name_in_chat_choice(choice, tool_name_restore_map);
            }
        }
        return cloned;
    }
    let source = value.get("response").unwrap_or(value);
    let id = source
        .get("id")
        .cloned()
        .or_else(|| value.get("id").cloned())
        .unwrap_or_else(|| Value::String(String::new()));
    let created = source
        .get("created")
        .cloned()
        .or_else(|| source.get("created_at").cloned())
        .or_else(|| value.get("created").cloned())
        .or_else(|| value.get("created_at").cloned())
        .unwrap_or_else(|| Value::Number(0.into()));
    let model = source
        .get("model")
        .cloned()
        .or_else(|| value.get("model").cloned())
        .unwrap_or_else(|| Value::String(String::new()));
    let usage = source
        .get("usage")
        .cloned()
        .or_else(|| value.get("usage").cloned());

    let mut assistant_text = String::new();
    let mut tool_calls = Vec::<Value>::new();
    if let Some(output_items) = source.get("output").and_then(Value::as_array) {
        for (idx, item) in output_items.iter().enumerate() {
            let Some(item_obj) = item.as_object() else {
                continue;
            };
            let item_type = item_obj
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if item_type == "message" {
                append_text_from_response_output_item(item_obj, &mut assistant_text);
                continue;
            }
            if is_openai_chat_tool_item_type(item_type) {
                let call_id = item_obj
                    .get("call_id")
                    .or_else(|| item_obj.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("call_{idx}"));
                let name = item_obj
                    .get("name")
                    .and_then(Value::as_str)
                    .map(|raw| restore_openai_tool_name(raw, tool_name_restore_map))
                    .unwrap_or_else(|| "tool".to_string());
                let arguments = extract_function_call_arguments_raw(item_obj)
                    .unwrap_or_else(|| "{}".to_string());
                tool_calls.push(json!({
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": arguments
                    }
                }));
                continue;
            }
            if matches!(
                item_type,
                "function_call_output" | "custom_tool_call_output"
            ) {
                let output_text = extract_text_from_response_output_payload(item_obj);
                if !output_text.is_empty() {
                    assistant_text.push_str(output_text.as_str());
                }
                continue;
            }
            if let Some(summary) = summarize_special_response_item_text(item_obj) {
                assistant_text.push_str(summary.as_str());
                continue;
            }
            append_text_from_response_output_item(item_obj, &mut assistant_text);
        }
    }
    if assistant_text.is_empty() {
        if let Some(fallback_text) = source.get("output_text").and_then(Value::as_str) {
            assistant_text.push_str(fallback_text);
        }
    }

    let mut message = serde_json::Map::new();
    message.insert("role".to_string(), Value::String("assistant".to_string()));
    if !assistant_text.is_empty() {
        message.insert("content".to_string(), Value::String(assistant_text));
    } else {
        message.insert("content".to_string(), Value::Null);
    }
    if !tool_calls.is_empty() {
        message.insert("tool_calls".to_string(), Value::Array(tool_calls));
    }
    let finish_reason = if message.get("tool_calls").is_some() {
        "tool_calls"
    } else {
        "stop"
    };

    let mut out = serde_json::Map::new();
    out.insert("id".to_string(), id);
    out.insert(
        "object".to_string(),
        Value::String("chat.completion".to_string()),
    );
    out.insert("created".to_string(), created);
    out.insert("model".to_string(), model);
    out.insert(
        "choices".to_string(),
        Value::Array(vec![json!({
            "index": 0,
            "message": Value::Object(message),
            "finish_reason": finish_reason
        })]),
    );
    if let Some(usage) = usage {
        out.insert("usage".to_string(), usage);
    }
    Value::Object(out)
}

/// 函数 `convert_openai_chat_stream_chunk`
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
#[allow(dead_code)]
pub(super) fn convert_openai_chat_stream_chunk(value: &Value) -> Option<Value> {
    convert_openai_chat_stream_chunk_with_tool_name_restore_map(value, None)
}

/// 函数 `convert_openai_chat_stream_chunk_with_tool_name_restore_map`
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
pub(super) fn convert_openai_chat_stream_chunk_with_tool_name_restore_map(
    value: &Value,
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
) -> Option<Value> {
    if value
        .get("object")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "chat.completion.chunk")
    {
        let mut cloned = value.clone();
        if let Some(choices) = cloned.get_mut("choices").and_then(Value::as_array_mut) {
            for choice in choices {
                restore_openai_tool_name_in_chat_choice(choice, tool_name_restore_map);
            }
        }
        return Some(cloned);
    }

    if let Some(chunk_type) = value.get("type").and_then(Value::as_str) {
        match chunk_type {
            "response.output_text.delta"
            | "response.output_text.done"
            | "response.content_part.added"
            | "response.content_part.delta"
            | "response.content_part.done" => {
                let text = extract_stream_event_text(value);
                if text.is_empty() {
                    return None;
                }
                return Some(build_openai_chat_text_chunk(value, text.as_str()));
            }
            "response.output_item.added" | "response.output_item.done" => {
                if let Some(tool_chunk) =
                    map_response_event_to_openai_chat_tool_chunk(value, tool_name_restore_map)
                {
                    return Some(tool_chunk);
                }
                if let Some(text) = value
                    .get("item")
                    .or_else(|| value.get("output_item"))
                    .and_then(Value::as_object)
                    .and_then(|item| {
                        let item_type =
                            item.get("type").and_then(Value::as_str).unwrap_or_default();
                        if matches!(
                            item_type,
                            "function_call_output" | "custom_tool_call_output"
                        ) {
                            let text = extract_text_from_response_output_payload(item);
                            if text.is_empty() {
                                None
                            } else {
                                Some(text)
                            }
                        } else {
                            None
                        }
                    })
                {
                    return Some(build_openai_chat_text_chunk(value, text.as_str()));
                }
                if let Some(summary) = value
                    .get("item")
                    .or_else(|| value.get("output_item"))
                    .and_then(Value::as_object)
                    .and_then(summarize_special_response_item_text)
                {
                    return Some(build_openai_chat_text_chunk(value, summary.as_str()));
                }
                let text = extract_stream_event_text(value);
                if text.is_empty() {
                    return None;
                }
                return Some(build_openai_chat_text_chunk(value, text.as_str()));
            }
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                return map_response_event_to_openai_chat_tool_chunk(value, tool_name_restore_map);
            }
            "response.completed" | "response.done" => {
                let response = value.get("response").unwrap_or(&Value::Null);
                let fallback_id = stream_event_response_id(value);
                let fallback_model = stream_event_model(value);
                let fallback_created = stream_event_created(value);
                let response_id = response
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or(fallback_id);
                let response_model = response
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or(fallback_model);
                let response_created = response
                    .get("created")
                    .and_then(Value::as_i64)
                    .or_else(|| response.get("created_at").and_then(Value::as_i64))
                    .unwrap_or(fallback_created);
                let usage = response
                    .get("usage")
                    .cloned()
                    .or_else(|| value.get("usage").cloned());
                let finish_reason = if response
                    .get("output")
                    .and_then(Value::as_array)
                    .is_some_and(|items| {
                        items.iter().any(|item| {
                            item.get("type")
                                .and_then(Value::as_str)
                                .is_some_and(is_openai_chat_tool_item_type)
                        })
                    }) {
                    "tool_calls"
                } else {
                    "stop"
                };
                let mut out = json!({
                    "id": response_id,
                    "object": "chat.completion.chunk",
                    "created": response_created,
                    "model": response_model,
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": finish_reason
                    }]
                });
                if let Some(usage) = usage {
                    if let Some(out_obj) = out.as_object_mut() {
                        out_obj.insert("usage".to_string(), usage);
                    }
                }
                return Some(out);
            }
            _ => {}
        }

        if let Some(tool_chunk) =
            map_response_event_to_openai_chat_tool_chunk(value, tool_name_restore_map)
        {
            return Some(tool_chunk);
        }
        let text = extract_stream_event_text(value);
        if !text.is_empty() {
            return Some(build_openai_chat_text_chunk(value, text.as_str()));
        }
    }
    None
}

/// 函数 `convert_openai_json_to_chat_completions`
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
pub(super) fn convert_openai_json_to_chat_completions(
    body: &[u8],
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
) -> Result<(Vec<u8>, &'static str), String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| "invalid upstream json payload".to_string())?;
    let mapped = map_openai_response_to_chat_completion(&value, tool_name_restore_map);
    let bytes = serde_json::to_vec(&mapped)
        .map_err(|err| format!("serialize chat.completion json failed: {err}"))?;
    Ok((bytes, "application/json"))
}

/// 函数 `convert_openai_sse_to_chat_completions_json`
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
pub(super) fn convert_openai_sse_to_chat_completions_json(
    body: &[u8],
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
) -> Result<(Vec<u8>, &'static str), String> {
    let text = std::str::from_utf8(body).map_err(|_| "invalid upstream sse bytes".to_string())?;
    let mut id = String::new();
    let mut model = String::new();
    let mut created: i64 = 0;
    let mut content = String::new();
    let mut finish_reason: Option<Value> = None;
    let mut usage: Option<Value> = None;
    let mut completed_response: Option<Value> = None;
    let mut tool_calls_by_index = BTreeMap::<usize, AggregatedChatToolCall>::new();
    let mut data_lines = Vec::<String>::new();
    let mut event_name: Option<String> = None;
    let mut saw_text_delta = false;

    let flush_frame = |lines: &mut Vec<String>,
                       event_name: &mut Option<String>,
                       id: &mut String,
                       model: &mut String,
                       created: &mut i64,
                       content: &mut String,
                       finish_reason: &mut Option<Value>,
                       usage: &mut Option<Value>,
                       completed_response: &mut Option<Value>,
                       tool_calls_by_index: &mut BTreeMap<usize, AggregatedChatToolCall>,
                       saw_text_delta: &mut bool| {
        if lines.is_empty() {
            *event_name = None;
            return;
        }
        let data = lines.join("\n");
        lines.clear();
        if data.trim() == "[DONE]" {
            *event_name = None;
            if finish_reason.is_none() {
                *finish_reason = Some(Value::String("stop".to_string()));
            }
            return;
        }
        let Some(value) = parse_openai_sse_event_value(&data, event_name.as_deref()) else {
            *event_name = None;
            return;
        };
        *event_name = None;
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if event_type == "response.output_text.delta" {
            *saw_text_delta = true;
        }
        if matches!(
            event_type,
            "response.output_text.done"
                | "response.content_part.added"
                | "response.content_part.delta"
                | "response.content_part.done"
        ) {
            return;
        }
        if matches!(
            event_type,
            "response.output_item.added" | "response.output_item.done"
        ) {
            let is_function_call_item = value
                .get("item")
                .or_else(|| value.get("output_item"))
                .and_then(|item| item.get("type"))
                .and_then(Value::as_str)
                .is_some_and(is_openai_chat_tool_item_type);
            if !is_function_call_item && *saw_text_delta {
                return;
            }
        }
        if value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(is_response_completed_event_type)
        {
            if let Some(response) = value.get("response") {
                *completed_response = Some(response.clone());
            }
        }
        let Some(chunk) = convert_openai_chat_stream_chunk_with_tool_name_restore_map(
            &value,
            tool_name_restore_map,
        ) else {
            return;
        };
        if let Some(v) = chunk.get("id").and_then(Value::as_str) {
            if !v.is_empty() {
                *id = v.to_string();
            }
        }
        if let Some(v) = chunk.get("model").and_then(Value::as_str) {
            if !v.is_empty() {
                *model = v.to_string();
            }
        }
        if let Some(v) = chunk.get("created").and_then(Value::as_i64) {
            *created = v;
        }
        if let Some(chunk_usage) = chunk.get("usage") {
            *usage = Some(chunk_usage.clone());
        }
        if let Some(choices) = chunk.get("choices").and_then(Value::as_array) {
            for choice in choices {
                if let Some(delta) = choice.get("delta").and_then(Value::as_object) {
                    if let Some(text_piece) = delta.get("content").and_then(Value::as_str) {
                        content.push_str(text_piece);
                    }
                    collect_chat_tool_calls_from_delta(delta, tool_calls_by_index);
                }
                if let Some(reason) = choice.get("finish_reason") {
                    if !reason.is_null() {
                        *finish_reason = Some(reason.clone());
                    }
                }
            }
        }
    };

    for line in text.lines() {
        if line.starts_with("event:") {
            event_name = Some(line[6..].trim_start().to_string());
            continue;
        }
        if line.starts_with("data:") {
            data_lines.push(line[5..].trim_start().to_string());
            continue;
        }
        if line.trim().is_empty() {
            flush_frame(
                &mut data_lines,
                &mut event_name,
                &mut id,
                &mut model,
                &mut created,
                &mut content,
                &mut finish_reason,
                &mut usage,
                &mut completed_response,
                &mut tool_calls_by_index,
                &mut saw_text_delta,
            );
        }
    }
    flush_frame(
        &mut data_lines,
        &mut event_name,
        &mut id,
        &mut model,
        &mut created,
        &mut content,
        &mut finish_reason,
        &mut usage,
        &mut completed_response,
        &mut tool_calls_by_index,
        &mut saw_text_delta,
    );

    if let Some(response) = completed_response.as_ref() {
        let completion = map_openai_response_to_chat_completion(response, tool_name_restore_map);
        if let Some(v) = completion.get("id").and_then(Value::as_str) {
            if !v.is_empty() {
                id = v.to_string();
            }
        }
        if let Some(v) = completion.get("model").and_then(Value::as_str) {
            if !v.is_empty() {
                model = v.to_string();
            }
        }
        if let Some(v) = completion.get("created").and_then(Value::as_i64) {
            created = v;
        }
        if usage.is_none() {
            if let Some(v) = completion.get("usage") {
                usage = Some(v.clone());
            }
        }
        if let Some(choice) = completion
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        {
            if let Some(message) = choice.get("message") {
                if content.is_empty() {
                    content = extract_chat_content_text(message.get("content"));
                }
                if let Some(message_obj) = message.as_object() {
                    merge_tool_calls_from_completed_message_prefer_longer(
                        &mut tool_calls_by_index,
                        message_obj,
                    );
                }
            }
            if finish_reason.is_none() {
                if let Some(v) = choice.get("finish_reason") {
                    if !v.is_null() {
                        finish_reason = Some(v.clone());
                    }
                }
            }
        }
    }
    let mapped_tool_calls = build_openai_chat_tool_calls(&tool_calls_by_index);
    if !mapped_tool_calls.is_empty() {
        let should_force_tool_calls_reason = finish_reason
            .as_ref()
            .and_then(Value::as_str)
            .is_none_or(|reason| reason.eq_ignore_ascii_case("stop"));
        if should_force_tool_calls_reason {
            finish_reason = Some(Value::String("tool_calls".to_string()));
        }
    }

    let mut out = json!({
        "id": id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": content
            },
            "finish_reason": finish_reason.unwrap_or(Value::String("stop".to_string()))
        }]
    });
    if !mapped_tool_calls.is_empty() {
        out["choices"][0]["message"]["tool_calls"] = Value::Array(mapped_tool_calls);
        if out["choices"][0]["message"]["content"]
            .as_str()
            .is_some_and(|value| value.is_empty())
        {
            out["choices"][0]["message"]["content"] = Value::Null;
        }
    }
    if let Some(usage) = usage {
        if let Some(out_obj) = out.as_object_mut() {
            out_obj.insert("usage".to_string(), usage);
        }
    }

    let bytes = serde_json::to_vec(&out)
        .map_err(|err| format!("serialize chat.completion json failed: {err}"))?;
    Ok((bytes, "application/json"))
}
