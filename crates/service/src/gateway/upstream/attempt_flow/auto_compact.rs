use bytes::Bytes;
use codexmanager_core::storage::{Account, Storage};
use serde_json::Value;
use std::time::Instant;

use super::super::GatewayUpstreamResponse;
use super::transport::{send_upstream_request_without_compression, UpstreamRequestContext};

const DEFAULT_AUTO_COMPACT_TOKEN_LIMIT: usize = 170_000;
const DEFAULT_AUTO_COMPACT_BODY_BYTES_LIMIT: usize = 8 * 1024 * 1024;
const DEFAULT_AUTO_COMPACT_RAW_TAIL_TOKENS: usize = 48_000;
const DEFAULT_AUTO_COMPACT_MIN_RAW_TAIL_ITEMS: usize = 8;
const DEFAULT_AUTO_COMPACT_MIN_PREFIX_TOKENS: usize = 4_096;
const MODEL_SCOPE_DEFAULT: &str = "default";
const COMPACT_REQUEST_PATH: &str = "/v1/responses/compact";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutoCompactTrigger {
    EstimatedTokens,
    BodyBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AutoCompactPlan {
    trigger: AutoCompactTrigger,
    estimated_input_tokens: usize,
    threshold_tokens: usize,
    prefix_items: usize,
    prefix_tokens: usize,
    tail_items: usize,
    tail_tokens: usize,
}

fn accumulate_text_len(value: &Value) -> usize {
    match value {
        Value::String(text) => text.chars().count(),
        Value::Array(items) => items.iter().map(accumulate_text_len).sum(),
        Value::Object(map) => {
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                return text.chars().count();
            }
            if let Some(content) = map.get("content") {
                return accumulate_text_len(content);
            }
            if let Some(input) = map.get("input") {
                return accumulate_text_len(input);
            }
            map.values().map(accumulate_text_len).sum()
        }
        _ => 0,
    }
}

fn estimate_value_tokens(value: &Value) -> usize {
    let chars = accumulate_text_len(value);
    if chars == 0 {
        0
    } else {
        (chars / 4).max(1)
    }
}

fn estimate_responses_input_tokens(root: &serde_json::Map<String, Value>) -> usize {
    let mut total = 0usize;
    for key in ["instructions", "input", "tools", "reasoning", "text"] {
        if let Some(value) = root.get(key) {
            total = total.saturating_add(estimate_value_tokens(value));
        }
    }
    total
}

fn resolve_model_auto_compact_limit(
    storage: &Storage,
    root: &serde_json::Map<String, Value>,
) -> usize {
    let default_limit = DEFAULT_AUTO_COMPACT_TOKEN_LIMIT;
    let Some(model) = root.get("model").and_then(Value::as_str).map(str::trim) else {
        return default_limit;
    };
    if model.is_empty() {
        return default_limit;
    }
    storage
        .list_model_catalog_models(MODEL_SCOPE_DEFAULT)
        .ok()
        .and_then(|rows| {
            rows.into_iter()
                .find(|row| row.slug.eq_ignore_ascii_case(model))
                .and_then(|row| row.auto_compact_token_limit)
        })
        .and_then(|limit| usize::try_from(limit).ok())
        .filter(|limit| *limit > 0)
        .unwrap_or(default_limit)
}

fn build_auto_compact_plan(
    root: &serde_json::Map<String, Value>,
    body_len: usize,
    threshold_tokens: usize,
) -> Option<AutoCompactPlan> {
    let input_items = root.get("input")?.as_array()?;
    if input_items.len() <= 1 {
        return None;
    }

    let estimated_input_tokens = estimate_responses_input_tokens(root);
    let body_limit_hit = body_len >= DEFAULT_AUTO_COMPACT_BODY_BYTES_LIMIT;
    let token_limit_hit = estimated_input_tokens > threshold_tokens;
    if !body_limit_hit && !token_limit_hit {
        return None;
    }

    let min_raw_tail_items = DEFAULT_AUTO_COMPACT_MIN_RAW_TAIL_ITEMS.min(input_items.len() - 1);
    let raw_tail_budget = DEFAULT_AUTO_COMPACT_RAW_TAIL_TOKENS.min(threshold_tokens / 3);
    let mut tail_tokens = 0usize;
    let mut tail_items = 0usize;
    let mut tail_start = input_items.len();

    while tail_start > 0 {
        let item_tokens = estimate_value_tokens(&input_items[tail_start - 1]);
        let next_tail_tokens = tail_tokens.saturating_add(item_tokens);
        if tail_items >= min_raw_tail_items && next_tail_tokens > raw_tail_budget.max(1) {
            break;
        }
        tail_start -= 1;
        tail_items += 1;
        tail_tokens = next_tail_tokens;
    }

    if tail_start == 0 {
        return None;
    }

    let prefix_tokens = input_items[..tail_start]
        .iter()
        .map(estimate_value_tokens)
        .sum::<usize>();
    if prefix_tokens < DEFAULT_AUTO_COMPACT_MIN_PREFIX_TOKENS && !body_limit_hit {
        return None;
    }

    Some(AutoCompactPlan {
        trigger: if token_limit_hit {
            AutoCompactTrigger::EstimatedTokens
        } else {
            AutoCompactTrigger::BodyBytes
        },
        estimated_input_tokens,
        threshold_tokens,
        prefix_items: tail_start,
        prefix_tokens,
        tail_items,
        tail_tokens,
    })
}

fn build_compact_request_root(
    root: &serde_json::Map<String, Value>,
    prefix_input: Vec<Value>,
) -> serde_json::Map<String, Value> {
    let mut compact_root = serde_json::Map::new();
    for key in [
        "model",
        "instructions",
        "tools",
        "parallel_tool_calls",
        "reasoning",
        "text",
    ] {
        if let Some(value) = root.get(key) {
            compact_root.insert(key.to_string(), value.clone());
        }
    }
    compact_root.insert("input".to_string(), Value::Array(prefix_input));
    compact_root
        .entry("instructions".to_string())
        .or_insert_with(|| Value::String(String::new()));
    compact_root
        .entry("parallel_tool_calls".to_string())
        .or_insert(Value::Bool(false));
    compact_root
        .entry("tools".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    compact_root
}

fn read_upstream_body_bytes(response: GatewayUpstreamResponse) -> Result<Bytes, String> {
    match response {
        GatewayUpstreamResponse::Blocking(response) => {
            response.bytes().map_err(|err| err.to_string())
        }
        GatewayUpstreamResponse::Stream(response) => response.read_all_bytes(),
    }
}

fn extract_compact_output_items(body: &[u8]) -> Result<Vec<Value>, String> {
    let root: Value = serde_json::from_slice(body).map_err(|err| err.to_string())?;
    let output = root
        .get("output")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| "compact response missing output array".to_string())?;
    if output.is_empty() {
        return Err("compact response returned empty output".to_string());
    }
    Ok(output)
}

fn build_compacted_body(
    root: &serde_json::Map<String, Value>,
    compact_output: Vec<Value>,
    tail_start: usize,
) -> Result<Bytes, String> {
    let tail_items = root
        .get("input")
        .and_then(Value::as_array)
        .ok_or_else(|| "responses request missing input array".to_string())?;
    let mut next_input = compact_output;
    next_input.extend(tail_items[tail_start..].iter().cloned());

    let mut next_root = root.clone();
    next_root.insert("input".to_string(), Value::Array(next_input));
    serde_json::to_vec(&Value::Object(next_root))
        .map(Bytes::from)
        .map_err(|err| err.to_string())
}

pub(super) fn maybe_prepare_auto_compacted_body(
    client: &reqwest::blocking::Client,
    storage: &Storage,
    method: &reqwest::Method,
    request_deadline: Option<Instant>,
    incoming_headers: &super::super::super::IncomingHeaderSnapshot,
    body: &Bytes,
    base: &str,
    account: &Account,
    auth_token: &str,
    strip_session_affinity: bool,
    debug: bool,
) -> Bytes {
    if method != reqwest::Method::POST
        || !super::super::config::should_send_chatgpt_account_header(base)
    {
        return body.clone();
    }

    let Ok(Value::Object(root)) = serde_json::from_slice::<Value>(body.as_ref()) else {
        return body.clone();
    };
    let threshold_tokens = resolve_model_auto_compact_limit(storage, &root);
    let Some(plan) = build_auto_compact_plan(&root, body.len(), threshold_tokens) else {
        return body.clone();
    };

    let compact_url = super::super::super::compute_upstream_url(base, COMPACT_REQUEST_PATH).0;
    let prefix_input = root
        .get("input")
        .and_then(Value::as_array)
        .map(|items| items[..plan.prefix_items].to_vec());
    let Some(prefix_input) = prefix_input else {
        return body.clone();
    };

    let compact_root = build_compact_request_root(&root, prefix_input);
    let compact_body = match serde_json::to_vec(&Value::Object(compact_root)) {
        Ok(bytes) => Bytes::from(bytes),
        Err(err) => {
            log::warn!(
                "event=gateway_auto_compact_serialize_failed account_id={} err={}",
                account.id,
                err
            );
            return body.clone();
        }
    };

    log::info!(
        "event=gateway_auto_compact_attempt account_id={} model={} trigger={} estimated_tokens={} threshold_tokens={} original_body_len={} compact_prefix_items={} compact_tail_items={} compact_prefix_tokens={} compact_tail_tokens={}",
        account.id,
        root.get("model").and_then(Value::as_str).unwrap_or("-"),
        match plan.trigger {
            AutoCompactTrigger::EstimatedTokens => "estimated_tokens",
            AutoCompactTrigger::BodyBytes => "body_bytes",
        },
        plan.estimated_input_tokens,
        plan.threshold_tokens,
        body.len(),
        plan.prefix_items,
        plan.tail_items,
        plan.prefix_tokens,
        plan.tail_tokens,
    );

    let response = match send_upstream_request_without_compression(
        client,
        method,
        compact_url.as_str(),
        request_deadline,
        UpstreamRequestContext {
            request_path: COMPACT_REQUEST_PATH,
        },
        incoming_headers,
        &compact_body,
        false,
        auth_token,
        account,
        strip_session_affinity,
    ) {
        Ok(response) => response,
        Err(err) => {
            log::warn!(
                "event=gateway_auto_compact_transport_failed account_id={} model={} err={}",
                account.id,
                root.get("model").and_then(Value::as_str).unwrap_or("-"),
                err
            );
            return body.clone();
        }
    };

    if !response.status().is_success() {
        log::warn!(
            "event=gateway_auto_compact_non_success account_id={} model={} status={}",
            account.id,
            root.get("model").and_then(Value::as_str).unwrap_or("-"),
            response.status().as_u16(),
        );
        return body.clone();
    }

    let response_body = match read_upstream_body_bytes(response) {
        Ok(bytes) => bytes,
        Err(err) => {
            log::warn!(
                "event=gateway_auto_compact_read_failed account_id={} model={} err={}",
                account.id,
                root.get("model").and_then(Value::as_str).unwrap_or("-"),
                err
            );
            return body.clone();
        }
    };

    let compact_output = match extract_compact_output_items(response_body.as_ref()) {
        Ok(output) => output,
        Err(err) => {
            log::warn!(
                "event=gateway_auto_compact_parse_failed account_id={} model={} err={}",
                account.id,
                root.get("model").and_then(Value::as_str).unwrap_or("-"),
                err
            );
            return body.clone();
        }
    };

    match build_compacted_body(&root, compact_output, plan.prefix_items) {
        Ok(compacted) => {
            if debug {
                log::debug!(
                    "event=gateway_auto_compact_applied account_id={} model={} compacted_body_len={} original_body_len={}",
                    account.id,
                    root.get("model").and_then(Value::as_str).unwrap_or("-"),
                    compacted.len(),
                    body.len(),
                );
            }
            compacted
        }
        Err(err) => {
            log::warn!(
                "event=gateway_auto_compact_merge_failed account_id={} model={} err={}",
                account.id,
                root.get("model").and_then(Value::as_str).unwrap_or("-"),
                err
            );
            body.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_auto_compact_plan, build_compact_request_root, build_compacted_body,
        estimate_responses_input_tokens, extract_compact_output_items, AutoCompactTrigger,
        DEFAULT_AUTO_COMPACT_TOKEN_LIMIT,
    };
    use serde_json::{json, Value};

    fn build_large_input_items(count: usize, chars_per_item: usize) -> Vec<Value> {
        (0..count)
            .map(|idx| {
                json!({
                    "type": "message",
                    "role": if idx % 2 == 0 { "user" } else { "assistant" },
                    "content": [{
                        "type": "input_text",
                        "text": "x".repeat(chars_per_item),
                    }]
                })
            })
            .collect()
    }

    #[test]
    fn responses_auto_compact_plan_triggers_on_estimated_tokens() {
        let root = json!({
            "model": "gpt-5.4",
            "instructions": "follow the repo rules",
            "input": build_large_input_items(12, 60_000),
            "stream": true,
        });
        let root = root.as_object().cloned().expect("root object");
        let estimated_tokens = estimate_responses_input_tokens(&root);
        assert!(estimated_tokens > DEFAULT_AUTO_COMPACT_TOKEN_LIMIT);

        let plan = build_auto_compact_plan(&root, 900_000, DEFAULT_AUTO_COMPACT_TOKEN_LIMIT)
            .expect("plan should be created");
        assert_eq!(plan.trigger, AutoCompactTrigger::EstimatedTokens);
        assert!(plan.prefix_items >= 1);
        assert!(plan.tail_items >= 1);
        assert!(plan.prefix_tokens > 0);
    }

    #[test]
    fn responses_auto_compact_payload_strips_non_compact_fields() {
        let root = json!({
            "model": "gpt-5.4",
            "instructions": "keep context tight",
            "input": build_large_input_items(3, 1200),
            "tools": [],
            "parallel_tool_calls": true,
            "reasoning": { "effort": "high" },
            "text": { "verbosity": "low" },
            "stream": true,
            "store": false,
            "service_tier": "priority",
            "include": ["reasoning.encrypted_content"],
            "prompt_cache_key": "thread-1",
            "client_metadata": { "a": 1 },
            "tool_choice": "auto"
        });
        let root = root.as_object().cloned().expect("root object");
        let compact_root = build_compact_request_root(
            &root,
            root.get("input")
                .and_then(Value::as_array)
                .expect("input array")[..2]
                .to_vec(),
        );

        assert!(compact_root.get("input").is_some());
        assert!(compact_root.get("stream").is_none());
        assert!(compact_root.get("store").is_none());
        assert!(compact_root.get("service_tier").is_none());
        assert!(compact_root.get("include").is_none());
        assert!(compact_root.get("prompt_cache_key").is_none());
        assert!(compact_root.get("client_metadata").is_none());
        assert!(compact_root.get("tool_choice").is_none());
        assert_eq!(
            compact_root
                .get("parallel_tool_calls")
                .and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn responses_auto_compact_merges_remote_output_back_into_input() {
        let root = json!({
            "model": "gpt-5.4",
            "input": build_large_input_items(4, 800),
            "stream": true,
        });
        let root = root.as_object().cloned().expect("root object");
        let response_body = json!({
            "output": [
                {
                    "type": "compaction",
                    "encrypted_content": "REMOTE_COMPACTED_SUMMARY"
                }
            ]
        });
        let compact_output = extract_compact_output_items(
            serde_json::to_string(&response_body)
                .expect("serialize response")
                .as_bytes(),
        )
        .expect("extract output");
        let compacted =
            build_compacted_body(&root, compact_output, 2).expect("build compacted body");
        let value: Value = serde_json::from_slice(compacted.as_ref()).expect("parse compacted");
        let input = value["input"].as_array().expect("input array");
        assert_eq!(input.len(), 3);
        assert_eq!(input[0]["type"], "compaction");
        assert_eq!(input[0]["encrypted_content"], "REMOTE_COMPACTED_SUMMARY");
    }
}
