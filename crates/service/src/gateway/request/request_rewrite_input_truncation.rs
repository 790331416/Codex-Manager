use serde_json::Value;

/// 中文注释：当 /v1/responses 请求体超过配置阈值时，从 input 数组头部裁剪旧消息，
/// 保留末尾最近的消息，避免上游 API 返回 413 Payload Too Large。
/// 裁剪后在 input[0] 插入一条汇总消息说明被截断的消息数量。

const MIN_KEEP_MESSAGES: usize = 2;

fn is_responses_path(path: &str) -> bool {
    path == "/v1/responses"
        || path.starts_with("/v1/responses?")
        || path.starts_with("/v1/responses/")
}

fn build_truncation_notice(removed_count: usize) -> Value {
    let text = format!(
        "[earlier conversation truncated by CodexManager — {} message{} removed to fit size limit]",
        removed_count,
        if removed_count == 1 { "" } else { "s" }
    );
    let mut content_part = serde_json::Map::new();
    content_part.insert("type".to_string(), Value::String("input_text".to_string()));
    content_part.insert("text".to_string(), Value::String(text));

    let mut message_item = serde_json::Map::new();
    message_item.insert("type".to_string(), Value::String("message".to_string()));
    message_item.insert("role".to_string(), Value::String("user".to_string()));
    message_item.insert(
        "content".to_string(),
        Value::Array(vec![Value::Object(content_part)]),
    );
    Value::Object(message_item)
}

/// 公开入口：从运行时配置读取阈值。
pub(super) fn maybe_truncate_input(path: &str, body: Vec<u8>) -> Vec<u8> {
    let threshold = crate::gateway::input_truncation_threshold_bytes();
    truncate_input_with_threshold(path, body, threshold)
}

/// 内部实现：接受显式阈值参数，方便单元测试。
fn truncate_input_with_threshold(path: &str, body: Vec<u8>, threshold: usize) -> Vec<u8> {
    if threshold == 0 || body.len() <= threshold {
        return body;
    }
    if !is_responses_path(path) {
        return body;
    }

    let mut payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return body,
    };

    // 中文注释：先检查 input 是否存在且为数组，且长度 > 保底值。
    {
        let has_valid_input = payload
            .as_object()
            .and_then(|obj| obj.get("input"))
            .and_then(Value::as_array)
            .is_some_and(|arr| arr.len() > MIN_KEEP_MESSAGES);
        if !has_valid_input {
            return body;
        }
    }

    let original_len = body.len();
    let mut removed_count: usize = 0;

    // 中文注释：逐条从头部移除最旧的消息，直到 body 大小 <= 阈值或只剩保底数量。
    loop {
        let input_len = payload["input"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);

        if removed_count == 0 {
            // 还没有移除过，直接看 input 长度
            if input_len <= MIN_KEEP_MESSAGES {
                break;
            }
        } else {
            // 已有汇总消息在 index 0，实际消息从 index 1 开始
            // 保底：汇总消息 + MIN_KEEP_MESSAGES 条实际消息
            if input_len <= MIN_KEEP_MESSAGES + 1 {
                break;
            }
        }

        // 移除最旧的消息
        if let Some(arr) = payload.get_mut("input").and_then(Value::as_array_mut) {
            if removed_count > 0 {
                // 汇总消息在 index 0，移除 index 1（最旧的实际消息）
                arr.remove(1);
            } else {
                // 第一次移除 index 0
                arr.remove(0);
            }
        }
        removed_count += 1;

        // 插入/更新汇总消息在 index 0
        let notice = build_truncation_notice(removed_count);
        if let Some(arr) = payload.get_mut("input").and_then(Value::as_array_mut) {
            if removed_count == 1 {
                arr.insert(0, notice);
            } else {
                arr[0] = notice;
            }
        }

        // 检查大小 — 这里 payload 没有被 mutably borrowed，可以安全序列化
        let serialized = match serde_json::to_vec(&payload) {
            Ok(s) => s,
            Err(_) => break,
        };
        if serialized.len() <= threshold {
            log::info!(
                "event=gateway_input_truncated path={} removed_messages={} original_bytes={} final_bytes={}",
                path,
                removed_count,
                original_len,
                serialized.len()
            );
            return serialized;
        }
    }

    // 中文注释：即使裁剪到保底也没降到阈值以下，仍返回裁剪后的结果（至少减小了一些）。
    let final_body = serde_json::to_vec(&payload).unwrap_or(body);
    if removed_count > 0 {
        log::warn!(
            "event=gateway_input_truncated_partial path={} removed_messages={} original_bytes={} final_bytes={} still_over_threshold=true",
            path,
            removed_count,
            original_len,
            final_body.len()
        );
    }
    final_body
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_message(role: &str, text: &str) -> Value {
        json!({
            "type": "message",
            "role": role,
            "content": [{
                "type": "input_text",
                "text": text
            }]
        })
    }

    fn make_responses_body(messages: Vec<Value>) -> Vec<u8> {
        let body = json!({
            "model": "o4-mini",
            "instructions": "You are a helpful assistant.",
            "input": messages,
            "stream": true
        });
        serde_json::to_vec(&body).unwrap()
    }

    #[test]
    fn body_under_threshold_is_not_modified() {
        let messages = vec![
            make_message("user", "hello"),
            make_message("assistant", "hi there"),
        ];
        let body = make_responses_body(messages);
        // 阈值大于 body → 不会触发裁剪
        let result = truncate_input_with_threshold("/v1/responses", body.clone(), 10_485_760);
        assert_eq!(result.len(), body.len());
    }

    #[test]
    fn non_responses_path_is_not_modified() {
        let messages = vec![make_message("user", "hello")];
        let body = make_responses_body(messages);
        // 阈值 1 字节但路径不匹配 → 不会触发裁剪
        let result = truncate_input_with_threshold("/v1/chat/completions", body.clone(), 1);
        assert_eq!(result.len(), body.len());
    }

    #[test]
    fn truncates_old_messages_when_over_threshold() {
        // 生成多条消息构造大 body
        let big_text = "x".repeat(1000);
        let mut messages = Vec::new();
        for i in 0..20 {
            let role = if i % 2 == 0 { "user" } else { "assistant" };
            messages.push(make_message(role, &format!("msg-{}: {}", i, big_text)));
        }
        let body = make_responses_body(messages);
        let original_len = body.len();

        // 阈值设成原始大小的一半
        let threshold = original_len / 2;
        let result = truncate_input_with_threshold("/v1/responses", body, threshold);
        assert!(
            result.len() <= threshold,
            "Expected {} <= {}",
            result.len(),
            threshold
        );

        // 验证汇总消息存在
        let parsed: Value = serde_json::from_slice(&result).unwrap();
        let input = parsed["input"].as_array().unwrap();
        let first_msg_text = input[0]["content"][0]["text"].as_str().unwrap();
        assert!(first_msg_text.contains("truncated by CodexManager"));
        assert!(first_msg_text.contains("removed to fit size limit"));
    }

    #[test]
    fn does_not_truncate_below_min_keep() {
        let messages = vec![
            make_message("user", "hello"),
            make_message("assistant", "hi"),
        ];
        let body = make_responses_body(messages);
        // 阈值极小，但只有 2 条消息 → 不裁剪（保底）
        let result = truncate_input_with_threshold("/v1/responses", body.clone(), 1);
        assert_eq!(result.len(), body.len());
    }

    #[test]
    fn threshold_zero_disables_truncation() {
        let messages = vec![make_message("user", &"x".repeat(10000))];
        let body = make_responses_body(messages);
        let result = truncate_input_with_threshold("/v1/responses", body.clone(), 0);
        assert_eq!(result.len(), body.len());
    }

    #[test]
    fn preserves_min_keep_plus_notice_when_heavily_truncated() {
        let big_text = "y".repeat(5000);
        let mut messages = Vec::new();
        for i in 0..10 {
            let role = if i % 2 == 0 { "user" } else { "assistant" };
            messages.push(make_message(role, &format!("msg-{}: {}", i, big_text)));
        }
        let body = make_responses_body(messages);

        // 阈值极小，强制裁剪到保底
        let result = truncate_input_with_threshold("/v1/responses", body, 500);
        let parsed: Value = serde_json::from_slice(&result).unwrap();
        let input = parsed["input"].as_array().unwrap();

        // 应该有 MIN_KEEP_MESSAGES + 1 (notice) = 3 条
        assert_eq!(input.len(), MIN_KEEP_MESSAGES + 1);

        // 第一条是汇总消息
        let notice_text = input[0]["content"][0]["text"].as_str().unwrap();
        assert!(notice_text.contains("truncated by CodexManager"));

        // 最后两条是原始的最后两条消息
        let last = input[input.len() - 1]["content"][0]["text"].as_str().unwrap();
        assert!(last.starts_with("msg-9:"));
    }
}
