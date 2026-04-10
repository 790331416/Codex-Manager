# Nginx Reverse Proxy Cache Hit Issue

## Conclusion

If CodexManager is deployed behind Docker or the standalone service and then placed behind a default Nginx reverse proxy, the hidden failure is usually not "the model cache is broken". The real issue is that **Nginx drops custom headers that contain underscores**.

CodexManager depends on these headers to keep a stable thread anchor:

- `conversation_id`
- `session_id`
- `x-client-request-id`
- `x-codex-turn-metadata`
- `x-codex-turn-state`

Once those headers are lost in the proxy layer, the backend can no longer keep a stable thread anchor. The visible symptom is lower `cached_input_tokens` on the deployed web version than on the desktop app.

## Why this happens

By default, Nginx is unfriendly to headers with underscores.

If you do not explicitly enable:

```nginx
underscores_in_headers on;
ignore_invalid_headers off;
```

headers such as `conversation_id` and `session_id` may be treated as invalid and dropped before they reach CodexManager.

That directly affects these paths:

1. `crates/service/src/gateway/request/incoming_headers.rs`
   extracts `conversation_id`, `session_id`, `x-codex-turn-state`, and related headers.
2. `crates/service/src/gateway/request/session_affinity.rs`
   computes the effective thread anchor.
3. `crates/service/src/gateway/request/request_rewrite_responses.rs`
   rewrites the request body with a stable `prompt_cache_key`.

If the proxy strips those headers, the backend falls back to a weaker session anchor and prompt cache reuse becomes unstable.

## Typical symptoms

- The deployed web UI shows only a few thousand `cached_input_tokens`
- The local desktop app reaches tens of thousands on similar requests
- Repeated requests in the same conversation do not show strong cache growth
- The model, account, and prompt are similar, but the deployed result is far worse than desktop

Notes:

- A low cache count on the first request is normal
- Different models, accounts, and conversations do not produce identical cache values
- The real warning sign is a persistent gap across multiple requests in the same conversation

## Correct fix

### 1. Allow underscore headers

Add this inside `http {}`:

```nginx
underscores_in_headers on;
ignore_invalid_headers off;
```

### 2. Explicitly forward session-related headers

Inside the API reverse proxy `location`, add:

```nginx
proxy_set_header conversation_id $http_conversation_id;
proxy_set_header session_id $http_session_id;
proxy_set_header x-client-request-id $http_x_client_request_id;
proxy_set_header x-openai-subagent $http_x_openai_subagent;
proxy_set_header x-codex-beta-features $http_x_codex_beta_features;
proxy_set_header x-codex-turn-metadata $http_x_codex_turn_metadata;
proxy_set_header x-codex-turn-state $http_x_codex_turn_state;
```

This should be treated as a deployment baseline, not as an optional tweak.

### 3. Disable buffering for streaming API traffic

Recommended:

```nginx
proxy_buffering off;
proxy_request_buffering off;
proxy_read_timeout 3600s;
proxy_send_timeout 3600s;
```

This does not create cache hits by itself, but it reduces proxy-side interference for Responses, SSE, and WebSocket traffic.

## Recommended example config

See:

- [`docker/nginx/nginx.conf`](../../../docker/nginx/nginx.conf)

The sample covers:

- `manager.example.com -> codexmanager-service:45760`
- `web.example.com -> codexmanager-web:45761`
- HTTPS redirect
- underscore header support
- explicit session header forwarding
- streaming-friendly API proxy settings

## How to verify after deployment

Validate at least one critical path:

1. Use the same client, same account, and same conversation against the deployed domain.
2. Send 3 to 5 consecutive requests.
3. Check whether `cached_input_tokens` rises clearly in the newest `/v1/responses` logs.
4. Compare with the desktop app using the same account, model, and prompt.

If the fix works:

- the first request may still be low
- the second and later requests should show stronger cache reuse
- the deployed result should move much closer to the desktop behavior

## Common misdiagnoses

### Misdiagnosis 1: Docker broke the cache

Usually false. Docker is only the runtime container. The actual regression is commonly introduced by the reverse proxy in front of it.

### Misdiagnosis 2: Cloudflare changed the request

Cloudflare can complicate the request path, but the most common direct cause is still Nginx dropping underscore headers by default.

### Misdiagnosis 3: Low cache numbers mean the backend logic is wrong

Not necessarily. CodexManager logs what the upstream usage reports. If the thread anchor is already lost before the request reaches the service, the backend can only log the degraded cache hit result.

## Troubleshooting checklist

1. Confirm `underscores_in_headers on;` exists in `http {}`
2. Confirm `ignore_invalid_headers off;` exists in `http {}`
3. Confirm the API proxy block forwards `conversation_id` and `session_id`
4. Check whether a second proxy layer or hosting panel rewrites headers
5. Verify the comparison really uses the same account, model, and conversation
6. Make sure you are not comparing a first request with later repeated requests

## Source references

- [`crates/service/src/gateway/request/incoming_headers.rs`](../../../crates/service/src/gateway/request/incoming_headers.rs)
- [`crates/service/src/gateway/request/session_affinity.rs`](../../../crates/service/src/gateway/request/session_affinity.rs)
- [`crates/service/src/gateway/request/request_rewrite_responses.rs`](../../../crates/service/src/gateway/request/request_rewrite_responses.rs)
- [`crates/service/src/gateway/observability/http_bridge/aggregate/output_text.rs`](../../../crates/service/src/gateway/observability/http_bridge/aggregate/output_text.rs)
- [`crates/service/src/gateway/protocol_adapter/response_conversion/sse_conversion/openai_sse_anthropic_bridge.rs`](../../../crates/service/src/gateway/protocol_adapter/response_conversion/sse_conversion/openai_sse_anthropic_bridge.rs)
