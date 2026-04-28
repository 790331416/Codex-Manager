#!/usr/bin/env python3
"""
Local reverse proxy for ChatGPT Codex upstream using curl_cffi browser impersonation.

Why this exists:
- CodexManager currently talks to the upstream with reqwest/rustls.
- On some cloud hosts, Cloudflare still challenges that client stack even when a SOCKS proxy is used.
- curl_cffi can impersonate a real browser TLS/HTTP fingerprint and often passes where reqwest does not.

Typical usage:
  python3 scripts/curl_cffi_chatgpt_proxy.py \
    --listen 127.0.0.1:8787 \
    --proxy socks5h://127.0.0.1:40000

Then point CodexManager to:
  CODEXMANAGER_UPSTREAM_BASE_URL=http://127.0.0.1:8787/backend-api/codex
"""

from __future__ import annotations

import argparse
import codecs
import copy
import json
import os
import sys
import time
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterable, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from curl_cffi import requests


DEFAULT_LISTEN = "127.0.0.1:8787"
DEFAULT_UPSTREAM_ORIGIN = "https://chatgpt.com"
DEFAULT_IMPERSONATE = "chrome124"
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
SKIP_REQUEST_HEADERS = HOP_BY_HOP_HEADERS | {
    "host",
    "content-length",
}
SKIP_RESPONSE_HEADERS = HOP_BY_HOP_HEADERS | {
    "content-length",
    "content-encoding",
    "server",
    "date",
}
ERROR_BODY_LOG_LIMIT = 4096
REQUEST_BODY_LOG_LIMIT = 4096
DEBUG_HEADER_ALLOWLIST = {
    "accept",
    "content-type",
    "authorization",
    "chatgpt-account-id",
    "openai-account-id",
    "origin",
    "referer",
    "user-agent",
    "x-requested-with",
}
RESPONSES_TERMINAL_EVENT_TYPES = {
    "response.completed",
    "response.done",
    "response.failed",
    "response.incomplete",
}
SSE_TRACK_BUFFER_LIMIT = 64 * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Local curl_cffi-based reverse proxy for ChatGPT Codex upstream."
    )
    parser.add_argument(
        "--listen",
        default=os.environ.get("CURL_CFFI_PROXY_LISTEN", DEFAULT_LISTEN),
        help="listen host:port (default: %(default)s)",
    )
    parser.add_argument(
        "--upstream-origin",
        default=os.environ.get(
            "CURL_CFFI_PROXY_UPSTREAM_ORIGIN", DEFAULT_UPSTREAM_ORIGIN
        ),
        help="target origin, path is preserved from the incoming request (default: %(default)s)",
    )
    parser.add_argument(
        "--proxy",
        default=os.environ.get("CURL_CFFI_PROXY_URL", ""),
        help="optional outgoing proxy for curl_cffi, e.g. socks5h://127.0.0.1:40000",
    )
    parser.add_argument(
        "--impersonate",
        default=os.environ.get("CURL_CFFI_PROXY_IMPERSONATE", DEFAULT_IMPERSONATE),
        help="curl_cffi browser fingerprint profile (default: %(default)s)",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=float(os.environ.get("CURL_CFFI_PROXY_CONNECT_TIMEOUT", "15")),
        help="upstream connect timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--read-timeout",
        type=float,
        default=float(os.environ.get("CURL_CFFI_PROXY_READ_TIMEOUT", "600")),
        help="upstream read timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=os.environ.get("CURL_CFFI_PROXY_VERBOSE", "").lower() in {"1", "true", "yes"},
        help="print request/response details to stderr",
    )
    return parser.parse_args()


def split_host_port(value: str) -> Tuple[str, int]:
    if ":" not in value:
        raise ValueError(f"invalid listen address: {value!r}, expected host:port")
    host, raw_port = value.rsplit(":", 1)
    return host, int(raw_port)


def normalize_origin(value: str) -> str:
    raw = value.strip().rstrip("/")
    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            f"invalid upstream origin: {value!r}, expected http(s)://host[:port]"
        )
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def target_url(origin: str, request_target: str) -> str:
    parts = urlsplit(request_target)
    path = parts.path or "/"
    return urlunsplit((urlsplit(origin).scheme, urlsplit(origin).netloc, path, parts.query, ""))


def build_request_headers(handler: BaseHTTPRequestHandler) -> Dict[str, str]:
    outgoing: Dict[str, str] = {}
    for key, value in handler.headers.items():
        lowered = key.lower()
        if lowered in SKIP_REQUEST_HEADERS:
            continue
        outgoing[key] = value
    if not any(key.lower() == "accept-encoding" for key in outgoing):
        outgoing["Accept-Encoding"] = "identity"
    return outgoing


def response_headers_for_client(
    response: requests.Response,
) -> Iterable[Tuple[str, str]]:
    for key, value in response.headers.items():
        if key.lower() in SKIP_RESPONSE_HEADERS:
            continue
        yield key, value


def redact_header_value(name: str, value: str) -> str:
    lowered = name.lower()
    if lowered == "authorization":
        prefix, _, suffix = value.partition(" ")
        if suffix:
            return f"{prefix} ***"
        return "***"
    return value


def debug_request_headers(headers: Dict[str, str]) -> str:
    pairs = []
    for key in sorted(headers):
        if key.lower() not in DEBUG_HEADER_ALLOWLIST:
            continue
        pairs.append(f"{key}={redact_header_value(key, headers[key])}")
    return ", ".join(pairs) if pairs else "-"


def body_preview(body: bytes, limit: int) -> str:
    if not body:
        return "<empty>"
    preview = body[:limit]
    return preview.decode("utf-8", errors="replace")


def is_responses_sse_stream(
    request_target: str, response: requests.Response
) -> bool:
    path = urlsplit(request_target).path.lower()
    content_type = response.headers.get("Content-Type", "").lower()
    return "text/event-stream" in content_type and "/responses" in path


class ResponsesSseTerminalTracker:
    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="ignore")
        self._buffer = ""
        self._last_response: Optional[dict] = None
        self.saw_terminal = False

    def feed(self, chunk: bytes) -> None:
        if self.saw_terminal or not chunk:
            return
        self._append_text(self._decoder.decode(chunk))

    def finish(self) -> None:
        if self.saw_terminal:
            return
        self._append_text(self._decoder.decode(b"", final=True))
        trailing = self._buffer.strip()
        if trailing:
            self._process_frame(trailing)
        self._buffer = ""

    def _append_text(self, text: str) -> None:
        if not text:
            return
        self._buffer += text.replace("\r\n", "\n").replace("\r", "\n")
        while "\n\n" in self._buffer:
            frame, self._buffer = self._buffer.split("\n\n", 1)
            self._process_frame(frame)
            if self.saw_terminal:
                self._buffer = ""
                return
        if len(self._buffer) > SSE_TRACK_BUFFER_LIMIT:
            self._buffer = self._buffer[-SSE_TRACK_BUFFER_LIMIT:]

    def _process_frame(self, frame: str) -> None:
        stripped = frame.strip()
        if not stripped:
            return
        event_name = ""
        data_lines = []
        for raw_line in stripped.split("\n"):
            line = raw_line.strip()
            if not line or line.startswith(":"):
                continue
            lowered = line.lower()
            if lowered.startswith("event:"):
                event_name = line[6:].strip().lower()
            elif lowered.startswith("data:"):
                data_lines.append(line[5:].lstrip())
        if event_name in RESPONSES_TERMINAL_EVENT_TYPES:
            self.saw_terminal = True
            return
        data = "\n".join(data_lines).strip()
        if not data:
            return
        if data == "[DONE]":
            self.saw_terminal = True
            return
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            return
        response_obj = payload.get("response")
        if isinstance(response_obj, dict):
            self._last_response = copy.deepcopy(response_obj)
        event_type = str(payload.get("type", "")).strip().lower()
        if event_type in RESPONSES_TERMINAL_EVENT_TYPES:
            self.saw_terminal = True

    def build_incomplete_terminal_frames(self, detail: str) -> bytes:
        message = detail.strip() or "stream closed before response.completed"
        response = copy.deepcopy(self._last_response) if self._last_response else {}
        if not isinstance(response, dict):
            response = {}
        response.setdefault("id", "resp_proxy_incomplete")
        response.setdefault("object", "response")
        response.setdefault("created_at", int(time.time()))
        response["status"] = "incomplete"
        response.pop("completed_at", None)
        response["error"] = {
            "code": "stream_interrupted",
            "message": message,
        }
        response["incomplete_details"] = {"reason": "error"}
        response.setdefault("output", [])

        incomplete_payload = {
            "type": "response.incomplete",
            "response": response,
        }
        # Some reconnecting clients keep waiting for `response.completed` even after an
        # incomplete terminal. Emit a compatibility terminal frame with the same
        # incomplete response body so they can stop retrying while still seeing
        # `response.status = incomplete`.
        completed_payload = {
            "type": "response.completed",
            "response": response,
        }
        return (
            "event: response.incomplete\n"
            f"data: {json.dumps(incomplete_payload, ensure_ascii=False, separators=(',', ':'))}\n\n"
            "event: response.completed\n"
            f"data: {json.dumps(completed_payload, ensure_ascii=False, separators=(',', ':'))}\n\n"
        ).encode("utf-8")


def maybe_emit_responses_incomplete_event(
    handler: BaseHTTPRequestHandler,
    tracker: Optional[ResponsesSseTerminalTracker],
    detail: str,
    verbose: bool,
) -> None:
    if tracker is None:
        return
    tracker.finish()
    if tracker.saw_terminal:
        return
    tracker.saw_terminal = True
    if verbose:
        sys.stderr.write(
            f"[proxy] synthesize response.incomplete detail={detail.strip() or '-'}\n"
        )
    payload = tracker.build_incomplete_terminal_frames(detail)
    handler.wfile.write(payload)
    handler.wfile.flush()


class ProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: Tuple[str, int], config: argparse.Namespace):
        super().__init__(server_address, ProxyHandler)
        self.config = config


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "curl-cffi-chatgpt-proxy"
    sys_version = ""

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_PUT(self) -> None:
        self._handle()

    def do_PATCH(self) -> None:
        self._handle()

    def do_DELETE(self) -> None:
        self._handle()

    def do_OPTIONS(self) -> None:
        self._handle()

    def do_HEAD(self) -> None:
        self._handle()

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt % args))

    def _health_response(self) -> None:
        payload = {
            "ok": True,
            "listen": self.server.server_address,
            "upstreamOrigin": self.server.config.upstream_origin,
            "proxy": self.server.config.proxy or None,
            "impersonate": self.server.config.impersonate,
        }
        body = json.dumps(payload).encode("utf-8")
        self.close_connection = True
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _handle(self) -> None:
        response_started = False
        responses_tracker: Optional[ResponsesSseTerminalTracker] = None
        try:
            if self.path == "/__proxy_health":
                self._health_response()
                return

            content_length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(content_length) if content_length > 0 else b""
            cfg = self.server.config
            url = target_url(cfg.upstream_origin, self.path)
            headers = build_request_headers(self)
            proxies = None
            if cfg.proxy:
                proxies = {"http": cfg.proxy, "https": cfg.proxy}

            if cfg.verbose:
                sys.stderr.write(
                    f"[proxy] {self.command} {self.path} -> {url} body={len(body)}B "
                    f"headers=[{debug_request_headers(headers)}]\n"
                )
                if body:
                    sys.stderr.write(
                        f"[proxy] request body preview={body_preview(body, REQUEST_BODY_LOG_LIMIT)}\n"
                    )

            try:
                upstream = requests.request(
                    method=self.command,
                    url=url,
                    headers=headers,
                    data=body,
                    proxies=proxies,
                    impersonate=cfg.impersonate,
                    stream=True,
                    timeout=(cfg.connect_timeout, cfg.read_timeout),
                    allow_redirects=False,
                )
            except Exception as err:
                if cfg.verbose:
                    sys.stderr.write(f"[proxy] upstream request failed: {err}\n")
                message = {
                    "error": "upstream_request_failed",
                    "detail": str(err),
                    "url": url,
                }
                encoded = json.dumps(message, ensure_ascii=False).encode("utf-8")
                self.close_connection = True
                self.send_response(HTTPStatus.BAD_GATEWAY)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(encoded)
                return

            try:
                self.close_connection = True
                if cfg.verbose and upstream.status_code >= 400:
                    content_type = upstream.headers.get("Content-Type", "")
                    try:
                        preview = upstream.content[:ERROR_BODY_LOG_LIMIT]
                    except Exception:
                        preview = b""
                    if preview:
                        try:
                            preview_text = preview.decode("utf-8", errors="replace")
                        except Exception:
                            preview_text = repr(preview)
                    else:
                        preview_text = "<empty>"
                    sys.stderr.write(
                        "[proxy] upstream error "
                        f"status={upstream.status_code} "
                        f"content_type={content_type or '-'} "
                        f"body_preview={preview_text}\n"
                    )

                self.send_response(upstream.status_code)
                for key, value in response_headers_for_client(upstream):
                    self.send_header(key, value)
                # We stream close-delimited bodies to stay compatible with SSE-style responses
                # without re-buffering the whole upstream payload in memory.
                self.send_header("Connection", "close")
                self.send_header("X-Curl-Cffi-Proxy", "1")
                self.end_headers()
                response_started = True
                if upstream.status_code < 400 and is_responses_sse_stream(self.path, upstream):
                    responses_tracker = ResponsesSseTerminalTracker()

                if self.command == "HEAD":
                    return

                for chunk in upstream.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    if responses_tracker is not None:
                        responses_tracker.feed(chunk)
                    self.wfile.write(chunk)
                    self.wfile.flush()
                maybe_emit_responses_incomplete_event(
                    self,
                    responses_tracker,
                    "stream closed before response.completed",
                    cfg.verbose,
                )
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as err:
                if cfg.verbose:
                    sys.stderr.write(f"[proxy] upstream stream failed: {err}\n")
                maybe_emit_responses_incomplete_event(
                    self,
                    responses_tracker,
                    f"stream closed before response.completed: {err}",
                    cfg.verbose,
                )
                return
            finally:
                upstream.close()
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception:
            sys.stderr.write("[proxy] unexpected handler error:\n")
            traceback.print_exc(file=sys.stderr)
            if response_started:
                return
            try:
                payload = json.dumps(
                    {
                        "error": "proxy_internal_error",
                        "detail": "unexpected handler error",
                    }
                ).encode("utf-8")
                self.close_connection = True
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                return


def main() -> int:
    args = parse_args()
    try:
        host, port = split_host_port(args.listen)
        args.upstream_origin = normalize_origin(args.upstream_origin)
    except ValueError as err:
        print(f"error: {err}", file=sys.stderr)
        return 2

    server = ProxyServer((host, port), args)
    display_host = host if host not in {"0.0.0.0", ""} else "127.0.0.1"
    print(
        f"curl_cffi proxy listening on http://{display_host}:{port} "
        f"(upstream_origin={args.upstream_origin}, proxy={args.proxy or '-'}, impersonate={args.impersonate})"
    )
    print(
        "point CodexManager at: "
        f"CODEXMANAGER_UPSTREAM_BASE_URL=http://{display_host}:{port}/backend-api/codex"
    )
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nstopping proxy")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
