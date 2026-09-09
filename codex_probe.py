"""Codex connectivity checks without desktop automation.

Default: authenticated HTTPS plus WebSocket handshake and ping/pong, no generation.
Opt-in generation: both streaming transports must return text and response.completed.
"""
from __future__ import annotations

import base64
import hashlib
import http.client
import json
import os
import re
import socket
import ssl
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse


CLIENT_VERSION = "0.153.4"
MAX_BYTES = 2 * 1024 * 1024
MAX_HEADER = 65536
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class ProbeError(Exception):
    def __init__(self, message: str, kind: str = "network") -> None:
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True)
class Credentials:
    mode: str
    token: str = field(repr=False)
    account_id: str = field(default="", repr=False)

    def headers(self) -> dict[str, str]:
        values = {
            "Authorization": f"Bearer {self.token}",
            "User-Agent": f"codex_cli_rs/{CLIENT_VERSION}",
            "originator": "codex_cli_rs",
        }
        if self.account_id:
            values["ChatGPT-Account-Id"] = self.account_id
        if any("\r" in v or "\n" in v or not v.isascii() for v in values.values()):
            raise ProbeError("登录凭据包含无效字符", "auth")
        return values


def codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))


def load_credentials(args: Any) -> Credentials:
    """Re-read on every round so externally renewed credentials take effect."""
    mode = getattr(args, "codex_auth_mode", "auto")
    data: dict[str, Any] = {}
    path = Path(getattr(args, "codex_auth_file", None) or codex_home() / "auth.json")
    if mode != "api-key":
        try:
            raw = path.read_bytes()
            if len(raw) > MAX_BYTES:
                raise ValueError()
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError()
        except FileNotFoundError:
            if mode == "chatgpt":
                raise ProbeError("缺少 Codex auth.json 登录文件", "auth") from None
        except (OSError, ValueError):
            raise ProbeError("无法读取 Codex 登录文件或文件格式无效", "auth") from None
    if mode == "auto":
        mode = "chatgpt" if data.get("auth_mode") == "chatgpt" or (not data.get("auth_mode") and data.get("tokens")) else "api-key"
    if mode == "chatgpt":
        tokens = data.get("tokens") or {}
        if not isinstance(tokens, dict):
            raise ProbeError("Codex 登录文件缺少有效 tokens 对象", "auth")
        token, account = tokens.get("access_token"), tokens.get("account_id")
        if not isinstance(token, str) or not token or not isinstance(account, str) or not account:
            raise ProbeError("Codex 登录文件缺少 access_token/account_id", "auth")
        # Only inspect expiry; server-side authentication remains authoritative.
        try:
            claims = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "===").decode())
            expiry = claims.get("exp")
        except (ValueError, IndexError, AttributeError, UnicodeError):
            expiry = None
        if isinstance(expiry, (int, float)) and expiry <= time.time() + 60:
            raise ProbeError("Codex 登录已过期或即将过期，请更新登录凭据后重试", "auth")
        credentials = Credentials(mode, token, account)
    else:
        key = os.getenv(getattr(args, "openai_api_key_env", "OPENAI_API_KEY"), "") or data.get("OPENAI_API_KEY")
        if not isinstance(key, str) or not key:
            raise ProbeError("缺少真实凭据：需要 Codex 登录文件或 API Key", "auth")
        credentials = Credentials("api-key", key)
    credentials.headers()
    return credentials


def configured_value(key: str, section: str = "") -> str:
    # Only read the small set of scalar settings used by these probes. Never
    # evaluate TOML or return arbitrary provider headers/credential fields.
    try:
        lines = (codex_home() / "config.toml").read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    current_section = ""
    for line in lines:
        table = re.fullmatch(r"\s*\[([^\]]+)\]\s*(?:#.*)?", line)
        if table:
            current_section = table[1]
            continue
        if current_section != section:
            continue
        match = re.fullmatch(r'\s*' + re.escape(key) + r'\s*=\s*("(?:[^"\\]|\\.)*"|\'[^\']*\'|true|false)\s*(?:#.*)?', line)
        if match:
            value = match[1]
            try:
                return json.loads(value) if value.startswith('"') else value[1:-1] if value.startswith("'") else value
            except ValueError:
                return ""
    return ""


def validate_provider(credentials: Credentials) -> None:
    """Do not silently test OpenAI when Codex is configured for another backend."""
    provider = configured_value("model_provider") or "openai"
    section = "model_providers." + provider
    base = configured_value("base_url", section)
    wire = configured_value("wire_api", section)
    if wire and wire != "responses":
        raise ProbeError("Codex 当前 provider 不是 Responses 协议，不能代用官方链路验证", "config")
    if provider != "openai" and configured_value("requires_openai_auth", section) != "true":
        raise ProbeError("Codex 当前自定义 provider 未使用 OpenAI 认证，请明确配置对应测试链路", "config")
    if not base:
        base = os.getenv("OPENAI_BASE_URL", "")
    if base and base.rstrip("/") not in ("https://chatgpt.com/backend-api/codex", "https://api.openai.com/v1"):
        raise ProbeError("Codex 配置了自定义后端，不能用官方端点代替该链路验证", "config")
    if base:
        expected = "https://chatgpt.com/backend-api/codex" if credentials.mode == "chatgpt" else "https://api.openai.com/v1"
        if base.rstrip("/") != expected:
            raise ProbeError("Codex provider 地址与探测认证方式不一致", "config")
    chatgpt_base = configured_value("chatgpt_base_url")
    if credentials.mode == "chatgpt" and chatgpt_base and chatgpt_base.rstrip("/") != "https://chatgpt.com/backend-api":
        raise ProbeError("Codex 配置了自定义 ChatGPT 后端，不能代用默认链路验证", "config")


def validate_destination(url: str, credentials: Credentials, scheme: str) -> None:
    try:
        parsed = urlparse(url)
        host = "chatgpt.com" if credentials.mode == "chatgpt" else "api.openai.com"
        prefix = "/backend-api/codex/" if credentials.mode == "chatgpt" else "/v1/"
        valid = (parsed.scheme == scheme and parsed.hostname == host
                 and parsed.port in (None, 443) and not parsed.username and not parsed.password
                 and parsed.path.startswith(prefix) and not parsed.fragment
                 and not any(c in url for c in "\r\n"))
    except ValueError:
        valid = False
    if not valid:
        raise ProbeError("探测地址必须属于当前认证方式对应的官方 HTTPS/WSS 后端（443）", "config")


@dataclass(frozen=True)
class Settings:
    credentials: Credentials
    model: str
    api_url: str
    sse_url: str
    ws_url: str
    timeout: float
    mode: str = "network"


def probe_mode(args: Any) -> str:
    return "generation" if getattr(args, "strict_stream_probes", False) else getattr(args, "probe_mode", "network")


def probe_label(args: Any) -> str:
    return "Codex 生成验证" if probe_mode(args) == "generation" else "Codex 网络/协议验证"


def settings(args: Any) -> Settings:
    credentials = load_credentials(args)
    validate_provider(credentials)
    base = "https://chatgpt.com/backend-api/codex" if credentials.mode == "chatgpt" else "https://api.openai.com/v1"
    mode = probe_mode(args)
    model = getattr(args, "openai_stream_model", None) or ""
    if mode == "generation" and not model:
        raise ProbeError("生成验证请显式指定 --openai-stream-model；网络模式无需模型", "config")
    if any(getattr(args, key, False) for key in ("quick_route_only", "skip_api_probe", "skip_sse_probe", "skip_websocket_probe")):
        raise ProbeError("请用 --probe-mode 选择验证范围，移除旧 quick/skip 探测参数", "config")
    api_url = getattr(args, "openai_api_url", None) or base + "/models"
    if credentials.mode == "chatgpt" and not getattr(args, "openai_api_url", None):
        api_url += "?client_version=" + CLIENT_VERSION
    sse_url = getattr(args, "openai_sse_url", None) or base + "/responses"
    ws_url = getattr(args, "openai_websocket_url", None) or "wss" + base[5:] + "/responses"
    for url, scheme in ((api_url, "https"), (sse_url, "https"), (ws_url, "wss")):
        validate_destination(url, credentials, scheme)
    return Settings(credentials, model, api_url, sse_url, ws_url, float(getattr(args, "stream_timeout", 45)), mode)


class Deadline:
    def __init__(self, seconds: float) -> None:
        self.end = time.monotonic() + seconds

    def remaining(self) -> float:
        remaining = self.end - time.monotonic()
        if remaining <= 0:
            raise ProbeError("探测总时限已到，响应未正常完成")
        return remaining

    def arm(self, sock: Any) -> None:
        sock.settimeout(self.remaining())


def tls_context() -> ssl.SSLContext:
    ca = os.getenv("CODEX_CA_CERTIFICATE") or os.getenv("SSL_CERT_FILE")
    try:
        return ssl.create_default_context(cafile=ca or None)
    except (OSError, ssl.SSLError):
        raise ProbeError("无法加载 TLS CA 证书配置", "config") from None


def http_error(status: int) -> ProbeError:
    kind = "auth" if status == 401 else "quota" if status == 429 else "config" if status in (400, 404, 422) else "service" if status >= 500 else "network"
    # Never echo bodies: an upstream may echo a credential in its error message.
    return ProbeError(f"HTTP {status}（请求失败）", kind)


def open_http(url: str, port: int, cfg: Settings, deadline: Deadline, payload: dict[str, Any] | None = None) -> tuple[Any, Any, Any]:
    validate_destination(url, cfg.credentials, "https")
    parsed = urlparse(url)
    conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=deadline.remaining(), context=tls_context())
    conn.set_tunnel(parsed.hostname, 443)
    headers = cfg.credentials.headers()
    headers["Accept"] = "text/event-stream" if payload is not None else "application/json"
    headers["Accept-Encoding"] = "identity"
    headers["Content-Type"] = "application/json"
    path = parsed.path + ("?" + parsed.query if parsed.query else "")
    try:
        conn.connect()
        sock = conn.sock
        deadline.arm(sock)
        conn.request("POST" if payload is not None else "GET", path,
                     body=json.dumps(payload).encode() if payload is not None else None, headers=headers)
        deadline.arm(sock)
        response = conn.getresponse()
        if response.status != 200:
            raise http_error(response.status)
        return conn, response, sock
    except BaseException:
        conn.close()
        raise


def http_chunks(response: Any, sock: Any, deadline: Deadline) -> Iterator[bytes]:
    total = 0
    while True:
        deadline.arm(sock)
        chunk = response.read1(4096)
        if not chunk:
            return
        total += len(chunk)
        if total > MAX_BYTES:
            raise ProbeError("响应超过探测大小上限")
        yield chunk


def probe_models(cfg: Settings, port: int) -> str:
    deadline = Deadline(cfg.timeout)
    conn, response, sock = open_http(cfg.api_url, port, cfg, deadline)
    try:
        if response.getheader("Content-Type", "").split(";")[0].strip() != "application/json":
            raise ProbeError("模型接口未返回 JSON")
        try:
            data = json.loads(b"".join(http_chunks(response, sock, deadline)))
        except ValueError:
            raise ProbeError("模型接口 JSON 无效") from None
        key = "models" if cfg.credentials.mode == "chatgpt" else "data"
        models = data.get(key) if isinstance(data, dict) else None
        if not isinstance(models, list) or not models:
            raise ProbeError("模型接口没有返回有效模型列表")
        # The authenticated generation below is authoritative for model access;
        # aliases need not be listed under the same name by the catalogue.
        return f"HTTP 200，真实鉴权成功，收到 {len(models)} 个模型"
    finally:
        response.close()
        conn.close()


def request_payload(cfg: Settings, expected: str) -> dict[str, Any]:
    return {
        "model": cfg.model,
        "instructions": "This is a connectivity check. Return only the exact text requested by the user. Do not use tools.",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "Reply with exactly: " + expected}]}],
        "tools": [], "store": False,
    }


def decode_event(data: str) -> dict[str, Any]:
    try:
        event = json.loads(data)
    except ValueError:
        raise ProbeError("流事件不是有效 JSON") from None
    if not isinstance(event, dict) or not isinstance(event.get("type"), str):
        raise ProbeError("流事件缺少有效 type")
    return event


def sse_events(chunks: Iterator[bytes]) -> Iterator[dict[str, Any]]:
    pending = b""
    data: list[str] = []
    event_type = ""
    for chunk in chunks:
        pending += chunk
        if len(pending) > MAX_HEADER:
            raise ProbeError("SSE 行超过大小上限")
        while b"\n" in pending:
            raw, pending = pending.split(b"\n", 1)
            try:
                line = raw.rstrip(b"\r").decode("utf-8")
            except UnicodeError:
                raise ProbeError("SSE 包含无效 UTF-8") from None
            if not line:
                if event_type in ("error", "response.failed", "response.incomplete") and not data:
                    raise ProbeError("SSE 收到错误事件")
                if data:
                    joined = "\n".join(data)
                    if joined == "[DONE]":
                        return
                    event = decode_event(joined)
                    if event_type and event_type != event["type"]:
                        raise ProbeError("SSE event 与 JSON type 不一致")
                    yield event
                data, event_type = [], ""
            elif line.startswith("data:"):
                data.append(line[5:].lstrip(" "))
            elif line.startswith("event:"):
                event_type = line[6:].strip()
    # Do not accept a truncated final event without its terminating blank line.


def verify_events(events: Iterator[dict[str, Any]], expected: str) -> int:
    deltas: list[str] = []
    response_id = ""
    count = 0
    for event in events:
        count += 1
        if count > 4096:
            raise ProbeError("流事件数量超过上限")
        typ = event["type"]
        if typ in ("error", "response.failed", "response.incomplete"):
            error = event.get("error") or (event.get("response") or {}).get("error") or {}
            code = error.get("code", "") if isinstance(error, dict) else ""
            kind = "quota" if code in ("insufficient_quota", "rate_limit_exceeded", "usage_limit_reached") else "auth" if code in ("invalid_api_key", "token_expired", "invalid_authentication") else "service"
            raise ProbeError(f"收到 {typ}，生成失败", kind)
        if typ == "response.created":
            response = event.get("response") or {}
            response_id = response.get("id", "") if isinstance(response, dict) else ""
        if typ == "response.output_text.delta":
            delta = event.get("delta")
            if not isinstance(delta, str):
                raise ProbeError("输出增量格式无效")
            deltas.append(delta)
        if typ == "response.completed":
            response = event.get("response")
            if not isinstance(response, dict) or response.get("status") != "completed" or not response.get("id") or response.get("error"):
                raise ProbeError("完成事件未包含有效 completed 响应")
            if response_id and response["id"] != response_id:
                raise ProbeError("流响应 ID 不一致")
            output = response.get("output")
            if not isinstance(output, list):
                raise ProbeError("完成事件缺少输出")
            text = "".join(
                part.get("text", "") for item in output if isinstance(item, dict) and item.get("type") == "message"
                and item.get("role") == "assistant" for part in item.get("content", [])
                if isinstance(part, dict) and part.get("type") == "output_text" and isinstance(part.get("text"), str)
            )
            # Codex can omit already-streamed output from the completed event.
            # The nonce still has to match the real text deltas in full.
            if not deltas or "".join(deltas).strip() != expected or (output and text.strip() != expected):
                raise ProbeError("真实输出与探测口令不一致，或没有收到文本增量")
            return count
    raise ProbeError("连接在 response.completed 前结束")


def probe_sse(cfg: Settings, port: int) -> str:
    expected = "OK_" + os.urandom(4).hex()
    payload = {**request_payload(cfg, expected), "stream": True}
    deadline = Deadline(cfg.timeout)
    conn, response, sock = open_http(cfg.sse_url, port, cfg, deadline, payload)
    try:
        content_type = response.getheader("Content-Type", "").split(";")[0].strip()
        # Codex's ChatGPT gateway can omit this header. In that case the full
        # SSE framing, nonce output and completion checks are still mandatory.
        if content_type != "text/event-stream" and not (not content_type and cfg.credentials.mode == "chatgpt"):
            raise ProbeError("HTTP 200 但 Content-Type 不是 text/event-stream")
        count = verify_events(sse_events(http_chunks(response, sock, deadline)), expected)
        return f"HTTP 200，真实文本校验通过，response.completed（{count} 个事件）"
    finally:
        response.close()
        conn.close()


def read_header(sock: Any, deadline: Deadline) -> tuple[int, dict[str, str]]:
    # Single-byte header reads avoid consuming the first WS frame or TLS bytes.
    data = bytearray()
    while not data.endswith(b"\r\n\r\n"):
        deadline.arm(sock)
        chunk = sock.recv(1)
        if not chunk:
            raise ProbeError("连接在 HTTP 响应头完成前关闭")
        data.extend(chunk)
        if len(data) > MAX_HEADER:
            raise ProbeError("HTTP 响应头过大")
    lines = data.decode("iso-8859-1").split("\r\n")
    match = re.fullmatch(r"HTTP/1\.[01] (\d{3})(?: .*)?", lines[0])
    if not match:
        raise ProbeError("HTTP 状态行无效")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            continue
        key, sep, value = line.partition(":")
        key = key.lower()
        if not sep or (key in headers and key in ("sec-websocket-accept", "upgrade", "sec-websocket-extensions", "sec-websocket-protocol")):
            raise ProbeError("HTTP 响应头无效或关键握手头重复")
        headers[key] = headers[key] + ", " + value.strip() if key in headers else value.strip()
    return int(match[1]), headers


def send_frame(sock: Any, opcode: int, payload: bytes, deadline: Deadline) -> None:
    mask = os.urandom(4)
    size = len(payload)
    header = bytes([0x80 | opcode])
    if size < 126:
        header += bytes([0x80 | size])
    elif size <= 65535:
        header += b"\xfe" + struct.pack("!H", size)
    else:
        header += b"\xff" + struct.pack("!Q", size)
    masked = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
    deadline.arm(sock)
    sock.sendall(header + mask + masked)


def recv_exact(sock: Any, count: int, deadline: Deadline) -> bytes:
    data = bytearray()
    while len(data) < count:
        deadline.arm(sock)
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise ProbeError("WebSocket 在帧完成前断开")
        data.extend(chunk)
    return bytes(data)


def websocket_frames(sock: Any, deadline: Deadline) -> Iterator[tuple[bool, int, bytes]]:
    total = 0
    while True:
        first, second = recv_exact(sock, 2, deadline)
        fin, opcode = bool(first & 0x80), first & 0x0F
        if first & 0x70 or second & 0x80:
            raise ProbeError("WebSocket 帧包含未协商扩展或服务端掩码")
        size = second & 0x7F
        if size == 126:
            size = struct.unpack("!H", recv_exact(sock, 2, deadline))[0]
        elif size == 127:
            size = struct.unpack("!Q", recv_exact(sock, 8, deadline))[0]
        total += size + 2
        if total > MAX_BYTES or (opcode >= 8 and (size > 125 or not fin)):
            raise ProbeError("WebSocket 帧大小或控制帧格式无效")
        payload = recv_exact(sock, size, deadline)
        yield fin, opcode, payload


def websocket_events(sock: Any, deadline: Deadline) -> Iterator[dict[str, Any]]:
    fragments = bytearray()
    fragmented = False
    for fin, opcode, payload in websocket_frames(sock, deadline):
        if opcode == 8:
            raise ProbeError("WebSocket 在 response.completed 前关闭")
        if opcode == 9:
            send_frame(sock, 10, payload, deadline)
            continue
        if opcode == 10:
            continue
        if opcode == 1 and not fragmented:
            fragments = bytearray(payload)
        elif opcode == 0 and fragmented:
            fragments.extend(payload)
        else:
            raise ProbeError("WebSocket 数据帧顺序或类型无效")
        fragmented = not fin
        if fin:
            try:
                text = fragments.decode("utf-8")
            except UnicodeError:
                raise ProbeError("WebSocket 文本不是有效 UTF-8") from None
            yield decode_event(text)
            fragments.clear()


def open_websocket(cfg: Settings, port: int, deadline: Deadline) -> Any:
    validate_destination(cfg.ws_url, cfg.credentials, "wss")
    parsed = urlparse(cfg.ws_url)
    sock = socket.create_connection(("127.0.0.1", port), timeout=deadline.remaining())
    try:
        deadline.arm(sock)
        sock.sendall(f"CONNECT {parsed.hostname}:443 HTTP/1.1\r\nHost: {parsed.hostname}:443\r\n\r\n".encode("ascii"))
        status, _ = read_header(sock, deadline)
        if status != 200:
            raise ProbeError(f"代理 CONNECT 失败：HTTP {status}")
        deadline.arm(sock)
        sock = tls_context().wrap_socket(sock, server_hostname=parsed.hostname)
        key = base64.b64encode(os.urandom(16)).decode()
        path = parsed.path + ("?" + parsed.query if parsed.query else "")
        headers = {"Host": parsed.hostname, "Upgrade": "websocket", "Connection": "Upgrade",
                   "Sec-WebSocket-Key": key, "Sec-WebSocket-Version": "13", **cfg.credentials.headers()}
        request = f"GET {path} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
        deadline.arm(sock)
        sock.sendall(request.encode("ascii"))
        status, response_headers = read_header(sock, deadline)
        if status != 101:
            raise http_error(status)
        accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
        if (response_headers.get("sec-websocket-accept") != accept
                or response_headers.get("upgrade", "").lower() != "websocket"
                or "upgrade" not in [v.strip().lower() for v in response_headers.get("connection", "").split(",")]
                or response_headers.get("sec-websocket-extensions") or response_headers.get("sec-websocket-protocol")):
            raise ProbeError("WebSocket 101 握手校验失败")
        return sock
    except BaseException:
        sock.close()
        raise


def probe_websocket_network(cfg: Settings, port: int) -> str:
    """Only RFC 6455 control frames: never send response.create or any text."""
    deadline = Deadline(cfg.timeout)
    sock = open_websocket(cfg, port, deadline)
    try:
        challenge = os.urandom(16)
        send_frame(sock, 9, challenge, deadline)
        for index, (fin, opcode, payload) in enumerate(websocket_frames(sock, deadline)):
            if opcode == 10 and payload == challenge:
                send_frame(sock, 8, struct.pack("!H", 1000), deadline)
                return "101 握手校验通过，随机 ping/pong 往返通过（未发送生成请求）"
            if opcode == 9:
                send_frame(sock, 10, payload, deadline)
            elif opcode != 10:
                raise ProbeError("WebSocket 在匹配 pong 前关闭或返回非控制帧")
            if index >= 63:
                raise ProbeError("WebSocket 未在控制帧上限内返回匹配 pong")
        raise ProbeError("WebSocket 没有返回匹配的 pong")
    finally:
        sock.close()


def probe_websocket(cfg: Settings, port: int) -> str:
    deadline = Deadline(cfg.timeout)
    sock = open_websocket(cfg, port, deadline)
    try:
        expected = "OK_" + os.urandom(4).hex()
        payload = {"type": "response.create", **request_payload(cfg, expected)}
        send_frame(sock, 1, json.dumps(payload).encode(), deadline)
        count = verify_events(websocket_events(sock, deadline), expected)
        send_frame(sock, 8, struct.pack("!H", 1000), deadline)
        return f"101 握手校验通过，真实文本校验通过，response.completed（{count} 个事件）"
    finally:
        sock.close()


def run_step(name: str, probe: Any, cfg: Settings, port: int) -> tuple[bool | None, str]:
    started = time.monotonic()
    try:
        message = probe(cfg, port)
        elapsed = int((time.monotonic() - started) * 1000)
        return True, f"{name}：{message}，{elapsed} 毫秒"
    except ProbeError as exc:
        blocked = exc.kind in ("auth", "quota", "config", "service")
        label = {"auth": "鉴权失败", "quota": "额度或限流", "config": "配置错误", "service": "服务端错误"}.get(exc.kind, "网络/协议失败")
        return None if blocked else False, f"{name}：{label}，{exc}"
    except (OSError, http.client.HTTPException) as exc:
        return False, f"{name}：网络传输失败（{type(exc).__name__}）"
