#!/usr/bin/env python3
"""
在终端持续监控本地 Clash Verge / Mihomo 的 ChatGPT 连通性。

示例：
  ./monitor_clash_verge.py
  ./monitor_clash_verge.py --interval 60 --clear
  ./monitor_clash_verge.py --url https://www.baidu.com/
  ./monitor_clash_verge.py --current-only
  ./monitor_clash_verge.py --switch-group 大哥云
  ./monitor_clash_verge.py --no-auto-switch
  CLASH_API=http://127.0.0.1:9097 CLASH_SECRET=xxx ./monitor_clash_verge.py
  ./monitor_clash_verge.py --unix-socket /tmp/verge/verge-mihomo.sock
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import http.client
import json
import os
import re
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen


DEFAULT_TEST_URL = "https://chatgpt.com/cdn-cgi/trace"
DEFAULT_OPENAI_API_URL = "https://api.openai.com/v1/models"
DEFAULT_OPENAI_SSE_URL = "https://api.openai.com/v1/responses"
DEFAULT_OPENAI_WEBSOCKET_URL = "wss://api.openai.com/v1/responses"
DEFAULT_OPENAI_STREAM_MODEL = "gpt-5.6"
DEFAULT_EXCLUDE_REGEX = r"香港|港|🇭🇰|Hong\s*Kong|HongKong|\bHKG\b|\bHK(?:\b|[0-9_-])"
DEFAULT_SKIP_CANDIDATE_REGEX = r"剩余流量|套餐到期|距离下次重置|Traffic|Expire|Reset"
DEFAULT_FREE_SUBSCRIPTION_URLS = (
    "https://ghfile.geekertao.top/https://github.com/PuddinCat/BestClash/blob/main/proxies.yaml",
    "https://proxy.v2gh.com/https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub",
    "https://raw.githubusercontent.com/shaoyouvip/free/refs/heads/main/all.yaml",
)
ANSI_GREEN = "\033[32m"
ANSI_RED = "\033[31m"
ANSI_RESET = "\033[0m"
CLASH_VERGE_DIR = (
    Path.home() / "Library/Application Support/io.github.clash-verge-rev.clash-verge-rev"
)
DEFAULT_LOG_DIR = Path.cwd()
DEFAULT_CACHE_DIR = Path.cwd() / ".clash-monitor-cache"
DEFAULT_FREE_STATS_PATH = Path.cwd() / "clash-free-stats.json"
DEFAULT_PROFILES_YAML = CLASH_VERGE_DIR / "profiles.yaml"
DEFAULT_PROFILE_DIR = CLASH_VERGE_DIR / "profiles"
COMMON_CORE_PATHS = (
    "/Applications/Clash Verge.app/Contents/MacOS/verge-mihomo",
    "/Applications/Clash Verge.app/Contents/MacOS/verge-mihomo-alpha",
    "/Applications/Clash Verge.app/Contents/MacOS/clash-meta",
    "/Applications/Clash Verge.app/Contents/MacOS/clash",
)
COMMON_HTTP_CONTROLLERS = (
    "http://127.0.0.1:9097",
    "http://127.0.0.1:9090",
    "http://127.0.0.1:19090",
)
COMMON_UNIX_SOCKETS = (
    "/tmp/verge/verge-mihomo.sock",
    "/tmp/clash-verge-rev.sock",
)
GROUP_TYPES = {
    "Selector",
    "URLTest",
    "Fallback",
    "LoadBalance",
    "Relay",
    "Compatible",
}
SKIP_PROXY_NAMES = {"DIRECT", "REJECT", "REJECT-DROP", "PASS"}
LOG_FILE: Any = None


class ApiError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class ProfileError(Exception):
    pass


@dataclass(frozen=True)
class Controller:
    base_url: str | None = None
    unix_socket: str | None = None
    secret: str = ""

    @property
    def label(self) -> str:
        if self.unix_socket:
            return f"unix:{self.unix_socket}"
        return self.base_url or "未知"


@dataclass(frozen=True)
class ConfigHint:
    path: Path
    controller: str = ""
    unix_socket: str = ""
    secret: str = ""
    mixed_port: int | None = None


@dataclass(frozen=True)
class RemoteProfile:
    uid: str
    name: str
    path: Path
    source_type: str = "owned"
    source_url: str = ""


@dataclass(frozen=True)
class NodeTarget:
    api_name: str
    name: str
    proxy_type: str
    subscription: str = ""
    profile_uid: str = ""
    profile_path: str = ""
    source_type: str = "owned"
    source_url: str = ""


@dataclass(frozen=True)
class DelayResult:
    api_name: str
    name: str
    proxy_type: str
    subscription: str
    ok: bool
    delay_ms: int | None = None
    error: str = ""


@dataclass
class AllProfilesRuntime:
    controller: Controller
    version: Any
    process: Any
    temp_dir: Any
    log_file: Any
    targets: dict[str, NodeTarget]
    profile_count: int
    node_count: int
    core_path: Path
    config_path: Path
    mixed_port: int
    label: str = "订阅"

    def close(self) -> None:
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log_file:
            self.log_file.close()
        if self.temp_dir:
            self.temp_dir.cleanup()


@dataclass(frozen=True)
class RunOutcome:
    owned_profiles: AllProfilesRuntime | None
    free_profiles: AllProfilesRuntime | None
    active_source_type: str = "owned"
    no_available_chatgpt_node: bool = False


@dataclass(frozen=True)
class SwitchOutcome:
    route_ok: bool | None = None
    source_type: str = ""


@dataclass(frozen=True)
class ProbeStep:
    name: str
    ok: bool
    message: str


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, unix_socket: str, timeout: float | None = None) -> None:
        super().__init__("localhost", timeout=timeout)
        self.unix_socket = unix_socket

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if self.timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
            sock.settimeout(self.timeout)
        sock.connect(self.unix_socket)
        self.sock = sock


def strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def log(*values: Any, sep: str = " ", end: str = "\n", file: Any = None, flush: bool = False) -> None:
    target = file or sys.stdout
    text = sep.join(str(value) for value in values)
    print(text, end=end, file=target, flush=flush)
    if LOG_FILE is not None and target in (sys.stdout, sys.stderr):
        LOG_FILE.write(strip_ansi(text) + end)
        LOG_FILE.flush()


def clean_yaml_scalar(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value[0] in {"'", '"'} and value[-1:] == value[0]:
        return value[1:-1]
    if "#" in value:
        value = value.split("#", 1)[0].strip()
    if value.lower() in {"null", "none", "~"}:
        return ""
    return value


def load_yaml_data(path: Path) -> Any:
    try:
        import yaml  # type: ignore

        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except ImportError:
        pass
    except Exception as exc:
        raise ProfileError(f"Failed to parse YAML {path}: {exc}") from exc

    ruby = shutil.which("ruby") or "/usr/bin/ruby"
    if not Path(ruby).exists():
        raise ProfileError(
            "PyYAML is not installed and Ruby was not found; cannot parse subscription YAML."
        )

    code = "data=YAML.load_file(ARGV[0]); puts JSON.generate(data)"
    try:
        completed = subprocess.run(
            [ruby, "-ryaml", "-rjson", "-e", code, str(path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ProfileError(f"Failed to parse YAML {path} with Ruby: {exc}") from exc

    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ProfileError(f"Ruby returned invalid JSON for {path}") from exc


def profiles_yaml_path(args: argparse.Namespace) -> Path:
    value = args.profiles_yaml or os.getenv("CLASH_PROFILES_YAML")
    return Path(value).expanduser() if value else DEFAULT_PROFILES_YAML


def profile_dir_from(profiles_yaml: Path) -> Path:
    return profiles_yaml.parent / "profiles"


def discover_remote_profiles(args: argparse.Namespace) -> list[RemoteProfile]:
    profiles_yaml = profiles_yaml_path(args)
    profile_dir = profile_dir_from(profiles_yaml)
    if not profiles_yaml.exists():
        return discover_profiles_by_files(profile_dir)

    data = load_yaml_data(profiles_yaml)
    if not isinstance(data, dict):
        raise ProfileError(f"{profiles_yaml} did not contain a profile config object")

    profiles: list[RemoteProfile] = []
    for item in data.get("items", []):
        if not isinstance(item, dict) or item.get("type") != "remote":
            continue
        file_name = item.get("file")
        if not file_name:
            continue
        path = profile_dir / str(file_name)
        if not path.exists():
            continue
        uid = str(item.get("uid") or path.stem)
        name = str(item.get("name") or uid)
        profiles.append(RemoteProfile(uid=uid, name=name, path=path))

    if profiles:
        return profiles
    return discover_profiles_by_files(profile_dir)


def discover_profiles_by_files(profile_dir: Path) -> list[RemoteProfile]:
    profiles: list[RemoteProfile] = []
    if not profile_dir.exists():
        return profiles
    for path in sorted([*profile_dir.glob("*.yaml"), *profile_dir.glob("*.yml")]):
        try:
            data = load_yaml_data(path)
        except ProfileError:
            continue
        if isinstance(data, dict) and isinstance(data.get("proxies"), list):
            profiles.append(RemoteProfile(uid=path.stem, name=path.stem, path=path))
    return profiles


def free_subscription_urls(args: argparse.Namespace) -> list[str]:
    urls = list(DEFAULT_FREE_SUBSCRIPTION_URLS)
    urls.extend(args.free_url or [])
    return list(dict.fromkeys(url.strip() for url in urls if url.strip()))


def cache_file_for_url(url: str, suffix: str) -> Path:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return DEFAULT_CACHE_DIR / f"free-{digest}{suffix}"


def cache_ttl_text(seconds: int) -> str:
    if seconds <= 0:
        return "已过期"
    minutes, remainder = divmod(seconds, 60)
    if minutes:
        return f"{minutes} 分 {remainder} 秒"
    return f"{remainder} 秒"


def write_cache_metadata(
    meta_path: Path,
    *,
    url: str,
    name: str,
    profile_path: Path,
    raw_path: Path,
    node_count: int,
    ttl_seconds: int,
    cached_at: float | None = None,
) -> None:
    now = cached_at if cached_at is not None else time.time()
    metadata = {
        "name": name,
        "url": url,
        "node_count": node_count,
        "profile_cache": str(profile_path),
        "raw_cache": str(raw_path) if raw_path.exists() else "",
        "fetched_at": datetime.fromtimestamp(now).isoformat(timespec="seconds"),
        "expires_at": datetime.fromtimestamp(now + ttl_seconds).isoformat(timespec="seconds"),
        "ttl_seconds": ttl_seconds,
    }
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")


def cached_profile_node_count(path: Path) -> int:
    try:
        data = load_yaml_data(path)
    except ProfileError:
        return 0
    if isinstance(data, dict) and isinstance(data.get("proxies"), list):
        return len([proxy for proxy in data["proxies"] if isinstance(proxy, dict)])
    return 0


def fetch_url_text(url: str, timeout: int = 30) -> str:
    request = Request(url, headers={"User-Agent": "clash-verge-monitor/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def decode_subscription_text(text: str) -> str:
    stripped = "".join(text.split())
    if not stripped:
        return text
    if "://" in text:
        return text
    try:
        padded = stripped + "=" * (-len(stripped) % 4)
        decoded = base64.b64decode(padded, validate=False)
        decoded_text = decoded.decode("utf-8", errors="replace")
        if "://" in decoded_text:
            return decoded_text
    except Exception:
        pass
    return text


def bool_param(values: dict[str, list[str]], *names: str) -> bool:
    for name in names:
        value = (values.get(name) or [""])[0].lower()
        if value in {"1", "true", "yes"}:
            return True
    return False


def first_param(values: dict[str, list[str]], *names: str) -> str:
    for name in names:
        value = (values.get(name) or [""])[0]
        if value:
            return value
    return ""


def parse_trojan_link(line: str) -> dict[str, Any] | None:
    parsed = urlparse(line)
    if not parsed.hostname or not parsed.port or not parsed.username:
        return None
    params = parse_qs(parsed.query)
    proxy: dict[str, Any] = {
        "name": unquote(parsed.fragment) or parsed.hostname,
        "type": "trojan",
        "server": parsed.hostname,
        "port": parsed.port,
        "password": unquote(parsed.username),
        "udp": True,
    }
    sni = first_param(params, "sni", "peer", "host")
    if sni:
        proxy["sni"] = sni
    if bool_param(params, "allowInsecure", "insecure"):
        proxy["skip-cert-verify"] = True
    network = first_param(params, "type", "network")
    if network == "ws":
        proxy["network"] = "ws"
        ws_opts: dict[str, Any] = {}
        path = first_param(params, "path")
        host = first_param(params, "host")
        if path:
            ws_opts["path"] = unquote(path)
        if host:
            ws_opts["headers"] = {"Host": host}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    return proxy


def parse_vless_link(line: str) -> dict[str, Any] | None:
    parsed = urlparse(line)
    if not parsed.hostname or not parsed.port or not parsed.username:
        return None
    params = parse_qs(parsed.query)
    security = first_param(params, "security")
    proxy: dict[str, Any] = {
        "name": unquote(parsed.fragment) or parsed.hostname,
        "type": "vless",
        "server": parsed.hostname,
        "port": parsed.port,
        "uuid": unquote(parsed.username),
        "udp": True,
    }
    if security in {"tls", "reality"}:
        proxy["tls"] = True
    if security == "reality":
        proxy["reality-opts"] = {
            "public-key": first_param(params, "pbk", "public-key"),
            "short-id": first_param(params, "sid", "short-id"),
        }
    flow = first_param(params, "flow")
    if flow:
        proxy["flow"] = flow
    servername = first_param(params, "sni", "servername")
    if servername:
        proxy["servername"] = servername
    if bool_param(params, "allowInsecure", "insecure"):
        proxy["skip-cert-verify"] = True
    network = first_param(params, "type", "network")
    if network:
        proxy["network"] = network
    if network == "ws":
        ws_opts: dict[str, Any] = {}
        path = first_param(params, "path")
        host = first_param(params, "host")
        if path:
            ws_opts["path"] = unquote(path)
        if host:
            ws_opts["headers"] = {"Host": host}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    return proxy


def parse_vmess_link(line: str) -> dict[str, Any] | None:
    raw = line.split("://", 1)[1].strip()
    try:
        padded = raw + "=" * (-len(raw) % 4)
        data = json.loads(base64.b64decode(padded).decode("utf-8", errors="replace"))
    except Exception:
        return None
    server = data.get("add")
    port = data.get("port")
    uuid = data.get("id")
    if not server or not port or not uuid:
        return None
    proxy: dict[str, Any] = {
        "name": data.get("ps") or server,
        "type": "vmess",
        "server": server,
        "port": int(port),
        "uuid": uuid,
        "alterId": int(data.get("aid") or 0),
        "cipher": data.get("scy") or "auto",
        "udp": True,
    }
    if data.get("tls"):
        proxy["tls"] = True
    if data.get("sni"):
        proxy["servername"] = data.get("sni")
    if data.get("net"):
        proxy["network"] = data.get("net")
    if data.get("net") == "ws":
        ws_opts: dict[str, Any] = {}
        if data.get("path"):
            ws_opts["path"] = data.get("path")
        if data.get("host"):
            ws_opts["headers"] = {"Host": data.get("host")}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
    return proxy


def parse_ss_link(line: str) -> dict[str, Any] | None:
    parsed = urlparse(line)
    name = unquote(parsed.fragment) or parsed.hostname or "ss"
    body = line.split("://", 1)[1].split("#", 1)[0].split("?", 1)[0]
    try:
        if "@" in body:
            userinfo, serverinfo = body.rsplit("@", 1)
            if ":" not in userinfo:
                padded = userinfo + "=" * (-len(userinfo) % 4)
                userinfo = base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
        else:
            padded = body + "=" * (-len(body) % 4)
            decoded = base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
            userinfo, serverinfo = decoded.rsplit("@", 1)
        cipher, password = userinfo.split(":", 1)
        server, port_text = serverinfo.rsplit(":", 1)
        return {
            "name": name,
            "type": "ss",
            "server": server,
            "port": int(port_text),
            "cipher": cipher,
            "password": password,
            "udp": True,
        }
    except Exception:
        return None


def parse_share_link(line: str) -> dict[str, Any] | None:
    line = line.strip()
    if line.startswith("trojan://"):
        return parse_trojan_link(line)
    if line.startswith("vless://"):
        return parse_vless_link(line)
    if line.startswith("vmess://"):
        return parse_vmess_link(line)
    if line.startswith("ss://"):
        return parse_ss_link(line)
    return None


def write_free_profile_from_text(url: str, name: str, text: str, output_path: Path) -> int:
    decoded = decode_subscription_text(text)
    proxies: list[dict[str, Any]] = []

    if "proxies:" in decoded[:1000]:
        raw_path = output_path.with_suffix(".raw.yaml")
        raw_path.write_text(decoded, encoding="utf-8")
        data = load_yaml_data(raw_path)
        if isinstance(data, dict) and isinstance(data.get("proxies"), list):
            proxies = [proxy for proxy in data["proxies"] if isinstance(proxy, dict)]
    else:
        for line in decoded.splitlines():
            proxy = parse_share_link(line)
            if proxy:
                proxies.append(proxy)

    seen: set[str] = set()
    cleaned: list[dict[str, Any]] = []
    for index, proxy in enumerate(proxies, start=1):
        proxy = dict(proxy)
        original = str(proxy.get("name") or f"{name}-{index}")
        unique = unique_node_name(name, original, seen) if original in seen else original
        seen.add(unique)
        proxy["name"] = unique
        cleaned.append(proxy)

    proxy_names = [str(proxy["name"]) for proxy in cleaned]
    profile = {
        "proxies": cleaned,
        "proxy-groups": [
            {"name": name, "type": "select", "proxies": proxy_names},
            {
                "name": "自动选择",
                "type": "url-test",
                "proxies": proxy_names,
                "url": DEFAULT_TEST_URL,
                "interval": 300,
            },
        ],
        "rules": [f"MATCH,{name}"],
    }
    output_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(cleaned)


def discover_free_profiles(args: argparse.Namespace) -> list[RemoteProfile]:
    if args.no_free_backup:
        return []

    DEFAULT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    profiles: list[RemoteProfile] = []

    for index, url in enumerate(free_subscription_urls(args), start=1):
        name = f"免费源{index}"
        cache_path = cache_file_for_url(url, ".yaml")
        raw_cache_path = cache_file_for_url(url, ".raw.txt")
        meta_cache_path = cache_file_for_url(url, ".meta.json")
        expired = (
            not cache_path.exists()
            or time.time() - cache_path.stat().st_mtime > args.free_cache_ttl
        )

        if expired:
            try:
                text = fetch_url_text(url, timeout=args.free_fetch_timeout)
                raw_cache_path.write_text(text, encoding="utf-8")
                count = write_free_profile_from_text(url, name, text, cache_path)
                write_cache_metadata(
                    meta_cache_path,
                    url=url,
                    name=name,
                    profile_path=cache_path,
                    raw_path=raw_cache_path,
                    node_count=count,
                    ttl_seconds=args.free_cache_ttl,
                )
                log(
                    f"免费订阅：已刷新 {name}，节点数={count}，"
                    f"本地缓存={cache_path}，地址={url}"
                )
            except Exception as exc:
                if cache_path.exists():
                    log(
                        f"免费订阅：刷新 {name} 失败，使用旧缓存={cache_path}："
                        f"{zh_error(str(exc))}"
                    )
                else:
                    log(f"免费订阅：刷新 {name} 失败且无缓存，跳过：{zh_error(str(exc))}")
                    continue
        else:
            remaining = int(args.free_cache_ttl - (time.time() - cache_path.stat().st_mtime))
            if not meta_cache_path.exists():
                write_cache_metadata(
                    meta_cache_path,
                    url=url,
                    name=name,
                    profile_path=cache_path,
                    raw_path=raw_cache_path,
                    node_count=cached_profile_node_count(cache_path),
                    ttl_seconds=args.free_cache_ttl,
                    cached_at=cache_path.stat().st_mtime,
                )
            log(
                f"免费订阅：使用本地缓存 {name}，剩余有效期约 "
                f"{cache_ttl_text(remaining)}，缓存={cache_path}"
            )

        profiles.append(
            RemoteProfile(
                uid=f"free{index}",
                name=name,
                path=cache_path,
                source_type="free",
                source_url=url,
            )
        )
    return profiles


def read_config_hint(path: Path) -> ConfigHint | None:
    if not path.exists() or not path.is_file():
        return None

    wanted = {
        "external-controller",
        "external-controller-unix",
        "secret",
        "mixed-port",
    }
    values: dict[str, str] = {}

    try:
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = re.match(r"^([A-Za-z0-9_-]+):\s*(.*?)\s*$", line)
            if not match:
                continue
            key, raw_value = match.groups()
            if key in wanted:
                values[key] = clean_yaml_scalar(raw_value)
    except OSError:
        return None

    mixed_port: int | None = None
    raw_mixed_port = values.get("mixed-port", "")
    if raw_mixed_port.isdigit():
        mixed_port = int(raw_mixed_port)

    if not any(values.values()) and mixed_port is None:
        return None

    return ConfigHint(
        path=path,
        controller=values.get("external-controller", ""),
        unix_socket=values.get("external-controller-unix", ""),
        secret=values.get("secret", ""),
        mixed_port=mixed_port,
    )


def config_paths() -> list[Path]:
    home = Path.home()
    paths = [
        home
        / "Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/config.yaml",
        home
        / "Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/clash-verge.yaml",
        home
        / "Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/clash-verge-check.yaml",
        home / "Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/verge.yaml",
        home / ".config/clash/config.yaml",
        home / ".config/mihomo/config.yaml",
    ]
    profile_dir = (
        home / "Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/profiles"
    )
    if profile_dir.exists():
        paths.extend(sorted(profile_dir.glob("*.yaml")))
        paths.extend(sorted(profile_dir.glob("*.yml")))
    return paths


def normalize_controller(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        return value.rstrip("/")
    if value.startswith(":"):
        return f"http://127.0.0.1{value}"
    return f"http://{value}".rstrip("/")


def add_candidate(
    candidates: list[Controller],
    seen: set[tuple[str | None, str | None, str]],
    controller: Controller,
) -> None:
    key = (controller.base_url, controller.unix_socket, controller.secret)
    if key not in seen:
        seen.add(key)
        candidates.append(controller)


def build_candidates(args: argparse.Namespace, hints: list[ConfigHint]) -> list[Controller]:
    candidates: list[Controller] = []
    seen: set[tuple[str | None, str | None, str]] = set()

    env_secret = os.getenv("CLASH_SECRET", "")
    secrets = [env_secret]
    secrets.extend(hint.secret for hint in hints if hint.secret)
    secrets.append("")
    deduped_secrets = list(dict.fromkeys(secrets))

    explicit_api = args.api or os.getenv("CLASH_API") or os.getenv("CLASH_CONTROLLER")
    explicit_socket = args.unix_socket or os.getenv("CLASH_UNIX_SOCKET")

    explicit_secrets = [args.secret or env_secret] if (args.secret or env_secret) else deduped_secrets
    if explicit_socket:
        for secret in explicit_secrets:
            add_candidate(candidates, seen, Controller(unix_socket=explicit_socket, secret=secret))
    if explicit_api:
        for secret in explicit_secrets:
            add_candidate(
                candidates,
                seen,
                Controller(base_url=normalize_controller(explicit_api), secret=secret),
            )

    for hint in hints:
        if hint.unix_socket:
            add_candidate(
                candidates,
                seen,
                Controller(unix_socket=hint.unix_socket, secret=args.secret or hint.secret),
            )
        if hint.controller:
            add_candidate(
                candidates,
                seen,
                Controller(
                    base_url=normalize_controller(hint.controller),
                    secret=args.secret or hint.secret,
                ),
            )

    for socket_path in COMMON_UNIX_SOCKETS:
        if Path(socket_path).exists():
            for secret in deduped_secrets:
                add_candidate(
                    candidates,
                    seen,
                    Controller(unix_socket=socket_path, secret=args.secret or secret),
                )

    for base_url in COMMON_HTTP_CONTROLLERS:
        for secret in deduped_secrets:
            add_candidate(
                candidates,
                seen,
                Controller(base_url=base_url, secret=args.secret or secret),
            )

    return candidates


def api_json(
    controller: Controller,
    path: str,
    timeout: float = 5.0,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> Any:
    headers = {"Host": "127.0.0.1"}
    if controller.secret:
        headers["Authorization"] = f"Bearer {controller.secret}"
    body: bytes | None = None
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(body))

    conn: http.client.HTTPConnection
    if controller.unix_socket:
        conn = UnixHTTPConnection(controller.unix_socket, timeout=timeout)
        request_path = path
    elif controller.base_url:
        parsed = urlparse(controller.base_url)
        klass = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        conn = klass(parsed.hostname or "127.0.0.1", parsed.port, timeout=timeout)
        base_path = parsed.path.rstrip("/")
        request_path = f"{base_path}{path}" if base_path else path
    else:
        raise ApiError("No controller endpoint configured")

    try:
        conn.request(method, request_path, body=body, headers=headers)
        response = conn.getresponse()
        body = response.read()
    except OSError as exc:
        raise ApiError(str(exc)) from exc
    finally:
        conn.close()

    text = body.decode("utf-8", errors="replace")
    if response.status >= 400:
        message = text
        try:
            parsed_body = json.loads(text)
            message = parsed_body.get("message") or parsed_body.get("error") or text
        except json.JSONDecodeError:
            pass
        raise ApiError(message or response.reason, response.status)

    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ApiError(f"Invalid JSON from controller: {text[:120]}") from exc


def find_controller(
    args: argparse.Namespace,
    hints: list[ConfigHint],
) -> tuple[Controller, Any, list[str]]:
    errors: list[str] = []
    for candidate in build_candidates(args, hints):
        try:
            version = api_json(candidate, "/version", timeout=2.0)
            return candidate, version, errors
        except ApiError as exc:
            if exc.status == 401:
                errors.append(f"{candidate.label}: unauthorized")
            else:
                errors.append(f"{candidate.label}: {exc}")
    raise ApiError(
        "Could not connect to Clash controller. "
        "Try --api, --unix-socket, or set CLASH_API / CLASH_SECRET."
    )


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def find_core_path(args: argparse.Namespace) -> Path:
    explicit = args.core or os.getenv("CLASH_CORE") or os.getenv("MIHOMO_CORE")
    if explicit:
        path = Path(explicit).expanduser()
        if path.exists() and os.access(path, os.X_OK):
            return path
        raise ProfileError(f"Core binary is not executable: {path}")

    for path_text in COMMON_CORE_PATHS:
        path = Path(path_text)
        if path.exists() and os.access(path, os.X_OK):
            return path

    for command in ("mihomo", "clash-meta", "clash"):
        found = shutil.which(command)
        if found:
            return Path(found)

    raise ProfileError(
        "Could not find mihomo/clash core. Pass --core or set CLASH_CORE."
    )


def unique_node_name(subscription: str, original_name: str, seen: set[str]) -> str:
    base = f"{subscription} / {original_name}"
    candidate = base
    index = 2
    while candidate in seen:
        candidate = f"{base} #{index}"
        index += 1
    seen.add(candidate)
    return candidate


def build_all_profiles_config(
    args: argparse.Namespace,
    controller_port: int,
    mixed_port: int,
    profiles: list[RemoteProfile] | None = None,
) -> tuple[dict[str, Any], dict[str, NodeTarget], int]:
    profiles = profiles if profiles is not None else discover_remote_profiles(args)
    if not profiles:
        raise ProfileError("No remote subscription profiles were found.")

    proxy_names: list[str] = []
    proxies: list[dict[str, Any]] = []
    targets: dict[str, NodeTarget] = {}
    seen_names: set[str] = set()

    for profile in profiles:
        data = load_yaml_data(profile.path)
        if not isinstance(data, dict):
            continue
        profile_proxies = data.get("proxies", [])
        if not isinstance(profile_proxies, list):
            continue

        for proxy in profile_proxies:
            if not isinstance(proxy, dict):
                continue
            original_name = str(proxy.get("name") or "unnamed")
            proxy_type = str(proxy.get("type") or "Unknown")
            api_name = unique_node_name(profile.name, original_name, seen_names)
            cloned = dict(proxy)
            cloned["name"] = api_name
            proxies.append(cloned)
            proxy_names.append(api_name)
            targets[api_name] = NodeTarget(
                api_name=api_name,
                name=original_name,
                proxy_type=proxy_type,
                subscription=profile.name,
                profile_uid=profile.uid,
                profile_path=str(profile.path),
                source_type=profile.source_type,
                source_url=profile.source_url,
            )

    if not proxies:
        raise ProfileError("Remote subscriptions were found, but no nodes were found in them.")

    config = {
        "mixed-port": mixed_port,
        "allow-lan": False,
        "bind-address": "127.0.0.1",
        "mode": "rule",
        "log-level": "warning",
        "ipv6": False,
        "external-controller": f"127.0.0.1:{controller_port}",
        "secret": "",
        "profile": {"store-selected": False, "store-fake-ip": False},
        "dns": {
            "enable": True,
            "ipv6": False,
            "default-nameserver": ["223.5.5.5", "119.29.29.29"],
            "nameserver": ["223.5.5.5", "119.29.29.29", "8.8.8.8", "1.1.1.1"],
        },
        "proxies": proxies,
        "proxy-groups": [
            {
                "name": "ALL_SUBSCRIPTIONS",
                "type": "select",
                "proxies": proxy_names,
            }
        ],
        "rules": ["MATCH,ALL_SUBSCRIPTIONS"],
    }
    return config, targets, len(profiles)


def last_log_lines(path: Path, count: int = 20) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(lines[-count:])


def start_all_profiles_runtime(
    args: argparse.Namespace,
    profiles: list[RemoteProfile] | None = None,
    label: str = "全部远程订阅",
) -> AllProfilesRuntime:
    core_path = find_core_path(args)
    controller_port = free_tcp_port()
    mixed_port = free_tcp_port()
    temp_dir = tempfile.TemporaryDirectory(prefix="clash-all-profiles-")
    temp_path = Path(temp_dir.name)
    config_path = temp_path / "all-profiles.yaml"
    log_path = temp_path / "mihomo.log"

    config, targets, profile_count = build_all_profiles_config(
        args,
        controller_port=controller_port,
        mixed_port=mixed_port,
        profiles=profiles,
    )
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    log_file = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [str(core_path), "-d", str(temp_path), "-f", str(config_path)],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    controller = Controller(base_url=f"http://127.0.0.1:{controller_port}")

    deadline = time.monotonic() + args.startup_timeout
    last_error = ""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            log_file.flush()
            logs = last_log_lines(log_path)
            log_file.close()
            temp_dir.cleanup()
            raise ProfileError(f"Temporary core exited early.\n{logs}")
        try:
            version = api_json(controller, "/version", timeout=1.0)
            return AllProfilesRuntime(
                controller=controller,
                version=version,
                process=process,
                temp_dir=temp_dir,
                log_file=log_file,
                targets=targets,
                profile_count=profile_count,
                node_count=len(targets),
                core_path=core_path,
                config_path=config_path,
                mixed_port=mixed_port,
                label=label,
            )
        except ApiError as exc:
            last_error = str(exc)
            time.sleep(0.2)

    process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)
    log_file.flush()
    logs = last_log_lines(log_path)
    log_file.close()
    temp_dir.cleanup()
    raise ProfileError(f"Temporary core did not start: {last_error}\n{logs}")


def delay_path(name: str, url: str, timeout_ms: int) -> str:
    query = urlencode({"timeout": str(timeout_ms), "url": url})
    return f"/proxies/{quote(name, safe='')}/delay?{query}"


def test_delay(
    controller: Controller,
    target: NodeTarget,
    url: str,
    timeout_ms: int,
) -> DelayResult:
    try:
        data = api_json(
            controller,
            delay_path(target.api_name, url, timeout_ms),
            timeout=max(timeout_ms / 1000 + 2, 3),
        )
        delay = data.get("delay") if isinstance(data, dict) else None
        if isinstance(delay, int) and delay >= 0:
            return DelayResult(
                api_name=target.api_name,
                name=target.name,
                proxy_type=target.proxy_type,
                subscription=target.subscription,
                ok=True,
                delay_ms=delay,
            )
        message = ""
        if isinstance(data, dict):
            message = str(data.get("message") or data.get("error") or data)
        return DelayResult(
            api_name=target.api_name,
            name=target.name,
            proxy_type=target.proxy_type,
            subscription=target.subscription,
            ok=False,
            error=message or "no delay",
        )
    except ApiError as exc:
        return DelayResult(
            api_name=target.api_name,
            name=target.name,
            proxy_type=target.proxy_type,
            subscription=target.subscription,
            ok=False,
            error=str(exc),
        )


def is_group(name: str, proxy: dict[str, Any]) -> bool:
    proxy_type = str(proxy.get("type", ""))
    return bool(proxy.get("all")) or proxy_type in GROUP_TYPES or name == "GLOBAL"


def collect_targets(
    proxies: dict[str, Any],
    include_groups: bool = False,
    metadata: dict[str, NodeTarget] | None = None,
) -> list[NodeTarget]:
    targets: list[NodeTarget] = []
    metadata = metadata or {}
    for name, proxy in proxies.items():
        if name in SKIP_PROXY_NAMES:
            continue
        if not isinstance(proxy, dict):
            continue
        proxy_type = str(proxy.get("type", "Unknown"))
        if is_group(name, proxy) and not include_groups:
            continue
        if name in metadata:
            original = metadata[name]
            targets.append(
                NodeTarget(
                    api_name=name,
                    name=original.name,
                    proxy_type=proxy_type or original.proxy_type,
                    subscription=original.subscription,
                    profile_uid=original.profile_uid,
                    profile_path=original.profile_path,
                )
            )
        else:
            targets.append(
                NodeTarget(
                    api_name=name,
                    name=name,
                    proxy_type=proxy_type,
                    subscription="当前配置",
                )
            )
    return targets


def selected_groups(proxies: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for name, proxy in proxies.items():
        if not isinstance(proxy, dict):
            continue
        now = proxy.get("now")
        if now and is_group(name, proxy):
            lines.append(f"{name} => {now}")
    return lines


def rules_from_response(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    rules = data.get("rules")
    if not isinstance(rules, list):
        return []
    return [rule for rule in rules if isinstance(rule, dict)]


def fetch_rules(controller: Controller) -> list[dict[str, Any]]:
    try:
        return rules_from_response(api_json(controller, "/rules", timeout=5.0))
    except ApiError:
        return []


def rule_proxy(rule: dict[str, Any]) -> str:
    proxy = rule.get("proxy")
    return str(proxy) if proxy else ""


def rule_index(rule: dict[str, Any]) -> int:
    index = rule.get("index")
    return index if isinstance(index, int) else 0


def rule_target_for_url(rules: list[dict[str, Any]], url: str) -> str:
    host = (urlparse(url).hostname or "").lower().rstrip(".")
    match_proxy = ""
    for rule in sorted(rules, key=rule_index):
        rule_type = str(rule.get("type") or "").replace("-", "").lower()
        payload = str(rule.get("payload") or "").lower().rstrip(".")
        proxy = rule_proxy(rule)
        if not proxy:
            continue
        if rule_type == "match":
            match_proxy = proxy
            break
        if not host or not payload:
            continue
        if rule_type == "domain" and host == payload:
            return proxy
        if rule_type == "domainsuffix" and (host == payload or host.endswith(f".{payload}")):
            return proxy
        if rule_type == "domainkeyword" and payload in host:
            return proxy
    return match_proxy


def single_proxy_rule_target(rules: list[dict[str, Any]]) -> str:
    targets: list[str] = []
    seen: set[str] = set()
    for rule in rules:
        target = rule_proxy(rule)
        if not target or target in SKIP_PROXY_NAMES or target in seen:
            continue
        seen.add(target)
        targets.append(target)
    return targets[0] if len(targets) == 1 else ""


def resolve_chain(name: str, proxies: dict[str, Any]) -> list[str]:
    chain: list[str] = []
    seen: set[str] = set()
    current = name
    while current and current not in seen:
        seen.add(current)
        chain.append(current)
        proxy = proxies.get(current)
        if not isinstance(proxy, dict):
            break
        nxt = proxy.get("now")
        if not nxt or nxt == current:
            break
        current = str(nxt)
    return chain


def current_route_chain(
    proxies: dict[str, Any],
    configs: dict[str, Any],
    rules: list[dict[str, Any]],
    url: str,
) -> list[str]:
    mode = str(get_config_value(configs, "mode") or "").lower()
    if mode == "global":
        return ["global", *resolve_chain("GLOBAL", proxies)]
    if mode == "direct":
        return ["direct", "DIRECT"]
    if mode != "rule":
        return [mode or "未知"]

    target = rule_target_for_url(rules, url) or single_proxy_rule_target(rules)
    if not target:
        return ["rule", "未知"]
    return ["rule", *resolve_chain(target, proxies)]


def format_route_chain(chain: list[str]) -> str:
    return " => ".join(short(part, 64) for part in chain if part)


def get_config_value(configs: dict[str, Any], key: str) -> Any:
    if key in configs:
        return configs[key]
    kebab = key.replace("_", "-")
    if kebab in configs:
        return configs[kebab]
    return None


def mixed_port_from(
    args: argparse.Namespace,
    configs: dict[str, Any],
    hints: list[ConfigHint],
) -> int | None:
    if args.proxy_port:
        return args.proxy_port
    env_port = os.getenv("CLASH_MIXED_PORT", "")
    if env_port.isdigit():
        return int(env_port)
    value = get_config_value(configs, "mixed-port")
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    for hint in hints:
        if hint.mixed_port:
            return hint.mixed_port
    return None


def check_via_mixed_proxy_once(url: str, port: int, timeout_s: float) -> tuple[bool, str, bool]:
    proxy_url = f"http://127.0.0.1:{port}"
    opener = build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
    request = Request(url, headers={"User-Agent": "clash-verge-monitor/1.0"})
    started = time.perf_counter()
    try:
        with opener.open(request, timeout=timeout_s) as response:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return (
                True,
                f"HTTP {response.status}, {elapsed_ms} 毫秒",
                False,
            )
    except HTTPError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        if 200 <= exc.code < 400:
            return (
                True,
                f"HTTP {exc.code}, {elapsed_ms} 毫秒",
                False,
            )
        retryable = exc.code in {408, 429} or 500 <= exc.code < 600
        return (
            False,
            f"HTTP {exc.code}, {elapsed_ms} 毫秒",
            retryable,
        )
    except (OSError, URLError) as exc:
        return False, str(exc), True


def check_via_mixed_proxy(
    url: str,
    port: int,
    timeout_s: float,
    attempts: int = 1,
    retry_delay_s: float = 0.0,
) -> tuple[bool, str]:
    attempts = max(1, attempts)
    failures: list[str] = []
    for attempt in range(1, attempts + 1):
        ok, message, retryable = check_via_mixed_proxy_once(url, port, timeout_s)
        if ok:
            if attempt == 1:
                return True, message
            return (
                True,
                f"{message}（第 {attempt}/{attempts} 次成功；此前失败：{failures[-1]}）",
            )

        failures.append(message)
        if not retryable or attempt >= attempts:
            break
        if retry_delay_s > 0:
            time.sleep(retry_delay_s)

    if len(failures) == 1:
        return False, failures[-1]
    recent = "; ".join(failures[-3:])
    return False, f"{failures[-1]}（连续 {len(failures)} 次失败；最近错误：{recent}）"


def opener_for_mixed_proxy(port: int) -> Any:
    proxy_url = f"http://127.0.0.1:{port}"
    return build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))


def probe_api_https_via_mixed_proxy(url: str, port: int, timeout_s: float) -> ProbeStep:
    opener = opener_for_mixed_proxy(port)
    request = Request(
        url,
        headers={
            "Authorization": "Bearer probe",
            "User-Agent": "clash-verge-monitor/1.0",
        },
    )
    started = time.perf_counter()
    try:
        with opener.open(request, timeout=timeout_s) as response:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            ok = 200 <= response.status < 400
            return ProbeStep(
                "API HTTPS",
                ok,
                f"HTTP {response.status}, {elapsed_ms} 毫秒",
            )
    except HTTPError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        ok = exc.code == 401
        suffix = "（鉴权挑战，API 域名可达）" if ok else ""
        return ProbeStep("API HTTPS", ok, f"HTTP {exc.code}, {elapsed_ms} 毫秒{suffix}")
    except (OSError, URLError) as exc:
        return ProbeStep("API HTTPS", False, str(exc))


def openai_probe_key(args: argparse.Namespace, strict: bool) -> str:
    if not strict:
        return "probe"
    return os.getenv(args.openai_api_key_env, "")


def probe_sse_via_mixed_proxy(args: argparse.Namespace, port: int, timeout_s: float) -> ProbeStep:
    strict = args.strict_stream_probes
    api_key = openai_probe_key(args, strict)
    if strict and not api_key:
        return ProbeStep(
            "SSE",
            False,
            f"缺少 {args.openai_api_key_env}，无法验证真实 SSE 首事件",
        )

    opener = opener_for_mixed_proxy(port)
    payload = json.dumps(
        {
            "model": args.openai_stream_model,
            "input": "ping",
            "stream": True,
            "store": False,
            "max_output_tokens": 1,
        }
    ).encode("utf-8")
    request = Request(
        args.openai_sse_url,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "User-Agent": "clash-verge-monitor/1.0",
        },
        method="POST",
    )
    started = time.perf_counter()
    try:
        with opener.open(request, timeout=timeout_s) as response:
            for _ in range(32):
                line = response.readline(4096)
                if not line:
                    break
                if line.startswith((b"event:", b"data:")):
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    return ProbeStep("SSE", True, f"首事件 {elapsed_ms} 毫秒")
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return ProbeStep("SSE", False, f"HTTP {response.status} 但未收到 SSE 事件，{elapsed_ms} 毫秒")
    except HTTPError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        if not strict and exc.code in {400, 401, 403}:
            return ProbeStep(
                "SSE",
                True,
                f"预检 HTTP {exc.code}, {elapsed_ms} 毫秒（端点可达，未验证真实流）",
            )
        return ProbeStep("SSE", False, f"HTTP {exc.code}, {elapsed_ms} 毫秒")
    except (OSError, URLError) as exc:
        return ProbeStep("SSE", False, str(exc))


def read_http_header(sock: socket.socket, max_bytes: int = 65536) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total < max_bytes:
        chunk = sock.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if b"\r\n\r\n" in b"".join(chunks):
            break
    return b"".join(chunks)


def parse_http_status(header_bytes: bytes) -> tuple[int | None, str]:
    text = header_bytes.decode("iso-8859-1", errors="replace")
    first_line = text.splitlines()[0] if text.splitlines() else ""
    parts = first_line.split(None, 2)
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1]), first_line
    return None, first_line or "empty response"


def websocket_status_via_mixed_proxy(
    url: str,
    port: int,
    timeout_s: float,
    headers: dict[str, str],
) -> tuple[int | None, str, int]:
    parsed = urlparse(url)
    if parsed.scheme != "wss" or not parsed.hostname:
        raise ValueError(f"只支持 wss:// WebSocket 探测地址：{url}")

    host = parsed.hostname
    target_port = parsed.port or 443
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    started = time.perf_counter()
    raw_sock: socket.socket | None = socket.create_connection(
        ("127.0.0.1", port),
        timeout=timeout_s,
    )
    try:
        raw_sock.settimeout(timeout_s)
        connect_request = (
            f"CONNECT {host}:{target_port} HTTP/1.1\r\n"
            f"Host: {host}:{target_port}\r\n"
            "Proxy-Connection: keep-alive\r\n"
            "\r\n"
        ).encode("ascii")
        raw_sock.sendall(connect_request)
        connect_header = read_http_header(raw_sock)
        connect_status, connect_line = parse_http_status(connect_header)
        if connect_status != 200:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return connect_status, f"CONNECT {connect_line}", elapsed_ms

        context = ssl.create_default_context()
        tls_sock = context.wrap_socket(raw_sock, server_hostname=host)
        raw_sock = None
        try:
            tls_sock.settimeout(timeout_s)
            ws_key = base64.b64encode(os.urandom(16)).decode("ascii")
            lines = [
                f"GET {path} HTTP/1.1",
                f"Host: {host}",
                "Upgrade: websocket",
                "Connection: Upgrade",
                f"Sec-WebSocket-Key: {ws_key}",
                "Sec-WebSocket-Version: 13",
                "User-Agent: clash-verge-monitor/1.0",
            ]
            for key, value in headers.items():
                if value:
                    lines.append(f"{key}: {value}")
            lines.extend(["", ""])
            tls_sock.sendall("\r\n".join(lines).encode("ascii"))
            response_header = read_http_header(tls_sock)
        finally:
            tls_sock.close()
    finally:
        if raw_sock is not None:
            raw_sock.close()

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    status, line = parse_http_status(response_header)
    return status, line, elapsed_ms


def probe_websocket_via_mixed_proxy(args: argparse.Namespace, port: int, timeout_s: float) -> ProbeStep:
    strict = args.strict_stream_probes
    api_key = openai_probe_key(args, strict)
    if strict and not api_key:
        return ProbeStep(
            "WS",
            False,
            f"缺少 {args.openai_api_key_env}，无法验证 WebSocket 101 握手",
        )

    try:
        status, line, elapsed_ms = websocket_status_via_mixed_proxy(
            args.openai_websocket_url,
            port,
            timeout_s,
            {"Authorization": f"Bearer {api_key}"},
        )
    except Exception as exc:
        return ProbeStep("WS", False, str(exc))

    if status == 101:
        return ProbeStep("WS", True, f"101 Switching Protocols, {elapsed_ms} 毫秒")
    if not strict and status in {400, 401, 403}:
        return ProbeStep(
            "WS",
            True,
            f"预检 HTTP {status}, {elapsed_ms} 毫秒（端点可达，未验证 101）",
        )
    return ProbeStep("WS", False, f"{line}, {elapsed_ms} 毫秒")


def format_probe_steps(steps: list[ProbeStep]) -> str:
    parts: list[str] = []
    for step in steps:
        message = zh_error(step.message)
        parts.append(message if step.name == "HTTP" else f"{step.name} {message}")
    return "; ".join(parts)


def comprehensive_route_check(
    args: argparse.Namespace,
    port: int,
    attempts: int | None = None,
) -> tuple[bool, str]:
    timeout_s = max(args.timeout / 1000 + 2, 3)
    http_ok, http_message = check_via_mixed_proxy(
        args.url,
        port,
        timeout_s=timeout_s,
        attempts=attempts if attempts is not None else args.route_retries,
        retry_delay_s=args.route_retry_delay,
    )
    steps = [ProbeStep("HTTP", http_ok, http_message)]
    if not http_ok or args.quick_route_only:
        return http_ok, format_probe_steps(steps)

    if not args.skip_api_probe:
        steps.append(probe_api_https_via_mixed_proxy(args.openai_api_url, port, timeout_s))
    if not args.skip_sse_probe:
        steps.append(probe_sse_via_mixed_proxy(args, port, timeout_s))
    if not args.skip_websocket_probe:
        steps.append(probe_websocket_via_mixed_proxy(args, port, timeout_s))

    return all(step.ok for step in steps), format_probe_steps(steps)


def deep_probe_delay_limit(args: argparse.Namespace) -> int:
    return args.deep_probe_fast_ms if args.deep_probe_fast_ms > 0 else args.slow


def sorted_fast_switch_candidates(
    results: list[DelayResult],
    args: argparse.Namespace,
    known_targets: dict[str, NodeTarget] | None = None,
) -> list[DelayResult]:
    usable = [
        result
        for result in results
        if result.ok
        and result.delay_ms is not None
        and (not known_targets or result.api_name in known_targets)
    ]
    usable.sort(key=lambda result: (result.delay_ms or 10**9, result.subscription, result.name))
    fast_limit = deep_probe_delay_limit(args)
    fast = [result for result in usable if (result.delay_ms or 10**9) <= fast_limit]
    if args.deep_probe_max_candidates > 0:
        fast = fast[: args.deep_probe_max_candidates]
    return fast


def short(text: str, limit: int) -> str:
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def green(text: str) -> str:
    if not sys.stdout.isatty():
        return text
    return f"{ANSI_GREEN}{text}{ANSI_RESET}"


def red(text: str) -> str:
    if not sys.stdout.isatty():
        return text
    return f"{ANSI_RED}{text}{ANSI_RESET}"


def zh_error(text: str) -> str:
    replacements = {
        "Timeout": "超时",
        "timed out": "超时",
        "An error occurred in the delay test": "延迟测试失败",
        "Connection refused": "连接被拒绝",
        "EOF occurred in violation of protocol": "协议异常断开",
        "unknown": "未知",
        "no delay": "没有返回延迟",
    }
    result = str(text)
    for source, target in replacements.items():
        result = result.replace(source, target)
    return result


def default_log_path() -> Path:
    return DEFAULT_LOG_DIR / f"clash-monitor-{datetime.now().strftime('%Y%m%d')}.log"


def setup_log_file(args: argparse.Namespace) -> Path | None:
    global LOG_FILE

    if args.no_log_file:
        return None

    path = Path(args.log_file).expanduser() if args.log_file else default_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE = path.open("a", encoding="utf-8")
    LOG_FILE.write("\n" + "=" * 88 + "\n")
    LOG_FILE.write(f"会话开始：{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    LOG_FILE.flush()
    return path


def close_log_file() -> None:
    global LOG_FILE

    if LOG_FILE is not None:
        LOG_FILE.write(f"会话结束：{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        LOG_FILE.flush()
        LOG_FILE.close()
        LOG_FILE = None


def format_delay(result: DelayResult, slow_ms: int) -> tuple[str, str]:
    if not result.ok:
        return "失败", "-"
    if result.delay_ms is None:
        return "可用", "-"
    if result.delay_ms >= slow_ms:
        return "偏慢", f"{result.delay_ms} 毫秒"
    return "可用", f"{result.delay_ms} 毫秒"


def run_delay_checks(
    controller: Controller,
    targets: list[NodeTarget],
    args: argparse.Namespace,
) -> list[DelayResult]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(test_delay, controller, target, args.url, args.timeout)
            for target in targets
        ]
        return [future.result() for future in concurrent.futures.as_completed(futures)]


def matches_regex(pattern: str, text: str) -> bool:
    if not pattern:
        return False
    try:
        return bool(re.search(pattern, text, re.IGNORECASE))
    except re.error:
        return False


def is_switch_candidate(target: NodeTarget, args: argparse.Namespace) -> bool:
    searchable = f"{target.subscription} {target.name}"
    if matches_regex(args.exclude_regex, searchable):
        return False
    if matches_regex(args.skip_candidate_regex, searchable):
        return False
    return True


def filter_excluded_targets(
    targets: list[NodeTarget],
    args: argparse.Namespace,
    label: str = "节点",
) -> list[NodeTarget]:
    if not args.exclude_regex:
        return targets

    kept: list[NodeTarget] = []
    excluded_count = 0
    for target in targets:
        searchable = f"{target.subscription} {target.name}"
        if matches_regex(args.exclude_regex, searchable):
            excluded_count += 1
            continue
        kept.append(target)

    if excluded_count:
        log(f"{label}过滤：已排除 {excluded_count} 个香港节点。")
    return kept


def requested_switch_groups(args: argparse.Namespace) -> list[str]:
    if not args.switch_group:
        return []
    return [item.strip() for item in args.switch_group.split(",") if item.strip()]


def switchable_groups(
    proxies: dict[str, Any],
    node_name: str,
    args: argparse.Namespace,
) -> list[str]:
    requested = requested_switch_groups(args)
    groups: list[str] = []

    for group_name, proxy in proxies.items():
        if not isinstance(proxy, dict):
            continue
        all_names = proxy.get("all")
        if not isinstance(all_names, list) or node_name not in all_names:
            continue
        if requested and group_name not in requested:
            continue
        proxy_type = str(proxy.get("type", ""))
        if not requested and proxy_type != "Selector":
            continue
        if group_name == "GLOBAL":
            continue
        groups.append(group_name)

    if requested:
        requested_set = set(requested)
        groups.sort(key=lambda name: requested.index(name) if name in requested_set else len(requested))
    return groups


def switch_group(controller: Controller, group_name: str, node_name: str) -> None:
    api_json(
        controller,
        f"/proxies/{quote(group_name, safe='')}",
        timeout=5.0,
        method="PUT",
        payload={"name": node_name},
    )


def controller_http_value(controller: Controller) -> str:
    if not controller.base_url:
        return ""
    parsed = urlparse(controller.base_url)
    host = parsed.hostname or "127.0.0.1"
    if parsed.port:
        return f"{host}:{parsed.port}"
    return host


def current_unix_socket(hints: list[ConfigHint], controller: Controller) -> str:
    if controller.unix_socket:
        return controller.unix_socket
    for hint in hints:
        if hint.unix_socket:
            return hint.unix_socket
    return ""


def build_runtime_switch_config(
    profile_path: Path,
    configs: dict[str, Any],
    controller: Controller,
    hints: list[ConfigHint],
) -> dict[str, Any]:
    data = load_yaml_data(profile_path)
    if not isinstance(data, dict):
        raise ProfileError(f"订阅配置不是有效对象：{profile_path}")

    switch_config = dict(data)

    for key in ("mixed-port", "port", "socks-port", "redir-port", "tproxy-port"):
        value = get_config_value(configs, key)
        if value is not None:
            switch_config[key] = value

    for key in (
        "allow-lan",
        "bind-address",
        "mode",
        "ipv6",
        "tun",
        "unified-delay",
        "tcp-concurrent",
        "global-client-fingerprint",
        "find-process-mode",
    ):
        value = get_config_value(configs, key)
        if value is not None:
            switch_config[key] = value

    controller_value = controller_http_value(controller)
    if controller_value:
        switch_config["external-controller"] = controller_value
    unix_socket = current_unix_socket(hints, controller)
    if unix_socket:
        switch_config["external-controller-unix"] = unix_socket

    return switch_config


def load_runtime_config(controller: Controller, path: Path) -> None:
    api_json(
        controller,
        "/configs",
        timeout=8.0,
        method="PUT",
        payload={"path": str(path), "force": True},
    )


def switch_to_full_subscription_node(
    controller: Controller,
    configs: dict[str, Any],
    mixed_port: int | None,
    results: list[DelayResult],
    all_profiles: AllProfilesRuntime,
    hints: list[ConfigHint],
    args: argparse.Namespace,
) -> SwitchOutcome:
    if args.no_auto_switch or args.current_only:
        return SwitchOutcome()

    usable = [
        result
        for result in results
        if result.ok
        and result.delay_ms is not None
        and result.api_name in all_profiles.targets
        and is_switch_candidate(all_profiles.targets[result.api_name], args)
    ]
    if not usable:
        return SwitchOutcome()

    candidates = sorted_fast_switch_candidates(usable, args, all_profiles.targets)
    if not candidates:
        log(
            f"  跨订阅切换：有 {len(usable)} 个基础连通节点，但没有节点快于 "
            f"{deep_probe_delay_limit(args)} 毫秒，本轮不做综合探测"
        )
        return SwitchOutcome()

    if len(candidates) < len(usable):
        log(
            f"  跨订阅切换：仅对最快 {len(candidates)}/{len(usable)} 个候选做综合探测"
        )

    for result in candidates:
        target = all_profiles.targets[result.api_name]
        if not target.profile_path:
            continue

        profile_path = Path(target.profile_path)
        if not profile_path.exists():
            log(f"  跨订阅切换：订阅文件不存在，跳过：{profile_path}")
            continue

        if args.dry_run_switch:
            log(
                f"  跨订阅切换：将综合探测订阅「{target.subscription}」"
                f"节点「{target.name}」({result.delay_ms} 毫秒)"
            )
            return SwitchOutcome()

        try:
            switch_group(all_profiles.controller, "ALL_SUBSCRIPTIONS", result.api_name)
        except ApiError as exc:
            log(f"  跨订阅切换：临时选择候选节点失败，跳过：{zh_error(str(exc))}")
            continue
        if args.deep_probe_settle > 0:
            time.sleep(args.deep_probe_settle)
        probe_ok, probe_message = comprehensive_route_check(
            args,
            all_profiles.mixed_port,
            attempts=1,
        )
        probe_line = (
            f"  候选综合探测：{target.subscription} / {target.name} "
            f"({'通过' if probe_ok else '失败'}) - {zh_error(probe_message)}"
        )
        log(green(probe_line) if probe_ok else red(probe_line))
        if not probe_ok:
            continue

        action = "将切换" if args.dry_run_switch else "已切换"
        log(green(
            f"  跨订阅切换：{action}到订阅「{target.subscription}」"
            f"节点「{target.name}」({result.delay_ms} 毫秒)"
        )
        )

        switch_config = build_runtime_switch_config(profile_path, configs, controller, hints)
        switch_path = CLASH_VERGE_DIR / "clash-monitor-switch.yaml"
        switch_path.write_text(json.dumps(switch_config, ensure_ascii=False, indent=2), encoding="utf-8")

        load_runtime_config(controller, switch_path)
        time.sleep(args.switch_settle)

        loaded_proxies_data = api_json(controller, "/proxies", timeout=8.0)
        loaded_proxies = (
            loaded_proxies_data.get("proxies", {}) if isinstance(loaded_proxies_data, dict) else {}
        )
        if not isinstance(loaded_proxies, dict):
            loaded_proxies = {}

        groups = switchable_groups(loaded_proxies, target.name, args)
        switched_groups: list[str] = []
        for group in groups:
            try:
                switch_group(controller, group, target.name)
                switched_groups.append(group)
            except ApiError:
                pass

        if switched_groups:
            log(green(f"  跨订阅切换：已设置策略组 {', '.join(switched_groups)} => {target.name}"))
        else:
            log("  跨订阅切换：已加载订阅，但没有找到可直接设置该节点的 selector 策略组")

        if mixed_port:
            time.sleep(args.switch_settle)
            ok, message = comprehensive_route_check(args, mixed_port)
            log(f"  跨订阅切换复查：{'可用' if ok else '失败'} - {zh_error(message)}")
            if ok:
                return SwitchOutcome(route_ok=True, source_type=target.source_type)
            continue
        return SwitchOutcome(source_type=target.source_type)

    log("  跨订阅切换：候选节点基础连通可用，但综合探测或切换复查均未通过")
    return SwitchOutcome(route_ok=False)


def auto_switch_if_needed(
    controller: Controller,
    proxies: dict[str, Any],
    mixed_port: int | None,
    route_ok: bool | None,
    args: argparse.Namespace,
) -> bool | None:
    if args.no_auto_switch:
        return None
    if route_ok is not False:
        return None

    log("  自动切换：当前 ChatGPT 路由不可用，正在查找可连通 ChatGPT 的节点...")
    targets = [
        target
        for target in collect_targets(proxies, include_groups=False)
        if is_switch_candidate(target, args)
    ]
    if not targets:
        log("  自动切换：当前已加载配置中，没有找到可切换的候选节点")
        return None

    results = run_delay_checks(controller, targets, args)
    usable = [result for result in results if result.ok and result.delay_ms is not None]
    if not usable:
        log("  自动切换：当前已加载配置中，没有可切换且可连通 ChatGPT 的节点")
        return None

    candidates = sorted_fast_switch_candidates(usable, args)
    if not candidates:
        log(
            f"  自动切换：有 {len(usable)} 个基础连通节点，但没有节点快于 "
            f"{deep_probe_delay_limit(args)} 毫秒，本轮不做综合探测"
        )
        return None
    if len(candidates) < len(usable):
        log(f"  自动切换：仅对最快 {len(candidates)}/{len(usable)} 个候选做综合探测")

    for result in candidates:
        groups = switchable_groups(proxies, result.api_name, args)
        if not groups:
            continue
        switched: list[str] = []
        failed: list[str] = []
        previous_by_group: dict[str, str] = {}
        for group in groups:
            try:
                previous = proxies.get(group, {}).get("now")
                if isinstance(previous, str) and previous:
                    previous_by_group[group] = previous
                if not args.dry_run_switch:
                    switch_group(controller, group, result.api_name)
                switched.append(group)
            except ApiError:
                failed.append(group)

        if switched:
            action = "将切换" if args.dry_run_switch else "已切换"
            message = (
                f"  自动切换：{action} {', '.join(switched)} "
                f"=> {result.name} ({result.delay_ms} 毫秒)"
            )
            log(message if args.dry_run_switch else green(message))
            if failed:
                log(f"  自动切换：以下策略组切换失败：{', '.join(failed)}")
            if args.dry_run_switch:
                return None
            if mixed_port and not args.dry_run_switch:
                if args.deep_probe_settle > 0:
                    time.sleep(args.deep_probe_settle)
                ok, message = comprehensive_route_check(args, mixed_port)
                log(f"  自动切换复查：{'可用' if ok else '失败'} - {zh_error(message)}")
                if ok:
                    return True
                if previous_by_group:
                    reverted: list[str] = []
                    for group, previous in previous_by_group.items():
                        try:
                            switch_group(controller, group, previous)
                            reverted.append(f"{group}=>{previous}")
                        except ApiError:
                            pass
                    if reverted:
                        log(f"  自动切换：复查失败，已回滚：{'; '.join(reverted)}")
                continue
            return None

    log("  自动切换：候选节点基础连通可用，但没有节点通过综合探测")
    return False


def print_results(
    results: list[DelayResult],
    args: argparse.Namespace,
) -> None:
    slow_ms = args.slow
    successful = [item for item in results if item.ok]
    failed = [item for item in results if not item.ok]
    slow = [item for item in successful if (item.delay_ms or 0) >= slow_ms]

    log(
        f"节点汇总：总数={len(results)} 可用={len(successful)} "
        f"偏慢={len(slow)} 失败={len(failed)}"
    )

    by_subscription: dict[str, list[DelayResult]] = {}
    for result in results:
        by_subscription.setdefault(result.subscription or "当前配置", []).append(result)

    log("订阅汇总：")
    for subscription in sorted(by_subscription):
        items = by_subscription[subscription]
        ok_items = [item for item in items if item.ok]
        ok_count = len(ok_items)
        slow_count = sum(1 for item in ok_items if (item.delay_ms or 0) >= slow_ms)
        best = min(
            ok_items,
            key=lambda item: item.delay_ms if item.delay_ms is not None else 10**9,
            default=None,
        )
        best_text = f"最快={short(best.name, 26)} {best.delay_ms} 毫秒" if best else "最快=-"
        log(
            f"  {short(subscription, 24):<24} "
            f"可用={ok_count}/{len(items)} 偏慢={slow_count} 失败={len(items) - ok_count} "
            f"{best_text}"
        )

    fastest = sorted(
        [item for item in successful if item.delay_ms is not None],
        key=lambda item: (item.delay_ms or 10**9, item.subscription.lower(), item.name.lower()),
    )[: args.summary_best]
    if fastest:
        log("最快节点：")
        for index, item in enumerate(fastest, start=1):
            status, delay = format_delay(item, slow_ms)
            log(
                f"  {index}. {delay:<9} {status:<4} "
                f"{short(item.subscription, 14)} / {short(item.name, 44)}"
            )
    else:
        log("最快节点：无")

    if failed:
        reasons: dict[str, int] = {}
        for item in failed:
            reason = short(zh_error(item.error or "未知"), 42)
            reasons[reason] = reasons.get(reason, 0) + 1
        reason_text = ", ".join(
            f"{reason} x{count}"
            for reason, count in sorted(reasons.items(), key=lambda pair: (-pair[1], pair[0]))[:3]
        )
        log(f"失败原因：{reason_text}")

    if not args.show_nodes:
        return

    log("-" * 112)
    log(f"{'状态':<7} {'延迟':>9} {'订阅':<18} {'类型':<12} 节点")
    log("-" * 112)

    sorted_results = sorted(
        results,
        key=lambda item: (
            not item.ok,
            item.subscription.lower(),
            item.delay_ms if item.delay_ms is not None else 10**9,
            item.name.lower(),
        ),
    )
    if args.limit > 0:
        sorted_results = sorted_results[: args.limit]

    for result in sorted_results:
        status, delay = format_delay(result, slow_ms)
        subscription = short(result.subscription or "当前配置", 18)
        name = short(result.name, 56)
        proxy_type = short(result.proxy_type, 12)
        log(f"{status:<7} {delay:>9} {subscription:<18} {proxy_type:<12} {name}")
        if args.show_errors and result.error:
            log(f"{'':<7} {'':>9} {'':<18} {'':<12} 错误：{short(zh_error(result.error), 70)}")

    if args.limit > 0 and len(results) > args.limit:
        log(f"... 还有 {len(results) - args.limit} 个节点被 --limit 隐藏")


def load_free_stats() -> dict[str, Any]:
    if not DEFAULT_FREE_STATS_PATH.exists():
        return {}
    try:
        data = json.loads(DEFAULT_FREE_STATS_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_free_stats(stats: dict[str, Any]) -> None:
    DEFAULT_FREE_STATS_PATH.write_text(
        json.dumps(stats, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def update_free_stats(results: list[DelayResult], runtime: AllProfilesRuntime, args: argparse.Namespace) -> None:
    by_url: dict[str, list[DelayResult]] = {}
    for result in results:
        target = runtime.targets.get(result.api_name)
        if not target or target.source_type != "free":
            continue
        by_url.setdefault(target.source_url or target.subscription, []).append(result)

    if not by_url:
        return

    stats = load_free_stats()
    log("免费源统计：")
    now = time.strftime("%Y-%m-%d %H:%M:%S")

    for url, items in by_url.items():
        source_stats = stats.setdefault(
            url,
            {
                "checks": 0,
                "success_checks": 0,
                "fail_checks": 0,
                "consecutive_failures": 0,
                "total_success_nodes": 0,
                "last_success_nodes": 0,
                "last_total_nodes": 0,
                "last_checked": "",
                "last_success": "",
            },
        )
        ok_count = sum(1 for item in items if item.ok)
        source_stats["checks"] = int(source_stats.get("checks", 0)) + 1
        source_stats["last_checked"] = now
        source_stats["last_success_nodes"] = ok_count
        source_stats["last_total_nodes"] = len(items)
        source_stats["total_success_nodes"] = int(source_stats.get("total_success_nodes", 0)) + ok_count
        if ok_count:
            source_stats["success_checks"] = int(source_stats.get("success_checks", 0)) + 1
            source_stats["consecutive_failures"] = 0
            source_stats["last_success"] = now
        else:
            source_stats["fail_checks"] = int(source_stats.get("fail_checks", 0)) + 1
            source_stats["consecutive_failures"] = int(source_stats.get("consecutive_failures", 0)) + 1

        checks = max(1, int(source_stats.get("checks", 1)))
        success_checks = int(source_stats.get("success_checks", 0))
        consecutive_failures = int(source_stats.get("consecutive_failures", 0))
        success_rate = success_checks / checks
        label = short(url, 54)
        log(
            f"  {label} 本次可用={ok_count}/{len(items)} "
            f"历史可用轮次={success_checks}/{checks} 连续失败={consecutive_failures}"
        )
        if consecutive_failures >= args.free_abandon_threshold or (
            checks >= args.free_abandon_threshold and success_rate < 0.2
        ):
            log("    提醒：该免费源近期大量未连通，可考虑移除或降低优先级。")

    save_free_stats(stats)
    log(f"免费源统计文件：{DEFAULT_FREE_STATS_PATH}")


def ensure_profiles_runtime(
    args: argparse.Namespace,
    runtime: AllProfilesRuntime | None,
    profiles: list[RemoteProfile],
    label: str,
) -> AllProfilesRuntime:
    if runtime is not None:
        return runtime
    return start_all_profiles_runtime(args, profiles=profiles, label=label)


def test_profiles_runtime(
    runtime: AllProfilesRuntime,
    args: argparse.Namespace,
) -> list[DelayResult]:
    node_proxies_data = api_json(runtime.controller, "/proxies", timeout=8.0)
    node_proxies = node_proxies_data.get("proxies", {}) if isinstance(node_proxies_data, dict) else {}
    if not isinstance(node_proxies, dict):
        node_proxies = {}
    targets = collect_targets(
        node_proxies,
        include_groups=args.include_groups,
        metadata=runtime.targets,
    )
    targets = filter_excluded_targets(targets, args, runtime.label)
    if not targets:
        log("没有在 /proxies 中找到可测试节点。")
        return []
    return run_delay_checks(runtime.controller, targets, args)


def print_header(
    controller: Controller,
    version: Any,
    configs: dict[str, Any],
    mixed_port: int | None,
    test_url: str,
) -> None:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    mode = get_config_value(configs, "mode") or "未知"

    log("=" * 88)
    log(f"{now} | ChatGPT 监控 | 模式={mode}")


def run_once(
    controller: Controller,
    version: Any,
    args: argparse.Namespace,
    hints: list[ConfigHint],
    owned_profiles: AllProfilesRuntime | None = None,
    free_profiles: AllProfilesRuntime | None = None,
    active_source_type: str = "owned",
) -> RunOutcome:
    configs_data = api_json(controller, "/configs", timeout=5.0)
    configs = configs_data if isinstance(configs_data, dict) else {}
    proxies_data = api_json(controller, "/proxies", timeout=8.0)
    proxies = proxies_data.get("proxies", {}) if isinstance(proxies_data, dict) else {}
    if not isinstance(proxies, dict):
        proxies = {}

    mixed_port = mixed_port_from(args, configs, hints)
    print_header(controller, version, configs, mixed_port, args.url)
    route_ok: bool | None = None
    if mixed_port:
        route_ok, message = comprehensive_route_check(args, mixed_port)
        route_line = f"ChatGPT 路由：{'可用' if route_ok else '失败'} - {zh_error(message)}"
        log(route_line if route_ok else red(route_line))
    else:
        log("ChatGPT 路由：跳过 - 未找到 mixed-port；需要时可传 --proxy-port")

    rules = fetch_rules(controller)
    route_chain = current_route_chain(proxies, configs, rules, args.url)
    if any(part.startswith("免费源") for part in route_chain):
        active_source_type = "free"
    log(f"当前路径：{format_route_chain(route_chain)}")
    switched_route_ok = auto_switch_if_needed(controller, proxies, mixed_port, route_ok, args)
    if switched_route_ok is not None:
        route_ok = switched_route_ok
    log()

    if route_ok is True and not args.always_test_nodes:
        if active_source_type == "free" and not args.current_only:
            log("当前使用免费备用节点；开始检查自有订阅是否恢复...")
            owned_profiles = ensure_profiles_runtime(
                args,
                owned_profiles,
                discover_remote_profiles(args),
                "自有订阅",
            )
            owned_results = test_profiles_runtime(owned_profiles, args)
            print_results(owned_results, args)
            if any(result.ok for result in owned_results):
                switch_outcome = switch_to_full_subscription_node(
                    controller,
                    configs,
                    mixed_port,
                    owned_results,
                    owned_profiles,
                    hints,
                    args,
                )
                if switch_outcome.route_ok is True:
                    active_source_type = "owned"
                    if free_profiles is not None:
                        free_profiles.close()
                        free_profiles = None
                    log(green("自有订阅已恢复，已优先切回自有节点。"))
                    return RunOutcome(
                        owned_profiles=owned_profiles,
                        free_profiles=free_profiles,
                        active_source_type=active_source_type,
                        no_available_chatgpt_node=False,
                    )
            log("自有订阅暂未恢复，继续使用免费备用节点，本轮跳过免费池全量测试。")
            return RunOutcome(
                owned_profiles=owned_profiles,
                free_profiles=free_profiles,
                active_source_type=active_source_type,
                no_available_chatgpt_node=False,
            )

        if owned_profiles is not None:
            owned_profiles.close()
            owned_profiles = None
        if free_profiles is not None:
            free_profiles.close()
            free_profiles = None
        log("当前 ChatGPT 路由可用，本轮跳过节点全量测试。")
        return RunOutcome(
            owned_profiles=owned_profiles,
            free_profiles=free_profiles,
            active_source_type=active_source_type,
            no_available_chatgpt_node=False,
        )

    if args.current_only:
        log("节点范围：仅当前已加载配置")
        targets = collect_targets(proxies, include_groups=args.include_groups)
        targets = filter_excluded_targets(targets, args, "当前配置")
        if not targets:
            log("没有在 /proxies 中找到可测试节点。")
            return RunOutcome(
                owned_profiles=owned_profiles,
                free_profiles=free_profiles,
                active_source_type=active_source_type,
                no_available_chatgpt_node=True,
            )
        results = run_delay_checks(controller, targets, args)
        print_results(results, args)
        return RunOutcome(
            owned_profiles=owned_profiles,
            free_profiles=free_profiles,
            active_source_type=active_source_type,
            no_available_chatgpt_node=not any(result.ok for result in results),
        )

    log("当前 ChatGPT 路由不可用，先检查自有全部订阅；如发现可用节点将优先切回/切到自有节点...")
    owned_profiles = ensure_profiles_runtime(
        args,
        owned_profiles,
        discover_remote_profiles(args),
        "自有订阅",
    )
    temp_version = owned_profiles.version.get("version") if isinstance(owned_profiles.version, dict) else owned_profiles.version
    log(
        "节点范围：自有全部订阅 "
        f"({owned_profiles.profile_count} 个订阅，{owned_profiles.node_count} 个节点) "
        f"通过 {owned_profiles.core_path.name} {temp_version or ''}".rstrip()
    )
    owned_results = test_profiles_runtime(owned_profiles, args)
    print_results(owned_results, args)
    owned_has_available = any(result.ok for result in owned_results)
    if owned_has_available:
        switch_outcome = switch_to_full_subscription_node(
            controller,
            configs,
            mixed_port,
            owned_results,
            owned_profiles,
            hints,
            args,
        )
        if switch_outcome.source_type:
            active_source_type = switch_outcome.source_type
        if switch_outcome.route_ok is True:
            return RunOutcome(
                owned_profiles=owned_profiles,
                free_profiles=free_profiles,
                active_source_type=active_source_type or "owned",
                no_available_chatgpt_node=False,
            )
        if switch_outcome.route_ok is False:
            log("  自有订阅切换：发现可用节点，但切换后当前 ChatGPT 路由仍不可用")

    if args.no_free_backup:
        log("免费备用已禁用，本轮不测试免费订阅。")
        return RunOutcome(
            owned_profiles=owned_profiles,
            free_profiles=free_profiles,
            active_source_type=active_source_type,
            no_available_chatgpt_node=not owned_has_available,
        )

    log("自有订阅暂未能恢复 ChatGPT，开始测试免费备用订阅...")
    free_profiles_list = discover_free_profiles(args)
    if not free_profiles_list:
        log("免费备用订阅不可用：没有可测试的免费源。")
        return RunOutcome(
            owned_profiles=owned_profiles,
            free_profiles=free_profiles,
            active_source_type=active_source_type,
            no_available_chatgpt_node=True,
        )

    free_profiles = ensure_profiles_runtime(args, free_profiles, free_profiles_list, "免费备用订阅")
    temp_version = free_profiles.version.get("version") if isinstance(free_profiles.version, dict) else free_profiles.version
    log(
        "节点范围：免费备用订阅 "
        f"({free_profiles.profile_count} 个订阅，{free_profiles.node_count} 个节点) "
        f"通过 {free_profiles.core_path.name} {temp_version or ''}".rstrip()
    )
    free_results = test_profiles_runtime(free_profiles, args)
    print_results(free_results, args)
    update_free_stats(free_results, free_profiles, args)
    free_has_available = any(result.ok for result in free_results)
    if free_has_available:
        switch_outcome = switch_to_full_subscription_node(
            controller,
            configs,
            mixed_port,
            free_results,
            free_profiles,
            hints,
            args,
        )
        if switch_outcome.source_type:
            active_source_type = switch_outcome.source_type
        if switch_outcome.route_ok is True:
            return RunOutcome(
                owned_profiles=owned_profiles,
                free_profiles=free_profiles,
                active_source_type=active_source_type,
                no_available_chatgpt_node=False,
            )
        if switch_outcome.route_ok is False:
            log("  免费备用切换：发现可用节点，但切换后当前 ChatGPT 路由仍不可用")

    return RunOutcome(
        owned_profiles=owned_profiles,
        free_profiles=free_profiles,
        active_source_type=active_source_type,
        no_available_chatgpt_node=not (owned_has_available or free_has_available),
    )


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def nonnegative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return number


def nonnegative_float(value: str) -> float:
    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return number


def wait_with_countdown(seconds: float, should_stop: Callable[[], bool]) -> None:
    if seconds <= 0:
        return

    log()
    if not sys.stdout.isatty():
        log(f"下次检查：{int(seconds)} 秒后。按 Ctrl+C 停止。")
        while seconds > 0 and not should_stop():
            step = min(1.0, seconds)
            time.sleep(step)
            seconds -= step
        return

    deadline = time.monotonic() + seconds
    last_displayed: int | None = None
    while not should_stop():
        remaining = max(0.0, deadline - time.monotonic())
        displayed = int(remaining + 0.999)
        if displayed != last_displayed:
            sys.stdout.write(
                f"\r下次检查：{displayed:>3} 秒后。按 Ctrl+C 停止。"
            )
            sys.stdout.flush()
            last_displayed = displayed
        if remaining <= 0:
            break
        time.sleep(min(0.2, remaining))

    sys.stdout.write("\r" + " " * 64 + "\r")
    sys.stdout.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="持续监控本地 Clash Verge / Mihomo 的 ChatGPT 连通性。"
    )
    parser.add_argument("--api", help="Clash API 地址，例如 http://127.0.0.1:9097")
    parser.add_argument("--unix-socket", help="Clash API Unix socket 路径")
    parser.add_argument("--secret", help="Clash API 密钥。也可使用环境变量 CLASH_SECRET")
    parser.add_argument("--core", help="用于检查全部订阅的 mihomo/clash 核心程序路径")
    parser.add_argument("--profiles-yaml", help="Clash Verge profiles.yaml 路径")
    parser.add_argument("--proxy-port", type=positive_int, help="用于当前路由探测的 mixed-port")
    parser.add_argument("--interval", type=positive_int, default=60, help="检查间隔秒数")
    parser.add_argument("--retry-interval", type=positive_int, default=30, help="没有可用 ChatGPT 节点时的快速复查间隔秒数")
    parser.add_argument("--timeout", type=positive_int, default=5000, help="节点延迟测试超时时间，单位毫秒")
    parser.add_argument(
        "--route-retries",
        type=positive_int,
        default=3,
        help="当前 ChatGPT 路由探测最多尝试次数，避免偶发 TLS/超时抖动触发切换",
    )
    parser.add_argument(
        "--route-retry-delay",
        type=nonnegative_float,
        default=1.0,
        help="当前路由探测失败后重试前等待秒数",
    )
    parser.add_argument(
        "--startup-timeout",
        type=positive_int,
        default=15,
        help="等待临时全订阅核心启动的秒数",
    )
    parser.add_argument("--url", default=DEFAULT_TEST_URL, help="连通性测试地址")
    parser.add_argument("--openai-api-url", default=DEFAULT_OPENAI_API_URL, help="OpenAI API HTTPS 预检地址")
    parser.add_argument("--openai-sse-url", default=DEFAULT_OPENAI_SSE_URL, help="OpenAI SSE 预检/严格探测地址")
    parser.add_argument("--openai-websocket-url", default=DEFAULT_OPENAI_WEBSOCKET_URL, help="OpenAI WebSocket 预检/严格探测地址")
    parser.add_argument(
        "--openai-stream-model",
        default=os.getenv("OPENAI_STREAM_TEST_MODEL", DEFAULT_OPENAI_STREAM_MODEL),
        help="严格 SSE 探测使用的模型；也可用 OPENAI_STREAM_TEST_MODEL 设置",
    )
    parser.add_argument(
        "--openai-api-key-env",
        default="OPENAI_API_KEY",
        help="严格 SSE/WebSocket 探测读取的 API Key 环境变量名",
    )
    parser.add_argument(
        "--strict-stream-probes",
        action="store_true",
        help="要求真实 SSE 首事件和 WebSocket 101；需要设置对应 API Key，可能产生极少量 API 调用",
    )
    parser.add_argument("--quick-route-only", action="store_true", help="只做原 HTTP 路由探测，跳过 API/SSE/WebSocket 综合探测")
    parser.add_argument("--skip-api-probe", action="store_true", help="综合探测时跳过 OpenAI API HTTPS 预检")
    parser.add_argument("--skip-sse-probe", action="store_true", help="综合探测时跳过 SSE 预检")
    parser.add_argument("--skip-websocket-probe", action="store_true", help="综合探测时跳过 WebSocket 预检")
    parser.add_argument(
        "--deep-probe-fast-ms",
        type=nonnegative_int,
        default=0,
        help="节点延迟不高于该毫秒数才进入综合探测；0 表示使用 --slow",
    )
    parser.add_argument(
        "--deep-probe-max-candidates",
        type=nonnegative_int,
        default=8,
        help="每轮最多对多少个最快候选节点做综合探测；0 表示不限制",
    )
    parser.add_argument(
        "--deep-probe-settle",
        type=nonnegative_float,
        default=0.5,
        help="临时切到候选节点后等待多少秒再做综合探测",
    )
    parser.add_argument("--workers", type=positive_int, default=16, help="并发节点测试数量")
    parser.add_argument("--slow", type=positive_int, default=1200, help="超过该毫秒数标记为偏慢")
    parser.add_argument(
        "--limit",
        type=nonnegative_int,
        default=20,
        help="启用 --show-nodes 时最多显示的节点行数；0 表示全部",
    )
    parser.add_argument(
        "--summary-best",
        type=positive_int,
        default=5,
        help="汇总中展示的最快可用节点数量",
    )
    parser.add_argument("--show-nodes", action="store_true", help="打印节点明细行")
    parser.add_argument("--group-limit", type=positive_int, default=20, help="最多展示的当前策略组数量")
    parser.add_argument("--include-groups", action="store_true", help="同时测试策略组")
    parser.add_argument(
        "--no-auto-switch",
        action="store_true",
        help="当前 ChatGPT 路由失败时不自动切换节点",
    )
    parser.add_argument(
        "--switch-group",
        help="要切换的策略组名称，多个用逗号分隔，例如 大哥云",
    )
    parser.add_argument(
        "--exclude-regex",
        default=DEFAULT_EXCLUDE_REGEX,
        help="测试和自动切换时排除节点的正则表达式；默认排除香港节点",
    )
    parser.add_argument(
        "--skip-candidate-regex",
        default=DEFAULT_SKIP_CANDIDATE_REGEX,
        help="自动切换时排除订阅信息节点的正则表达式",
    )
    parser.add_argument(
        "--switch-settle",
        type=positive_int,
        default=2,
        help="切换后等待几秒再复查",
    )
    parser.add_argument(
        "--dry-run-switch",
        action="store_true",
        help="只打印将要切换的目标，不实际修改 Clash",
    )
    parser.add_argument(
        "--current-only",
        action="store_true",
        help="只检查当前已加载的 Clash 配置",
    )
    parser.add_argument("--show-errors", action="store_true", help="打印失败错误明细")
    parser.add_argument("--always-test-nodes", action="store_true", help="即使当前 ChatGPT 路由可用，也强制测试节点")
    parser.add_argument("--free-url", action="append", help="额外免费订阅地址，可重复传入")
    parser.add_argument("--no-free-backup", action="store_true", help="禁用免费备用订阅")
    parser.add_argument("--free-cache-ttl", type=positive_int, default=1800, help="免费订阅缓存有效期秒数，默认 1800 秒")
    parser.add_argument("--free-fetch-timeout", type=positive_int, default=30, help="抓取免费订阅的超时时间秒数")
    parser.add_argument("--free-abandon-threshold", type=positive_int, default=5, help="免费源连续失败提醒阈值")
    parser.add_argument("--clear", action="store_true", help="每次刷新前清屏")
    parser.add_argument("--once", action="store_true", help="只检查一次后退出")
    parser.add_argument("--log-file", help="日志文件路径；默认写入当前目录")
    parser.add_argument("--no-log-file", action="store_true", help="不写入日志文件")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        log_path = setup_log_file(args)
    except OSError as exc:
        log(f"错误：无法打开日志文件：{exc}", file=sys.stderr)
        return 4
    if log_path:
        log(f"日志文件：{log_path}")

    hints = [hint for path in config_paths() if (hint := read_config_hint(path))]

    try:
        controller, version, _errors = find_controller(args, hints)
    except ApiError as exc:
        log(f"错误：无法连接 Clash 控制接口：{zh_error(str(exc))}", file=sys.stderr)
        close_log_file()
        return 2

    owned_profiles: AllProfilesRuntime | None = None
    free_profiles: AllProfilesRuntime | None = None
    active_source_type = "owned"

    stop = False

    def handle_signal(_signum: int, _frame: Any) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while not stop:
            started = time.monotonic()
            fast_retry = False
            if args.clear:
                sys.stdout.write("\033[2J\033[H")
                sys.stdout.flush()
            try:
                outcome = run_once(
                    controller,
                    version,
                    args,
                    hints,
                    owned_profiles=owned_profiles,
                    free_profiles=free_profiles,
                    active_source_type=active_source_type,
                )
                owned_profiles = outcome.owned_profiles
                free_profiles = outcome.free_profiles
                active_source_type = outcome.active_source_type or active_source_type
                fast_retry = outcome.no_available_chatgpt_node
            except ApiError as exc:
                log("=" * 88)
                log(f"控制接口错误：{zh_error(str(exc))}")
                log("将在下个检查周期重试。")
            except ProfileError as exc:
                log("=" * 88)
                log(f"全订阅检查错误：{zh_error(str(exc))}")
                log("将在下个检查周期重试。")
                fast_retry = True
            except Exception as exc:  # Keep the monitor alive for transient local issues.
                log("=" * 88)
                log(f"意外错误：{zh_error(str(exc))}")
                log("将在下个检查周期重试。")

            if args.once:
                break

            elapsed = time.monotonic() - started
            next_interval = args.retry_interval if fast_retry else args.interval
            if fast_retry:
                log(f"未发现可用 ChatGPT 节点，启用快速复查间隔：{next_interval} 秒。")
            sleep_for = max(0.0, next_interval - elapsed)
            wait_with_countdown(sleep_for, lambda: stop)
    finally:
        log("已停止。")
        if owned_profiles:
            owned_profiles.close()
        if free_profiles:
            free_profiles.close()
        close_log_file()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
