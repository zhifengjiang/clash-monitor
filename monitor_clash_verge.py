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
import copy
import concurrent.futures
import hashlib
import http.client
import ipaddress
import json
import logging
import os
import re
import shutil
import signal
import socket
import ssl
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen

import codex_probe


DEFAULT_TEST_URL = "https://chatgpt.com/cdn-cgi/trace"
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
DEFAULT_LOG_DIR = Path(__file__).resolve().parent / "logs"
MAX_LOG_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 5
DEFAULT_CACHE_DIR = Path.cwd() / ".clash-monitor-cache"
DEFAULT_FREE_STATS_PATH = Path.cwd() / "clash-free-stats.json"
DEFAULT_PROFILES_YAML = CLASH_VERGE_DIR / "profiles.yaml"
DEFAULT_PROFILE_DIR = CLASH_VERGE_DIR / "profiles"
DEFAULT_BASE_CONFIG = CLASH_VERGE_DIR / "clash-verge.yaml"
RUNTIME_CONFIG_SLOTS = (
    CLASH_VERGE_DIR / "clash-monitor-runtime-a.yaml",
    CLASH_VERGE_DIR / "clash-monitor-runtime-b.yaml",
)
MAX_PROFILE_BYTES = 8 * 1024 * 1024
MAX_SUBSCRIPTION_BYTES = 8 * 1024 * 1024
MAX_SUBSCRIPTION_NODES = 2000
INJECTED_NODE_NAME_RE = re.compile(
    r"^(?:可信备用|候选备用|免费备用) / .+ \[[0-9a-f]{8}\]$"
)
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
LOG_FILE: RotatingFileHandler | None = None


class ApiError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class ConcurrentSelectorChange(ApiError):
    pass


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
    profile_node_index: int = -1


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
    profiles_fingerprint: str
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
    route_failure_streak: int = 0
    active_config_path: str = ""
    route_ok: bool | None = None


@dataclass(frozen=True)
class SwitchOutcome:
    route_ok: bool | None = None
    source_type: str = ""
    switched: bool = False
    config_path: str = ""
    rollback_failed: bool = False


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
        write_log_file(text + end)


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
        size = path.stat().st_size
    except OSError as exc:
        raise ProfileError(f"Failed to read YAML {path}: {exc}") from exc
    if size > MAX_PROFILE_BYTES:
        raise ProfileError(
            f"YAML file is too large ({size} bytes, limit {MAX_PROFILE_BYTES}): {path}"
        )

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ProfileError(f"Failed to read YAML {path}: {exc}") from exc

    try:
        import yaml  # type: ignore

        class NoAliasSafeLoader(yaml.SafeLoader):
            def compose_node(self, parent: Any, index: Any) -> Any:
                if self.check_event(yaml.AliasEvent):
                    raise yaml.YAMLError("YAML aliases are disabled")
                return super().compose_node(parent, index)

        return yaml.load(text, Loader=NoAliasSafeLoader)
    except ImportError:
        pass
    except Exception as exc:
        raise ProfileError(f"Failed to parse YAML {path}: {exc}") from exc

    ruby = shutil.which("ruby") or "/usr/bin/ruby"
    if not Path(ruby).exists():
        raise ProfileError(
            "PyYAML is not installed and Ruby was not found; cannot parse subscription YAML."
        )

    # Keep the zero-dependency macOS fallback, but never use YAML.load/YAML.load_file:
    # public subscription files are untrusted input and must not instantiate Ruby objects.
    code = (
        "data=YAML.safe_load(File.read(ARGV[0]), "
        "permitted_classes: [], permitted_symbols: [], aliases: false, "
        "filename: ARGV[0]); puts JSON.generate(data)"
    )
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


def trusted_profile_uids(args: argparse.Namespace) -> set[str]:
    return {
        str(value).strip()
        for value in (getattr(args, "trusted_profile_uid", None) or [])
        if str(value).strip()
    }


def local_profile_source_type(args: argparse.Namespace, uid: str) -> str:
    trusted_uids = trusted_profile_uids(args)
    if trusted_uids and uid not in trusted_uids:
        return "unknown"
    return "owned"


def demote_profiles_to_unknown(
    profiles: list[RemoteProfile],
) -> list[RemoteProfile]:
    return [
        RemoteProfile(
            uid=profile.uid,
            name=profile.name,
            path=profile.path,
            source_type="unknown",
            source_url=profile.source_url,
        )
        for profile in profiles
    ]


def discover_remote_profiles(args: argparse.Namespace) -> list[RemoteProfile]:
    profiles_yaml = profiles_yaml_path(args)
    profile_dir = profile_dir_from(profiles_yaml)
    if not profiles_yaml.exists():
        return discover_profiles_by_files(profile_dir, args)

    data = load_yaml_data(profiles_yaml)
    if not isinstance(data, dict):
        raise ProfileError(f"{profiles_yaml} did not contain a profile config object")

    raw_items = data.get("items", [])
    if not isinstance(raw_items, list):
        raise ProfileError(f"{profiles_yaml} did not contain a valid items list")

    profiles: list[RemoteProfile] = []
    strict_uids = trusted_profile_uids(args)
    audit_incomplete = False
    uid_counts: dict[str, int] = {}
    for item in raw_items:
        if not isinstance(item, dict) or item.get("type") != "remote":
            continue
        file_name = item.get("file")
        if not file_name:
            audit_incomplete = audit_incomplete or bool(strict_uids)
            continue
        file_value = Path(str(file_name))
        if file_value.is_absolute() or ".." in file_value.parts:
            audit_incomplete = audit_incomplete or bool(strict_uids)
            continue
        path = profile_dir / file_value
        try:
            resolved_dir = profile_dir.resolve()
            resolved_path = path.resolve()
        except OSError:
            audit_incomplete = audit_incomplete or bool(strict_uids)
            continue
        if resolved_dir != resolved_path.parent and resolved_dir not in resolved_path.parents:
            audit_incomplete = audit_incomplete or bool(strict_uids)
            continue
        if path.is_symlink():
            audit_incomplete = audit_incomplete or bool(strict_uids)
            continue
        path = resolved_path
        if not path.exists():
            audit_incomplete = audit_incomplete or bool(strict_uids)
            continue
        raw_uid = item.get("uid")
        if strict_uids and not raw_uid:
            audit_incomplete = True
        uid = str(raw_uid or path.stem)
        uid_counts[uid] = uid_counts.get(uid, 0) + 1
        name = str(item.get("name") or uid)
        profiles.append(
            RemoteProfile(
                uid=uid,
                name=name,
                path=path,
                source_type=local_profile_source_type(args, uid),
            )
        )

    if not profiles:
        profiles = discover_profiles_by_files(profile_dir, args)
    if strict_uids:
        discovered_uids = {profile.uid for profile in profiles}
        duplicate_uids = {
            uid for uid, count in uid_counts.items() if count > 1
        }
        if (
            audit_incomplete
            or duplicate_uids
            or not strict_uids.issubset(discovered_uids)
        ):
            return demote_profiles_to_unknown(profiles)
    return profiles


def discover_profiles_by_files(
    profile_dir: Path,
    args: argparse.Namespace | None = None,
) -> list[RemoteProfile]:
    profiles: list[RemoteProfile] = []
    if not profile_dir.exists():
        return profiles
    for path in sorted([*profile_dir.glob("*.yaml"), *profile_dir.glob("*.yml")]):
        try:
            data = load_yaml_data(path)
        except ProfileError:
            continue
        if isinstance(data, dict) and isinstance(data.get("proxies"), list):
            source_type = (
                "unknown"
                if args is not None and trusted_profile_uids(args)
                else local_profile_source_type(args, path.stem)
                if args is not None
                else "owned"
            )
            profiles.append(
                RemoteProfile(
                    uid=path.stem,
                    name=path.stem,
                    path=path,
                    source_type=source_type,
                )
            )
    if args is not None and trusted_profile_uids(args):
        # File names are not authoritative Clash Verge UIDs. Without a valid
        # profiles.yaml mapping, fail closed instead of treating a matching stem
        # (or duplicate .yaml/.yml stems) as allowlisted.
        return demote_profiles_to_unknown(profiles)
    return profiles


def free_subscription_urls(args: argparse.Namespace) -> list[str]:
    urls = list(DEFAULT_FREE_SUBSCRIPTION_URLS)
    urls.extend(args.free_url or [])
    return list(dict.fromkeys(url.strip() for url in urls if url.strip()))


def cache_file_for_url(url: str, suffix: str) -> Path:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return DEFAULT_CACHE_DIR / f"free-{digest}{suffix}"


def safe_url_label(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or "unknown-host"
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{host}#{digest}"


def secure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass


def secure_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            os.chmod(handle.name, 0o600)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except OSError:
                pass
        raise


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
        "source": safe_url_label(url),
        "node_count": node_count,
        "profile_cache": str(profile_path),
        "raw_cache": str(raw_path) if raw_path.exists() else "",
        "fetched_at": datetime.fromtimestamp(now).isoformat(timespec="seconds"),
        "expires_at": datetime.fromtimestamp(now + ttl_seconds).isoformat(timespec="seconds"),
        "ttl_seconds": ttl_seconds,
    }
    secure_write_text(meta_path, json.dumps(metadata, ensure_ascii=False, indent=2))


def cached_profile_node_count(path: Path) -> int:
    try:
        data = load_yaml_data(path)
    except ProfileError:
        return 0
    if isinstance(data, dict) and isinstance(data.get("proxies"), list):
        return len([proxy for proxy in data["proxies"] if isinstance(proxy, dict)])
    return 0


def fetch_url_text(
    url: str,
    timeout: int = 30,
    max_bytes: int = MAX_SUBSCRIPTION_BYTES,
) -> str:
    if urlparse(url).scheme.lower() != "https":
        raise ProfileError(
            f"Subscription URL must use HTTPS: {safe_url_label(url)}"
        )
    request = Request(url, headers={"User-Agent": "clash-verge-monitor/1.0"})
    with urlopen(request, timeout=timeout) as response:
        final_url = response.geturl()
        if urlparse(final_url).scheme.lower() != "https":
            raise ProfileError(
                "Subscription redirect left HTTPS: "
                f"{safe_url_label(final_url)}"
            )
        body = response.read(max_bytes + 1)
        if len(body) > max_bytes:
            raise ProfileError(
                f"Subscription response exceeded {max_bytes} bytes: "
                f"{safe_url_label(final_url)}"
            )
        return body.decode("utf-8", errors="replace")


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
        secure_write_text(raw_path, decoded)
        data = load_yaml_data(raw_path)
        if isinstance(data, dict) and isinstance(data.get("proxies"), list):
            proxies = [proxy for proxy in data["proxies"] if isinstance(proxy, dict)]
    else:
        for line in decoded.splitlines():
            proxy = parse_share_link(line)
            if proxy:
                proxies.append(proxy)

    if len(proxies) > MAX_SUBSCRIPTION_NODES:
        raise ProfileError(
            f"Subscription contained {len(proxies)} nodes; "
            f"limit is {MAX_SUBSCRIPTION_NODES}"
        )

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
    secure_write_text(output_path, json.dumps(profile, ensure_ascii=False, indent=2))
    return len(cleaned)


def discover_free_profiles(args: argparse.Namespace) -> list[RemoteProfile]:
    if args.no_free_backup:
        return []

    secure_directory(DEFAULT_CACHE_DIR)
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
                secure_write_text(raw_cache_path, text)
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
                    f"本地缓存={cache_path}，来源={safe_url_label(url)}"
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
    return [
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


def normalize_controller(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith(("http://", "https://")):
        return value.rstrip("/")
    if value.startswith(":"):
        return f"http://127.0.0.1{value}"
    return f"http://{value}".rstrip("/")


def is_local_controller_url(value: str) -> bool:
    parsed = urlparse(normalize_controller(value))
    host = parsed.hostname
    if not host:
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def is_safe_unix_socket(value: str) -> bool:
    path = Path(value)
    if not path.is_absolute():
        return False
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return (
        not path.is_symlink()
        and stat.S_ISSOCK(metadata.st_mode)
        and metadata.st_uid in {0, os.getuid()}
    )


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
        if not is_safe_unix_socket(explicit_socket):
            raise ProfileError(
                "Clash Unix socket 必须是本机绝对路径、非符号链接，"
                "且由当前用户或 root 所有。"
            )
        for secret in explicit_secrets:
            add_candidate(candidates, seen, Controller(unix_socket=explicit_socket, secret=secret))
    if explicit_api:
        normalized_api = normalize_controller(explicit_api)
        if not is_local_controller_url(normalized_api):
            raise ProfileError(
                "For safety, the Clash controller must use a literal loopback address "
                "(127.0.0.0/8 or ::1)."
            )
        for secret in explicit_secrets:
            add_candidate(
                candidates,
                seen,
                Controller(base_url=normalized_api, secret=secret),
            )

    for hint in hints:
        if hint.unix_socket and is_safe_unix_socket(hint.unix_socket):
            add_candidate(
                candidates,
                seen,
                Controller(unix_socket=hint.unix_socket, secret=args.secret or hint.secret),
            )
        if hint.controller and is_local_controller_url(hint.controller):
            add_candidate(
                candidates,
                seen,
                Controller(
                    base_url=normalize_controller(hint.controller),
                    secret=args.secret or hint.secret,
                ),
            )

    for socket_path in COMMON_UNIX_SOCKETS:
        if is_safe_unix_socket(socket_path):
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


def profiles_fingerprint(profiles: list[RemoteProfile]) -> str:
    digest = hashlib.sha256()
    for profile in sorted(profiles, key=lambda item: (item.uid, str(item.path))):
        for value in (
            profile.uid,
            profile.name,
            str(profile.path),
            profile.source_type,
            profile.source_url,
        ):
            digest.update(value.encode("utf-8", errors="replace"))
            digest.update(b"\0")
        try:
            digest.update(profile.path.read_bytes())
        except OSError as exc:
            raise ProfileError(f"Unable to fingerprint profile {profile.path}: {exc}") from exc
        digest.update(b"\0")
    return digest.hexdigest()


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

        for proxy_index, proxy in enumerate(profile_proxies):
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
                profile_node_index=proxy_index,
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
    profiles = profiles if profiles is not None else discover_remote_profiles(args)
    fingerprint = profiles_fingerprint(profiles)
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
                profiles_fingerprint=fingerprint,
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
                    source_type=original.source_type,
                    source_url=original.source_url,
                    profile_node_index=original.profile_node_index,
                )
            )
        else:
            source_type = (
                "free"
                if (
                    is_monitor_injected_name(name)
                    and name.startswith("免费备用 / ")
                )
                or name.startswith("免费源")
                else "owned"
                if (
                    is_monitor_injected_name(name)
                    and name.startswith("可信备用 / ")
                )
                else "unknown"
            )
            targets.append(
                NodeTarget(
                    api_name=name,
                    name=name,
                    proxy_type=proxy_type,
                    subscription="当前配置",
                    source_type=source_type,
                )
            )
    return targets


def proxy_names_from_config_path(path: Path) -> set[str]:
    try:
        data = load_yaml_data(path)
    except (OSError, ProfileError):
        return set()
    proxies = data.get("proxies") if isinstance(data, dict) else None
    if not isinstance(proxies, list):
        return set()
    return {
        str(proxy.get("name") or "")
        for proxy in proxies
        if isinstance(proxy, dict) and proxy.get("name")
    }


def base_proxy_names_for_args(args: argparse.Namespace) -> set[str]:
    try:
        path = base_config_path(args)
    except ProfileError:
        return set()
    return proxy_names_from_config_path(path)


def explicitly_trusted_proxy_names(args: argparse.Namespace) -> set[str]:
    if not trusted_profile_uids(args):
        return set()
    try:
        profiles = discover_remote_profiles(args)
    except ProfileError:
        return set()

    trusted_names: set[str] = set()
    other_names: set[str] = set()
    for profile in profiles:
        try:
            data = load_yaml_data(profile.path)
        except ProfileError:
            return set()
        proxies = data.get("proxies") if isinstance(data, dict) else None
        if not isinstance(proxies, list):
            return set()
        names = {
            str(proxy.get("name") or "")
            for proxy in proxies
            if isinstance(proxy, dict) and proxy.get("name")
        }
        if profile.source_type == "owned":
            trusted_names.update(names)
        else:
            other_names.update(names)
    # A live proxy name carries no provider UID. Treat collisions as unknown
    # rather than accidentally promoting an unlisted profile.
    return trusted_names - other_names


def classify_live_targets(
    targets: list[NodeTarget],
    args: argparse.Namespace,
    trusted_names: set[str] | None = None,
) -> list[NodeTarget]:
    trusted_names = trusted_names or set()
    classified: list[NodeTarget] = []
    for target in targets:
        if (
            is_monitor_injected_name(target.api_name)
            and target.api_name.startswith("免费备用 / ")
        ) or target.api_name.startswith("免费源"):
            source_type = "free"
        elif (
            (
                is_monitor_injected_name(target.api_name)
                and target.api_name.startswith("可信备用 / ")
                and not trusted_profile_uids(args)
            )
            or target.api_name in trusted_names
        ):
            source_type = "owned"
        else:
            source_type = "unknown"
        classified.append(
            NodeTarget(
                api_name=target.api_name,
                name=target.name,
                proxy_type=target.proxy_type,
                subscription=target.subscription,
                profile_uid=target.profile_uid,
                profile_path=target.profile_path,
                source_type=source_type,
                source_url=target.source_url,
                profile_node_index=target.profile_node_index,
            )
        )
    return classified


def route_source_type(
    route_chain: list[str],
    args: argparse.Namespace,
    trusted_names: set[str] | None = None,
) -> str:
    if any(
        (
            is_monitor_injected_name(part)
            and part.startswith("免费备用 / ")
        )
        or part.startswith("免费源")
        for part in route_chain
    ):
        return "free"
    trusted_names = trusted_names or set()
    terminal = next(
        (
            part
            for part in reversed(route_chain)
            if part not in {"rule", "global", "direct", "未知"}
        ),
        "",
    )
    if (
        is_monitor_injected_name(terminal)
        and terminal.startswith("可信备用 / ")
        and not trusted_profile_uids(args)
    ) or terminal in trusted_names:
        return "owned"
    return "unknown"


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


def strict_openai_destination_ok(url: str, scheme: str) -> bool:
    parsed = urlparse(url)
    try:
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme.lower() == scheme
        and (parsed.hostname or "").lower() == "api.openai.com"
        and port in {None, 443}
        and parsed.username is None
        and parsed.password is None
    )



def comprehensive_route_check(
    args: argparse.Namespace,
    port: int,
    attempts: int | None = None,
) -> tuple[bool | None, str]:
    """True means the selected scope passed; None blocks node switching."""
    try:
        cfg = codex_probe.settings(args)
    except codex_probe.ProbeError as exc:
        return None, f"{codex_probe.probe_label(args)}受阻：{exc}"
    # Route selection must follow the actual model backend, including API-key
    # mode; the CDN URL remains only the cheap candidate latency filter.
    args._codex_route_url = cfg.sse_url
    timeout_s = max(args.timeout / 1000 + 2, 3)
    max_attempts = max(1, attempts if attempts is not None else args.route_retries)
    failures: list[str] = []
    for attempt in range(1, max_attempts + 1):
        http_ok, http_message = check_via_mixed_proxy(args.url, port, timeout_s=timeout_s, attempts=1)
        messages = [f"HTTP 基础连接：{http_message}"]
        results: list[bool | None] = [http_ok]
        if http_ok:
            probes = [("API", codex_probe.probe_models)]
            if cfg.mode == "generation":
                probes.extend([("SSE", codex_probe.probe_sse), ("WebSocket", codex_probe.probe_websocket)])
            else:
                probes.append(("WebSocket", codex_probe.probe_websocket_network))
            for name, probe in probes:
                ok, message = codex_probe.run_step(name, probe, cfg, port)
                results.append(ok)
                messages.append(message)
                if ok is None:
                    return None, "; ".join(messages) + "；本轮停止节点切换"
        scope = f"模式=生成，模型={cfg.model}" if cfg.mode == "generation" else "模式=网络（鉴权/API/WS 往返，不执行模型生成或 SSE 生成流测试）"
        message = f"认证={cfg.credentials.mode}，{scope}；" + "; ".join(messages)
        if all(result is True for result in results):
            suffix = f"（第 {attempt}/{max_attempts} 次成功）" if attempt > 1 else ""
            return True, message + suffix
        failures.append(message)
        if attempt < max_attempts and args.route_retry_delay > 0:
            time.sleep(args.route_retry_delay)
    suffix = f"（连续 {len(failures)} 次真实验证失败）" if len(failures) > 1 else ""
    return False, failures[-1] + suffix


def guarded_comprehensive_route_check(
    args: argparse.Namespace,
    port: int,
    attempts: int | None = None,
) -> tuple[bool | None, str]:
    try:
        return comprehensive_route_check(args, port, attempts=attempts)
    except Exception as exc:
        # Internal/configuration errors must not cycle through every proxy node.
        # Avoid logging exception bodies that could contain credentials.
        return None, f"Codex 真实验证异常（{type(exc).__name__}）；本轮停止节点切换"


def advance_route_failure_streak(route_ok: bool | None, current: int) -> int:
    if route_ok is True:
        return 0
    if route_ok is False:
        return max(0, current) + 1
    return max(0, current)


def should_recover_trusted_route(
    active_source_type: str,
    args: argparse.Namespace,
) -> bool:
    return active_source_type == "free" or (
        active_source_type == "unknown"
        and bool(trusted_profile_uids(args))
    )


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
    fast_limit = deep_probe_delay_limit(args)

    def source_priority(result: DelayResult) -> int:
        if not known_targets:
            return 0
        target = known_targets.get(result.api_name)
        if not target or target.source_type == "unknown":
            return 1
        return 2 if target.source_type == "free" else 0

    usable.sort(
        key=lambda result: (
            source_priority(result),
            0 if (result.delay_ms if result.delay_ms is not None else 10**9) <= fast_limit else 1,
            result.delay_ms if result.delay_ms is not None else 10**9,
            result.subscription,
            result.name,
        )
    )

    # The latency value is a preference, never an availability cutoff. Exhaust all
    # trusted candidates before falling back to public/free nodes. The candidate
    # cap only limits untrusted/free probing, where pools can contain thousands.
    if args.deep_probe_max_candidates > 0:
        if known_targets:
            trusted = [
                result
                for result in usable
                if known_targets[result.api_name].source_type == "owned"
            ]
            fallback = [
                result
                for result in usable
                if known_targets[result.api_name].source_type != "owned"
            ]
            return [*trusted, *fallback[: args.deep_probe_max_candidates]]
        return usable[: args.deep_probe_max_candidates]
    return usable


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


class PrivateRotatingFileHandler(RotatingFileHandler):
    """Keep newly created log files private, including after rollover."""

    def _open(self) -> Any:
        fd = os.open(self.baseFilename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.fchmod(fd, 0o600)
            return os.fdopen(fd, self.mode, encoding=self.encoding, errors=self.errors)
        except BaseException:
            os.close(fd)
            raise


def default_log_path() -> Path:
    return DEFAULT_LOG_DIR / "clash-monitor.log"


def write_log_file(text: str) -> None:
    if LOG_FILE is not None:
        LOG_FILE.handle(logging.LogRecord(
            "clash-monitor", logging.INFO, __file__, 0, strip_ansi(text), (), None,
        ))


def setup_log_file(args: argparse.Namespace) -> Path | None:
    global LOG_FILE

    close_log_file()
    if args.no_log_file:
        return None

    path = Path(args.log_file).expanduser() if args.log_file else default_log_path()
    if not path.is_absolute():
        path = DEFAULT_LOG_DIR / path
    path.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE = PrivateRotatingFileHandler(
        path, maxBytes=MAX_LOG_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8",
    )
    LOG_FILE.terminator = ""
    write_log_file("\n" + "=" * 88 + "\n")
    write_log_file(f"会话开始：{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    return path


def close_log_file() -> None:
    global LOG_FILE

    if LOG_FILE is not None:
        write_log_file(f"会话结束：{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
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


def route_switch_groups(
    proxies: dict[str, Any],
    route_chain: list[str],
    args: argparse.Namespace,
    node_name: str,
) -> list[str]:
    requested = requested_switch_groups(args)
    route_names = [
        name
        for name in route_chain
        if name.lower() not in {"rule", "global", "direct"}
        and name != "未知"
    ]
    candidates = (
        [name for name in requested if name in route_names]
        if requested
        else route_names
    )
    groups: list[str] = []
    for group_name in candidates:
        if group_name in groups or group_name == "GLOBAL":
            continue
        proxy = proxies.get(group_name)
        if not isinstance(proxy, dict):
            continue
        all_names = proxy.get("all")
        if not isinstance(all_names, list) or node_name not in all_names:
            continue
        if str(proxy.get("type", "")).lower() != "selector":
            continue
        groups.append(group_name)
    return groups


def switch_group(controller: Controller, group_name: str, node_name: str) -> None:
    api_json(
        controller,
        f"/proxies/{quote(group_name, safe='')}",
        timeout=5.0,
        method="PUT",
        payload={"name": node_name},
    )


def config_rule_target_for_url(rules: Any, url: str) -> str:
    if not isinstance(rules, list):
        return ""
    host = (urlparse(url).hostname or "").lower().rstrip(".")
    match_target = ""
    for raw_rule in rules:
        if isinstance(raw_rule, dict):
            rule_type = str(raw_rule.get("type") or "")
            payload = str(raw_rule.get("payload") or "")
            target = str(raw_rule.get("proxy") or "")
        elif isinstance(raw_rule, str):
            parts = [part.strip() for part in raw_rule.split(",")]
            if len(parts) < 2:
                continue
            rule_type = parts[0]
            if rule_type.upper() == "MATCH":
                payload = ""
                target = parts[1]
            elif len(parts) >= 3:
                payload = parts[1]
                target = parts[2]
            else:
                continue
        else:
            continue

        normalized = rule_type.replace("-", "").lower()
        payload = payload.lower().rstrip(".")
        if normalized == "match":
            match_target = target
            break
        if not host or not payload or not target:
            continue
        if normalized == "domain" and host == payload:
            return target
        if normalized == "domainsuffix" and (
            host == payload or host.endswith(f".{payload}")
        ):
            return target
        if normalized == "domainkeyword" and payload in host:
            return target
    return match_target


def base_config_path(args: argparse.Namespace) -> Path:
    value = getattr(args, "base_config", None) or os.getenv("CLASH_BASE_CONFIG")
    path = Path(value).expanduser() if value else DEFAULT_BASE_CONFIG
    slot_paths = {slot.expanduser().resolve() for slot in RUNTIME_CONFIG_SLOTS}
    if path.expanduser().resolve() in slot_paths:
        raise ProfileError("基础配置不能指向监控器生成的运行时配置槽。")
    if not path.exists() or not path.is_file():
        raise ProfileError(f"找不到 Clash 基础配置：{path}")
    return path


def config_select_groups(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    groups = config.get("proxy-groups")
    if not isinstance(groups, list):
        return {}
    selected: dict[str, dict[str, Any]] = {}
    for group in groups:
        if not isinstance(group, dict):
            continue
        name = str(group.get("name") or "")
        if name and str(group.get("type") or "").lower() == "select":
            selected[name] = group
    return selected


def select_base_switch_groups(
    base_config: dict[str, Any],
    args: argparse.Namespace,
    url: str,
    route_chain: list[str] | None = None,
) -> list[str]:
    selectors = config_select_groups(base_config)
    requested = requested_switch_groups(args)
    if requested:
        missing = [name for name in requested if name not in selectors]
        if missing:
            raise ProfileError(
                f"基础配置中不存在可切换的 select 策略组：{', '.join(missing)}"
            )
        concrete_route = [
            name
            for name in (route_chain or [])
            if name.lower() not in {"rule", "global", "direct"}
            and name != "未知"
        ]
        if route_chain and not concrete_route:
            raise ProfileError(
                "无法确认 ChatGPT 当前策略组，本轮拒绝修改 Selector。"
            )
        if concrete_route:
            off_route = [name for name in requested if name not in concrete_route]
            if off_route:
                raise ProfileError(
                    "指定的策略组不在 ChatGPT 当前路径上："
                    f"{', '.join(off_route)}"
                )
        return list(dict.fromkeys(requested))

    if route_chain:
        matched = [
            name for name in route_chain if name in selectors and name != "GLOBAL"
        ]
        matched = list(dict.fromkeys(matched))
        if len(matched) == 1:
            return matched
        if len(matched) > 1:
            raise ProfileError(
                "ChatGPT 当前路径经过多个 select 策略组，请用 --switch-group 明确指定。"
            )
        raise ProfileError(
            "当前运行路径无法与基础配置的 ChatGPT Selector 对应，"
            "本轮拒绝重载配置。"
        )

    target = config_rule_target_for_url(base_config.get("rules"), url)
    if target in selectors and target != "GLOBAL":
        return [target]
    raise ProfileError(
        "无法从基础配置唯一确定 ChatGPT 的 select 策略组；"
        "请用 --switch-group 明确指定。"
    )


def find_target_proxy(
    source_profile: dict[str, Any],
    target: NodeTarget,
) -> dict[str, Any]:
    proxies = source_profile.get("proxies")
    if not isinstance(proxies, list):
        raise ProfileError("订阅配置没有有效的 proxies 列表。")

    index = target.profile_node_index
    if 0 <= index < len(proxies):
        indexed = proxies[index]
        if isinstance(indexed, dict) and str(indexed.get("name") or "") == target.name:
            return copy.deepcopy(indexed)

    matches = [
        proxy
        for proxy in proxies
        if isinstance(proxy, dict) and str(proxy.get("name") or "") == target.name
    ]
    if not matches:
        raise ProfileError(f"订阅中找不到目标节点：{target.name}")
    if len(matches) > 1:
        raise ProfileError(f"订阅中存在重名节点，无法安全切换：{target.name}")
    return copy.deepcopy(matches[0])


def injected_proxy_name(
    target: NodeTarget,
    proxy: dict[str, Any],
    base_config: dict[str, Any],
) -> str:
    prefix = {
        "owned": "可信备用 / ",
        "free": "免费备用 / ",
    }.get(target.source_type, "候选备用 / ")
    identity = {
        "target": (
            target.profile_uid,
            target.subscription,
            target.name,
            target.api_name,
        ),
        "proxy": proxy,
        "base": base_config,
    }
    digest = hashlib.sha256(
        json.dumps(
            identity,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()[:8]
    label = f"{prefix}{target.subscription} / {target.name}"
    return f"{short(label, 108)} [{digest}]"


def is_monitor_injected_name(name: str) -> bool:
    return bool(INJECTED_NODE_NAME_RE.fullmatch(name))


def proxy_definition_identity(proxy: dict[str, Any]) -> str:
    definition = {
        key: value
        for key, value in proxy.items()
        if key != "name"
    }
    return hashlib.sha256(
        json.dumps(
            definition,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def verified_trusted_injected_names(
    args: argparse.Namespace,
    runtime_path: Path | None,
) -> set[str]:
    if runtime_path is None or not trusted_profile_uids(args):
        return set()
    try:
        runtime_data = load_yaml_data(runtime_path)
        profiles = discover_remote_profiles(args)
    except ProfileError:
        return set()

    trusted_identities: set[str] = set()
    other_identities: set[str] = set()
    for profile in profiles:
        try:
            profile_data = load_yaml_data(profile.path)
        except ProfileError:
            return set()
        proxies = (
            profile_data.get("proxies")
            if isinstance(profile_data, dict)
            else None
        )
        if not isinstance(proxies, list):
            return set()
        destination = (
            trusted_identities
            if profile.source_type == "owned"
            else other_identities
        )
        destination.update(
            proxy_definition_identity(proxy)
            for proxy in proxies
            if isinstance(proxy, dict)
        )

    runtime_proxies = (
        runtime_data.get("proxies")
        if isinstance(runtime_data, dict)
        else None
    )
    if not isinstance(runtime_proxies, list):
        return set()
    verified: set[str] = set()
    for proxy in runtime_proxies:
        if not isinstance(proxy, dict):
            continue
        name = str(proxy.get("name") or "")
        if not (
            is_monitor_injected_name(name)
            and name.startswith("可信备用 / ")
        ):
            continue
        identity = proxy_definition_identity(proxy)
        if identity in trusted_identities and identity not in other_identities:
            verified.add(name)
    return verified


def merge_runtime_switch_config(
    base_config: dict[str, Any],
    source_profile: dict[str, Any],
    target: NodeTarget,
    group_names: list[str],
) -> tuple[dict[str, Any], str]:
    if not isinstance(base_config, dict):
        raise ProfileError("基础配置不是有效对象。")
    if not group_names:
        raise ProfileError("没有可注入节点的目标 select 策略组。")

    merged = copy.deepcopy(base_config)
    proxies = merged.get("proxies")
    groups = merged.get("proxy-groups")
    if not isinstance(proxies, list) or not isinstance(groups, list):
        raise ProfileError("基础配置缺少 proxies 或 proxy-groups。")

    proxy = find_target_proxy(source_profile, target)
    injected_name = injected_proxy_name(target, proxy, base_config)
    existing_names = {
        str(proxy.get("name") or "")
        for proxy in proxies
        if isinstance(proxy, dict)
    }
    if injected_name in existing_names:
        raise ProfileError(f"基础配置已存在同名注入节点：{injected_name}")

    proxy["name"] = injected_name
    proxies.append(proxy)

    wanted = list(dict.fromkeys(group_names))
    found: set[str] = set()
    for group in groups:
        if not isinstance(group, dict):
            continue
        name = str(group.get("name") or "")
        if name not in wanted:
            continue
        if str(group.get("type") or "").lower() != "select":
            raise ProfileError(f"目标策略组不是 select：{name}")
        members = group.get("proxies")
        if not isinstance(members, list):
            raise ProfileError(f"目标策略组没有有效的 proxies 列表：{name}")
        if injected_name not in members:
            members.append(injected_name)
        found.add(name)

    missing = [name for name in wanted if name not in found]
    if missing:
        raise ProfileError(f"基础配置中找不到目标策略组：{', '.join(missing)}")
    return merged, injected_name


def strip_runtime_injections(config: dict[str, Any]) -> dict[str, Any]:
    cleaned = copy.deepcopy(config)
    proxies = cleaned.get("proxies")
    injected_names: set[str] = set()
    if isinstance(proxies, list):
        for proxy in proxies:
            if not isinstance(proxy, dict):
                continue
            name = str(proxy.get("name") or "")
            if is_monitor_injected_name(name):
                injected_names.add(name)
        cleaned["proxies"] = [
            proxy
            for proxy in proxies
            if not (
                isinstance(proxy, dict)
                and str(proxy.get("name") or "") in injected_names
            )
        ]

    groups = cleaned.get("proxy-groups")
    if isinstance(groups, list) and injected_names:
        for group in groups:
            if not isinstance(group, dict):
                continue
            members = group.get("proxies")
            if isinstance(members, list):
                group["proxies"] = [
                    member for member in members if member not in injected_names
                ]
    return cleaned


def normalized_group_type(value: Any) -> str:
    normalized = str(value or "").replace("-", "").lower()
    return "selector" if normalized == "select" else normalized


def normalized_proxy_type(value: Any) -> str:
    normalized = re.sub(r"[\s_-]+", "", str(value or "").lower())
    return {
        "ss": "shadowsocks",
        "ssr": "shadowsocksr",
        "wg": "wireguard",
        "socks": "socks5",
    }.get(normalized, normalized)


def rule_signatures(rules: Any) -> list[tuple[str, str, str]]:
    if not isinstance(rules, list):
        return []
    signatures: list[tuple[str, str, str]] = []
    for raw_rule in rules:
        if isinstance(raw_rule, dict):
            rule_type = str(raw_rule.get("type") or "")
            payload = str(raw_rule.get("payload") or "")
            target = str(raw_rule.get("proxy") or "")
        elif isinstance(raw_rule, str):
            parts = [part.strip() for part in raw_rule.split(",")]
            if len(parts) < 2:
                continue
            rule_type = parts[0]
            if normalized_group_type(rule_type) == "match":
                payload = ""
                target = parts[1]
            elif len(parts) >= 3:
                payload = parts[1]
                target = parts[2]
            else:
                continue
        else:
            continue
        signatures.append(
            (normalized_group_type(rule_type), payload, target)
        )
    return signatures


def live_matches_reference_config(
    reference: dict[str, Any],
    live_proxies: dict[str, Any],
    live_rules: list[dict[str, Any]],
    live_configs: dict[str, Any],
) -> tuple[bool, str]:
    reference_groups = reference.get("proxy-groups")
    if not isinstance(reference_groups, list):
        return False, "参考配置缺少 proxy-groups"

    expected_group_names: set[str] = set()
    for group in reference_groups:
        if not isinstance(group, dict):
            return False, "参考配置包含无效策略组"
        name = str(group.get("name") or "")
        members = group.get("proxies")
        live_group = live_proxies.get(name)
        if not name or not isinstance(members, list):
            return False, "参考配置包含无效策略组成员"
        if not isinstance(live_group, dict):
            return False, f"当前配置缺少策略组：{name}"
        if normalized_group_type(group.get("type")) != normalized_group_type(
            live_group.get("type")
        ):
            return False, f"策略组类型与参考配置不同：{name}"
        live_members = live_group.get("all")
        if not isinstance(live_members, list) or list(members) != list(live_members):
            return False, f"策略组成员与参考配置不同：{name}"
        expected_group_names.add(name)

    live_group_names = {
        name
        for name, proxy in live_proxies.items()
        if name != "GLOBAL" and isinstance(proxy, dict) and is_group(name, proxy)
    }
    if live_group_names != expected_group_names:
        return False, "当前策略组结构与参考配置不同"

    reference_proxies = reference.get("proxies")
    if not isinstance(reference_proxies, list):
        return False, "参考配置缺少 proxies"
    expected_proxy_names = {
        str(proxy.get("name") or "")
        for proxy in reference_proxies
        if isinstance(proxy, dict) and proxy.get("name")
    }
    if not expected_proxy_names.issubset(live_proxies):
        return False, "当前配置缺少参考配置中的节点"
    for proxy in reference_proxies:
        if not isinstance(proxy, dict) or not proxy.get("name"):
            continue
        name = str(proxy["name"])
        live_proxy = live_proxies.get(name)
        if not isinstance(live_proxy, dict):
            return False, f"当前配置缺少参考配置中的节点：{name}"
        if normalized_proxy_type(proxy.get("type")) != normalized_proxy_type(
            live_proxy.get("type")
        ):
            return False, f"节点类型与参考配置不同：{name}"

    if rule_signatures(reference.get("rules")) != rule_signatures(live_rules):
        return False, "当前规则与参考配置不同"

    for key in (
        "mode",
        "mixed-port",
        "port",
        "socks-port",
        "redir-port",
        "tproxy-port",
        "allow-lan",
        "ipv6",
    ):
        if key not in reference:
            continue
        live_value = get_config_value(live_configs, key)
        if live_value is not None and live_value != reference[key]:
            return False, f"当前 {key} 与参考配置不同"

    for key in ("dns", "tun"):
        if key not in reference:
            continue
        live_value = get_config_value(live_configs, key)
        if live_value is not None and live_value != reference[key]:
            return False, f"当前 {key} 与参考配置不同"
    return True, ""


def selector_group_snapshot(proxies: dict[str, Any]) -> dict[str, str]:
    snapshot: dict[str, str] = {}
    for name, proxy in proxies.items():
        if not isinstance(proxy, dict):
            continue
        if str(proxy.get("type") or "").lower() != "selector":
            continue
        now = proxy.get("now")
        if isinstance(now, str) and now:
            snapshot[name] = now
    return snapshot


def fetch_live_proxies(controller: Controller) -> dict[str, Any]:
    data = api_json(controller, "/proxies", timeout=8.0)
    proxies = data.get("proxies", {}) if isinstance(data, dict) else {}
    if not isinstance(proxies, dict):
        raise ApiError("Clash /proxies 没有返回有效对象")
    return proxies


def identify_live_config(
    controller: Controller,
    candidate_config: dict[str, Any],
    reference_config: dict[str, Any],
) -> tuple[str, dict[str, Any], str]:
    try:
        proxies = fetch_live_proxies(controller)
        configs_data = api_json(controller, "/configs", timeout=5.0)
        if not isinstance(configs_data, dict):
            return "unknown", proxies, "Clash /configs 没有返回有效对象"
        rules_data = api_json(controller, "/rules", timeout=5.0)
        rules = rules_from_response(rules_data)
    except ApiError as exc:
        return "unknown", {}, str(exc)

    candidate_matches, candidate_reason = live_matches_reference_config(
        candidate_config,
        proxies,
        rules,
        configs_data,
    )
    if candidate_matches:
        return "candidate", proxies, ""

    reference_matches, reference_reason = live_matches_reference_config(
        reference_config,
        proxies,
        rules,
        configs_data,
    )
    if reference_matches:
        return "reference", proxies, ""
    return (
        "external",
        proxies,
        f"候选不匹配：{candidate_reason}；回滚参考不匹配：{reference_reason}",
    )


def reapply_selector_snapshot(
    controller: Controller,
    snapshot: dict[str, str],
    baseline_proxies: dict[str, Any] | None = None,
) -> None:
    baseline_proxies = baseline_proxies or fetch_live_proxies(controller)
    baseline: dict[str, str] = {}
    for group_name, previous in snapshot.items():
        group = baseline_proxies.get(group_name)
        if not isinstance(group, dict):
            raise ApiError(f"策略组在配置重载后消失：{group_name}")
        if str(group.get("type") or "").lower() != "selector":
            raise ApiError(f"策略组类型在配置重载后改变：{group_name}")
        members = group.get("all")
        if not isinstance(members, list) or previous not in members:
            raise ApiError(f"策略组无法恢复原选择：{group_name} => {previous}")
        current = group.get("now")
        if not isinstance(current, str) or not current:
            raise ApiError(f"策略组没有有效的当前选择：{group_name}")
        baseline[group_name] = current

    for group_name, previous in snapshot.items():
        current_proxies = fetch_live_proxies(controller)
        current_group = current_proxies.get(group_name)
        if not isinstance(current_group, dict):
            raise ApiError(f"策略组在配置重载后消失：{group_name}")
        if current_group.get("now") != baseline[group_name]:
            raise ConcurrentSelectorChange(
                f"检测到外部策略组变更：{group_name}"
            )
        if current_group.get("now") != previous:
            switch_group(controller, group_name, previous)
            verified = fetch_live_proxies(controller).get(group_name)
            if not isinstance(verified, dict) or verified.get("now") != previous:
                raise ConcurrentSelectorChange(
                    f"策略组写入后被外部修改：{group_name}"
                )


def changed_selector_groups(
    proxies: dict[str, Any],
    expected: dict[str, str],
) -> list[str]:
    return [
        group_name
        for group_name, expected_now in expected.items()
        if not isinstance(proxies.get(group_name), dict)
        or proxies[group_name].get("now") != expected_now
    ]


def verify_selector_selections(
    controller: Controller,
    expected: dict[str, str],
) -> None:
    proxies = fetch_live_proxies(controller)
    for group_name, node_name in expected.items():
        group = proxies.get(group_name)
        if not isinstance(group, dict):
            raise ApiError(f"找不到策略组：{group_name}")
        members = group.get("all")
        if (
            str(group.get("type") or "").lower() != "selector"
            or not isinstance(members, list)
            or node_name not in members
            or group.get("now") != node_name
        ):
            raise ApiError(f"策略组切换验证失败：{group_name} => {node_name}")


def rollback_selectors_if_unchanged(
    controller: Controller,
    previous_by_group: dict[str, str],
    candidate_name: str,
) -> tuple[bool, list[str]]:
    expected: dict[str, str] = {}
    externally_changed: list[str] = []
    for group_name, previous in previous_by_group.items():
        # Re-read before every write. A single snapshot for several groups leaves
        # a window where a later manual selection can be overwritten.
        try:
            live_proxies = fetch_live_proxies(controller)
        except ApiError:
            return False, externally_changed
        group = live_proxies.get(group_name)
        if not isinstance(group, dict):
            return False, externally_changed
        current = group.get("now")
        if current == candidate_name:
            try:
                switch_group(controller, group_name, previous)
            except ApiError:
                return False, externally_changed
            expected[group_name] = previous
        elif current == previous:
            expected[group_name] = previous
        else:
            externally_changed.append(group_name)

    try:
        verify_selector_selections(controller, expected)
    except ApiError:
        return False, externally_changed
    return True, externally_changed


def runtime_slot_path(value: str) -> Path | None:
    if not value:
        return None
    candidate = Path(value).expanduser().absolute()
    for slot in RUNTIME_CONFIG_SLOTS:
        if candidate != slot.expanduser().absolute():
            continue
        if candidate.is_symlink() or not candidate.is_file():
            return None
        return candidate
    return None


def next_runtime_config_path(active_config_path: str) -> Path:
    active = runtime_slot_path(active_config_path)
    if active == RUNTIME_CONFIG_SLOTS[0].expanduser().absolute():
        return RUNTIME_CONFIG_SLOTS[1]
    return RUNTIME_CONFIG_SLOTS[0]


def detect_active_runtime_config(
    proxies: dict[str, Any],
    active_config_path: str,
) -> str:
    injected = {
        name
        for name in proxies
        if isinstance(name, str) and is_monitor_injected_name(name)
    }
    if not injected:
        return ""

    candidates: list[Path] = []
    active_slot = runtime_slot_path(active_config_path)
    if active_slot:
        candidates.append(active_slot)
    candidates.extend(slot for slot in RUNTIME_CONFIG_SLOTS if slot not in candidates)
    for path in candidates:
        if path.is_symlink() or not path.is_file():
            continue
        try:
            data = load_yaml_data(path)
        except ProfileError:
            continue
        slot_proxies = data.get("proxies") if isinstance(data, dict) else None
        names = {
            str(proxy.get("name") or "")
            for proxy in slot_proxies
            if isinstance(proxy, dict)
        } if isinstance(slot_proxies, list) else set()
        if names & injected:
            return str(path)
    return ""


def validate_runtime_config(core_path: Path, config_path: Path) -> None:
    try:
        completed = subprocess.run(
            [
                str(core_path),
                "-t",
                "-d",
                str(config_path.parent),
                "-f",
                str(config_path),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ProfileError(f"无法预检运行时配置：{exc}") from exc
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout or "未知错误").strip()
        raise ProfileError(f"运行时配置预检失败：{short(message, 240)}")


def load_runtime_config(controller: Controller, path: Path) -> None:
    api_json(
        controller,
        "/configs",
        timeout=8.0,
        method="PUT",
        payload={"path": str(path), "force": True},
    )


def restore_runtime_config(
    controller: Controller,
    rollback_path: Path,
    snapshot: dict[str, str],
    settle_seconds: float,
) -> None:
    load_runtime_config(controller, rollback_path)
    rollback_baseline = fetch_live_proxies(controller)
    if settle_seconds > 0:
        time.sleep(settle_seconds)
    reapply_selector_snapshot(
        controller,
        snapshot,
        baseline_proxies=rollback_baseline,
    )
    verify_selector_selections(controller, snapshot)


def switch_to_full_subscription_node(
    controller: Controller,
    mixed_port: int | None,
    results: list[DelayResult],
    all_profiles: AllProfilesRuntime,
    args: argparse.Namespace,
    route_chain: list[str],
    active_config_path: str = "",
    allowed_source_types: set[str] | None = None,
) -> SwitchOutcome:
    if args.no_auto_switch or args.current_only:
        return SwitchOutcome()
    if not mixed_port:
        log("  跨订阅切换：未找到 mixed-port，无法在提交前后验证，已跳过")
        return SwitchOutcome()

    usable = [
        result
        for result in results
        if result.ok
        and result.delay_ms is not None
        and result.api_name in all_profiles.targets
        and is_switch_candidate(all_profiles.targets[result.api_name], args)
        and (
            allowed_source_types is None
            or all_profiles.targets[result.api_name].source_type
            in allowed_source_types
        )
    ]
    if not usable:
        return SwitchOutcome()

    candidates = sorted_fast_switch_candidates(usable, args, all_profiles.targets)
    if len(candidates) < len(usable):
        log(
            f"  跨订阅切换：可信候选全部保留；免费候选仅探测 "
            f"{len(candidates)}/{len(usable)} 个"
        )

    base_path = base_config_path(args)
    active_runtime_path = runtime_slot_path(active_config_path)
    rollback_path = active_runtime_path or base_path
    reference_data = load_yaml_data(rollback_path)
    if not isinstance(reference_data, dict):
        raise ProfileError(f"参考配置不是有效对象：{rollback_path}")
    switch_base_data = strip_runtime_injections(reference_data)
    group_names = select_base_switch_groups(
        switch_base_data,
        args,
        getattr(args, "_codex_route_url", args.url),
        route_chain=route_chain,
    )

    initial_live_proxies = fetch_live_proxies(controller)
    has_injected_proxy = any(
        is_monitor_injected_name(name)
        for name in initial_live_proxies
        if isinstance(name, str)
    )
    if has_injected_proxy and active_runtime_path is None:
        log(
            "  跨订阅切换：检测到运行时注入节点，但找不到对应回滚配置；"
            "为避免覆盖当前状态，本轮不重载配置"
        )
        return SwitchOutcome()
    initial_configs_data = api_json(controller, "/configs", timeout=5.0)
    initial_configs = (
        initial_configs_data if isinstance(initial_configs_data, dict) else {}
    )
    compatible, reason = live_matches_reference_config(
        reference_data,
        initial_live_proxies,
        fetch_rules(controller),
        initial_configs,
    )
    if not compatible:
        log(
            "  跨订阅切换：当前运行配置与回滚参考不完全一致，"
            f"为避免覆盖 Clash 逻辑已跳过：{reason}"
        )
        return SwitchOutcome()

    candidate_path = next_runtime_config_path(
        str(active_runtime_path) if active_runtime_path else ""
    )

    for result in candidates:
        target = all_profiles.targets[result.api_name]
        if not target.profile_path:
            continue

        profile_path = Path(target.profile_path)
        if not profile_path.exists():
            log(f"  跨订阅切换：订阅文件不存在，跳过：{profile_path}")
            continue

        try:
            switch_group(all_profiles.controller, "ALL_SUBSCRIPTIONS", result.api_name)
        except ApiError as exc:
            log(f"  跨订阅切换：临时选择候选节点失败，跳过：{zh_error(str(exc))}")
            continue
        if args.deep_probe_settle > 0:
            time.sleep(args.deep_probe_settle)
        probe_ok, probe_message = guarded_comprehensive_route_check(
            args,
            all_profiles.mixed_port,
            attempts=1,
        )
        probe_line = (
            f"  候选综合探测：{target.subscription} / {target.name} "
            f"({'通过' if probe_ok else '失败'}) - {zh_error(probe_message)}"
        )
        log(green(probe_line) if probe_ok else red(probe_line))
        if probe_ok is None:
            raise codex_probe.ProbeError(probe_message, "blocked")
        if not probe_ok:
            continue

        try:
            source_data = load_yaml_data(profile_path)
            if not isinstance(source_data, dict):
                raise ProfileError(f"订阅配置不是有效对象：{profile_path}")
            switch_config, injected_name = merge_runtime_switch_config(
                switch_base_data,
                source_data,
                target,
                group_names,
            )
        except ProfileError as exc:
            log(f"  跨订阅切换：无法构建候选配置，跳过：{zh_error(str(exc))}")
            continue

        if args.dry_run_switch:
            log(
                f"  跨订阅切换：将把「{target.subscription} / {target.name}」"
                f"仅注入策略组 {', '.join(group_names)}；不会加载配置"
            )
            return SwitchOutcome()

        try:
            secure_write_text(
                candidate_path,
                json.dumps(switch_config, ensure_ascii=False, indent=2),
            )
            validate_runtime_config(all_profiles.core_path, candidate_path)
        except (OSError, ProfileError) as exc:
            log(f"  跨订阅切换：候选配置预检失败，跳过：{zh_error(str(exc))}")
            continue

        current_live_proxies = fetch_live_proxies(controller)
        current_configs_data = api_json(controller, "/configs", timeout=5.0)
        current_configs = (
            current_configs_data if isinstance(current_configs_data, dict) else {}
        )
        compatible, reason = live_matches_reference_config(
            reference_data,
            current_live_proxies,
            fetch_rules(controller),
            current_configs,
        )
        if not compatible:
            log(
                "  跨订阅切换：候选探测期间 Clash 配置发生变化，"
                f"已停止提交：{reason}"
            )
            return SwitchOutcome()

        snapshot = selector_group_snapshot(current_live_proxies)
        preserved = {
            group: previous
            for group, previous in snapshot.items()
            if group not in group_names
        }
        target_selections = {
            group_name: injected_name for group_name in group_names
        }
        candidate_selections = {**preserved, **target_selections}

        try:
            latest_reference_data = load_yaml_data(rollback_path)
        except ProfileError as exc:
            log(
                "  跨订阅切换：提交前无法重新读取回滚参考，已停止："
                f"{zh_error(str(exc))}"
            )
            return SwitchOutcome()
        if latest_reference_data != reference_data:
            log(
                "  跨订阅切换：候选探测期间回滚参考文件发生变化，"
                "为避免覆盖 DNS/TUN/节点等配置已停止提交"
            )
            return SwitchOutcome()
        latest_live_proxies = fetch_live_proxies(controller)
        if selector_group_snapshot(latest_live_proxies) != snapshot:
            log(
                "  跨订阅切换：提交前检测到 Selector 被外部修改，"
                "已停止且未覆盖"
            )
            return SwitchOutcome(route_ok=False, rollback_failed=True)

        load_attempted = False
        try:
            load_attempted = True
            load_runtime_config(controller, candidate_path)
            candidate_baseline = fetch_live_proxies(controller)
            if args.switch_settle > 0:
                time.sleep(args.switch_settle)
            reapply_selector_snapshot(
                controller,
                preserved,
                baseline_proxies=candidate_baseline,
            )
            reapply_selector_snapshot(
                controller,
                target_selections,
                baseline_proxies=candidate_baseline,
            )
        except ConcurrentSelectorChange as exc:
            log(
                "  跨订阅切换：提交期间检测到外部 Selector 变更，"
                f"已停止且未覆盖：{zh_error(str(exc))}"
            )
            return SwitchOutcome(route_ok=False, rollback_failed=True)
        except (ApiError, ProfileError, OSError) as exc:
            log(f"  跨订阅切换：提交候选失败：{zh_error(str(exc))}")
            if load_attempted:
                state, _live, state_reason = identify_live_config(
                    controller,
                    switch_config,
                    reference_data,
                )
                if state == "reference":
                    log(
                        "  跨订阅切换：候选未生效，当前仍是提交前配置；"
                        "继续尝试下一候选"
                    )
                    continue
                if state != "candidate":
                    detail = state_reason or state
                    log(
                        "  跨订阅切换：当前配置已不是监控器刚提交的候选，"
                        f"为避免覆盖外部变更已停止：{detail}"
                    )
                    return SwitchOutcome(
                        route_ok=False,
                        rollback_failed=True,
                    )
                try:
                    restore_runtime_config(
                        controller,
                        rollback_path,
                        snapshot,
                        args.switch_settle,
                    )
                    log("  跨订阅切换：已恢复提交前的配置和策略组选择")
                except (ApiError, ProfileError, OSError) as rollback_exc:
                    log(red(
                        "  跨订阅切换：回滚失败，已停止本轮自动切换："
                        f"{zh_error(str(rollback_exc))}"
                    ))
                    return SwitchOutcome(route_ok=False, rollback_failed=True)
            continue

        ok, message = guarded_comprehensive_route_check(args, mixed_port)
        log(
            f"  跨订阅切换复查：{'可用' if ok else '失败'} - "
            f"{zh_error(message)}"
        )
        state, live_after_probe, state_reason = identify_live_config(
            controller,
            switch_config,
            reference_data,
        )
        if state != "candidate":
            detail = (
                "已恢复为回滚参考配置"
                if state == "reference"
                else state_reason or state
            )
            log(
                "  跨订阅切换：复查期间配置被外部修改，"
                f"为避免覆盖已停止回滚：{detail}"
            )
            return SwitchOutcome(route_ok=False, rollback_failed=True)
        changed_groups = changed_selector_groups(
            live_after_probe,
            candidate_selections,
        )
        if changed_groups:
            log(
                "  跨订阅切换：复查期间 Selector 被外部修改，"
                f"未覆盖：{', '.join(changed_groups)}"
            )
            return SwitchOutcome(route_ok=False, rollback_failed=True)

        if ok:
            log(green(
                f"  跨订阅切换：已切换到「{target.subscription} / "
                f"{target.name}」({result.delay_ms} 毫秒)"
            ))
            return SwitchOutcome(
                route_ok=True,
                source_type=target.source_type,
                switched=True,
                config_path=str(candidate_path),
            )

        try:
            restore_runtime_config(
                controller,
                rollback_path,
                snapshot,
                args.switch_settle,
            )
            log("  跨订阅切换：复查失败，已恢复提交前的配置和策略组选择")
        except (ApiError, ProfileError, OSError) as exc:
            log(red(
                "  跨订阅切换：复查失败且回滚失败，已停止本轮自动切换："
                f"{zh_error(str(exc))}"
            ))
            return SwitchOutcome(route_ok=False, rollback_failed=True)

        if ok is None:
            raise codex_probe.ProbeError(message, "blocked")

    log("  跨订阅切换：候选节点基础连通可用，但综合探测或切换复查均未通过")
    return SwitchOutcome(route_ok=False)


def auto_switch_if_needed(
    controller: Controller,
    proxies: dict[str, Any],
    mixed_port: int | None,
    route_ok: bool | None,
    args: argparse.Namespace,
    route_chain: list[str],
    allowed_source_types: set[str] | None = None,
    trusted_names: set[str] | None = None,
    recovering_from_fallback: bool = False,
) -> SwitchOutcome:
    if args.no_auto_switch:
        return SwitchOutcome()
    if route_ok is not False and not recovering_from_fallback:
        return SwitchOutcome()
    if not mixed_port:
        log("  自动切换：未找到 mixed-port，无法验证和回滚，已跳过")
        return SwitchOutcome()

    if recovering_from_fallback:
        log("  优先级恢复：正在检查当前配置中可用的可信节点...")
    else:
        log("  自动切换：当前 ChatGPT 路由不可用，正在查找可连通 ChatGPT 的节点...")
    initial_selector_state = selector_group_snapshot(proxies)
    live_targets = classify_live_targets(
        collect_targets(proxies, include_groups=False),
        args,
        trusted_names=trusted_names,
    )
    targets = [
        target
        for target in live_targets
        if is_switch_candidate(target, args)
        and (
            allowed_source_types is None
            or target.source_type in allowed_source_types
        )
        and route_switch_groups(proxies, route_chain, args, target.api_name)
    ]
    if not targets:
        log("  自动切换：ChatGPT 当前路径的 Selector 中没有可切换候选节点")
        return SwitchOutcome()

    results = run_delay_checks(controller, targets, args)
    usable = [result for result in results if result.ok and result.delay_ms is not None]
    if not usable:
        log("  自动切换：当前已加载配置中，没有可切换且可连通 ChatGPT 的节点")
        return SwitchOutcome()

    target_metadata = {target.api_name: target for target in targets}
    candidates = sorted_fast_switch_candidates(usable, args, target_metadata)
    if len(candidates) < len(usable):
        log(
            f"  自动切换：可信候选全部保留；免费候选仅探测 "
            f"{len(candidates)}/{len(usable)} 个"
        )

    for result in candidates:
        try:
            current_proxies = fetch_live_proxies(controller)
        except ApiError as exc:
            log(f"  自动切换：无法刷新策略组状态：{zh_error(str(exc))}")
            return SwitchOutcome(route_ok=False)
        groups = route_switch_groups(
            current_proxies,
            route_chain,
            args,
            result.api_name,
        )
        if not groups:
            continue
        previous_by_group: dict[str, str] = {}
        for group in groups:
            previous = current_proxies.get(group, {}).get("now")
            if not isinstance(previous, str) or not previous:
                previous_by_group = {}
                break
            previous_by_group[group] = previous
        if len(previous_by_group) != len(groups):
            continue
        if all(previous == result.api_name for previous in previous_by_group.values()):
            continue
        externally_changed = [
            group
            for group, current in previous_by_group.items()
            if initial_selector_state.get(group) != current
        ]
        if externally_changed:
            log(
                "  自动切换：候选测试期间检测到外部策略组变更，"
                f"已停止且未覆盖：{', '.join(externally_changed)}"
            )
            return SwitchOutcome(route_ok=False, rollback_failed=True)

        if args.dry_run_switch:
            log(
                f"  自动切换：将切换 {', '.join(groups)} => "
                f"{result.name} ({result.delay_ms} 毫秒)"
            )
            return SwitchOutcome()

        switched: list[str] = []
        commit_external_change: list[str] = []
        for group in groups:
            try:
                latest_proxies = fetch_live_proxies(controller)
                latest_group = latest_proxies.get(group)
                if (
                    not isinstance(latest_group, dict)
                    or latest_group.get("now") != previous_by_group[group]
                ):
                    commit_external_change.append(group)
                    break
                switch_group(controller, group, result.api_name)
                switched.append(group)
            except ApiError as exc:
                log(f"  自动切换：策略组提交失败：{zh_error(str(exc))}")
                break

        if commit_external_change:
            if switched:
                rollback_ok, rollback_external = rollback_selectors_if_unchanged(
                    controller,
                    previous_by_group,
                    result.api_name,
                )
                if not rollback_ok:
                    log(red(
                        "  自动切换：提交期间检测到外部变更，且已提交部分的"
                        "回滚失败，已停止本轮自动切换"
                    ))
                    return SwitchOutcome(
                        route_ok=False,
                        rollback_failed=True,
                    )
                commit_external_change.extend(rollback_external)
            changed = ", ".join(dict.fromkeys(commit_external_change))
            log(
                "  自动切换：提交期间检测到外部策略组变更，"
                f"已停止且未覆盖：{changed}"
            )
            return SwitchOutcome(route_ok=False, rollback_failed=True)

        verification_failed = False
        try:
            verify_selector_selections(
                controller,
                {group: result.api_name for group in switched},
            )
        except ApiError as exc:
            log(f"  自动切换：策略组提交验证失败：{zh_error(str(exc))}")
            verification_failed = True

        if len(switched) != len(groups) or verification_failed:
            rollback_ok, externally_changed = rollback_selectors_if_unchanged(
                controller,
                previous_by_group,
                result.api_name,
            )
            if not rollback_ok:
                log(red("  自动切换：部分切换回滚失败，已停止本轮自动切换"))
                return SwitchOutcome(route_ok=False, rollback_failed=True)
            if externally_changed:
                log(
                    "  自动切换：检测到外部策略组变更，未覆盖："
                    f"{', '.join(externally_changed)}"
                )
            continue

        log(green(
            f"  自动切换：已切换 {', '.join(switched)} => "
            f"{result.name} ({result.delay_ms} 毫秒)"
        ))
        if args.deep_probe_settle > 0:
            time.sleep(args.deep_probe_settle)
        ok, message = guarded_comprehensive_route_check(
            args,
            mixed_port,
            attempts=1,
        )
        log(f"  自动切换复查：{'可用' if ok else '失败'} - {zh_error(message)}")
        if ok:
            target = target_metadata[result.api_name]
            return SwitchOutcome(
                route_ok=True,
                source_type=target.source_type,
                switched=True,
            )

        rollback_ok, externally_changed = rollback_selectors_if_unchanged(
            controller,
            previous_by_group,
            result.api_name,
        )
        if not rollback_ok:
            log(red("  自动切换：复查失败且策略组回滚失败，已停止本轮自动切换"))
            return SwitchOutcome(route_ok=False, rollback_failed=True)
        if externally_changed:
            log(
                "  自动切换：复查期间检测到外部策略组变更，未覆盖："
                f"{', '.join(externally_changed)}"
            )
        log("  自动切换：复查失败，已恢复原策略组选择")
        if ok is None:
            raise codex_probe.ProbeError(message, "blocked")

    log("  自动切换：候选节点基础连通可用，但没有节点通过综合探测")
    return SwitchOutcome(route_ok=False)


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
        if not isinstance(data, dict):
            return {}
        sanitized: dict[str, Any] = {}
        for source, value in data.items():
            key = (
                safe_url_label(source)
                if isinstance(source, str)
                and urlparse(source).scheme.lower() in {"http", "https"}
                else str(source)
            )
            sanitized[key] = value
        return sanitized
    except Exception:
        return {}


def save_free_stats(stats: dict[str, Any]) -> None:
    secure_write_text(
        DEFAULT_FREE_STATS_PATH,
        json.dumps(stats, ensure_ascii=False, indent=2),
    )


def update_free_stats(results: list[DelayResult], runtime: AllProfilesRuntime, args: argparse.Namespace) -> None:
    by_url: dict[str, list[DelayResult]] = {}
    for result in results:
        target = runtime.targets.get(result.api_name)
        if not target or target.source_type != "free":
            continue
        source = (
            safe_url_label(target.source_url)
            if target.source_url
            else target.subscription
        )
        by_url.setdefault(source, []).append(result)

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
    expected_fingerprint = profiles_fingerprint(profiles)
    if runtime is not None:
        process_alive = runtime.process is not None and runtime.process.poll() is None
        unchanged = runtime.profiles_fingerprint == expected_fingerprint
        if process_alive and unchanged:
            return runtime
        reason = "临时核心已退出" if not process_alive else "订阅内容已更新"
        log(f"{label}：{reason}，正在重建隔离测试核心。")
        runtime.close()
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
    log(f"{now} | Codex 真实连通性监控 | 模式={mode}")


def run_once(
    controller: Controller,
    version: Any,
    args: argparse.Namespace,
    hints: list[ConfigHint],
    owned_profiles: AllProfilesRuntime | None = None,
    free_profiles: AllProfilesRuntime | None = None,
    active_source_type: str = "owned",
    route_failure_streak: int = 0,
    active_config_path: str = "",
) -> RunOutcome:
    configs_data = api_json(controller, "/configs", timeout=5.0)
    configs = configs_data if isinstance(configs_data, dict) else {}
    proxies_data = api_json(controller, "/proxies", timeout=8.0)
    proxies = proxies_data.get("proxies", {}) if isinstance(proxies_data, dict) else {}
    if not isinstance(proxies, dict):
        proxies = {}
    active_config_path = detect_active_runtime_config(proxies, active_config_path)
    active_runtime_path = runtime_slot_path(active_config_path)
    if trusted_profile_uids(args):
        trusted_base_names = (
            explicitly_trusted_proxy_names(args)
            | verified_trusted_injected_names(args, active_runtime_path)
        )
    else:
        trusted_base_names = (
            proxy_names_from_config_path(active_runtime_path)
            if active_runtime_path
            else base_proxy_names_for_args(args)
        )

    mixed_port = mixed_port_from(args, configs, hints)
    print_header(controller, version, configs, mixed_port, args.url)
    route_ok: bool | None = None
    if mixed_port:
        route_ok, message = guarded_comprehensive_route_check(args, mixed_port)
        route_status = "通过" if route_ok is True else "受阻" if route_ok is None else "失败"
        route_line = f"{codex_probe.probe_label(args)}：{route_status} - {zh_error(message)}"
        log(route_line if route_ok else red(route_line))
    else:
        log("ChatGPT 路由：跳过 - 未找到 mixed-port；需要时可传 --proxy-port")
    route_failure_streak = advance_route_failure_streak(
        route_ok,
        route_failure_streak,
    )

    rules = fetch_rules(controller)
    route_chain = current_route_chain(proxies, configs, rules, getattr(args, "_codex_route_url", args.url))
    active_source_type = route_source_type(
        route_chain,
        args,
        trusted_names=trusted_base_names,
    )
    log(f"当前路径：{format_route_chain(route_chain)}")
    log()

    def finish(no_available: bool) -> RunOutcome:
        return RunOutcome(
            owned_profiles=owned_profiles,
            free_profiles=free_profiles,
            active_source_type=active_source_type,
            no_available_chatgpt_node=no_available,
            route_failure_streak=route_failure_streak,
            active_config_path=active_config_path,
            route_ok=route_ok,
        )

    if (
        route_ok is False
        and route_failure_streak < args.failure_threshold
        and not args.no_auto_switch
    ):
        log(
            f"连续失败 {route_failure_streak}/{args.failure_threshold}，"
            "本轮不切换；达到阈值后才启动候选测试，避免瞬时抖动。"
        )
        return finish(True)

    if route_ok is None:
        log("无法验证切换结果，本轮不测试或修改节点。")
        return finish(False)

    if route_ok is True:
        if (
            should_recover_trusted_route(active_source_type, args)
            and not args.current_only
        ):
            fallback_label = (
                "免费备用"
                if active_source_type == "free"
                else "未列入白名单的备用"
            )
            log(
                f"当前使用{fallback_label}节点；"
                "按优先级检查可信节点是否恢复..."
            )
            current_switch_outcome = auto_switch_if_needed(
                controller,
                proxies,
                mixed_port,
                route_ok,
                args,
                route_chain,
                allowed_source_types={"owned"},
                trusted_names=trusted_base_names,
                recovering_from_fallback=True,
            )
            if current_switch_outcome.route_ok is True:
                active_source_type = "owned"
                route_failure_streak = 0
                route_ok = True
                log(green("当前配置中的可信节点已恢复，已优先切回。"))
                return finish(False)
            if current_switch_outcome.rollback_failed:
                return finish(False)

            log("当前配置中的可信节点暂不可用；继续检查其他自有订阅...")
            owned_profiles = ensure_profiles_runtime(
                args,
                owned_profiles,
                discover_remote_profiles(args),
                "自有订阅",
            )
            owned_results = test_profiles_runtime(owned_profiles, args)
            print_results(owned_results, args)
            if any(
                result.ok
                and result.api_name in owned_profiles.targets
                and owned_profiles.targets[result.api_name].source_type
                == "owned"
                for result in owned_results
            ):
                switch_outcome = switch_to_full_subscription_node(
                    controller,
                    mixed_port,
                    owned_results,
                    owned_profiles,
                    args,
                    route_chain,
                    active_config_path,
                    allowed_source_types={"owned"},
                )
                if switch_outcome.route_ok is True:
                    active_source_type = "owned"
                    route_failure_streak = 0
                    route_ok = True
                    active_config_path = (
                        switch_outcome.config_path or active_config_path
                    )
                    if free_profiles is not None:
                        free_profiles.close()
                        free_profiles = None
                    log(green("自有订阅已恢复，已优先切回自有节点。"))
                    return finish(False)
                if switch_outcome.rollback_failed:
                    return finish(True)
            log(
                f"自有订阅暂未恢复，继续使用{fallback_label}节点，"
                "本轮跳过免费池全量测试。"
            )
            return finish(False)

        if not args.always_test_nodes:
            if owned_profiles is not None:
                owned_profiles.close()
                owned_profiles = None
            if free_profiles is not None:
                free_profiles.close()
                free_profiles = None
            log(f"当前 {codex_probe.probe_label(args)}通过，本轮跳过节点全量测试。")
            return finish(False)

    if args.current_only:
        log("节点范围：仅当前已加载配置")
        if route_ok is False:
            switch_outcome = auto_switch_if_needed(
                controller,
                proxies,
                mixed_port,
                route_ok,
                args,
                route_chain,
                trusted_names=trusted_base_names,
            )
            if switch_outcome.route_ok is True:
                active_source_type = (
                    switch_outcome.source_type or active_source_type
                )
                route_failure_streak = 0
                route_ok = True
                return finish(False)
            if switch_outcome.rollback_failed:
                return finish(True)
        targets = collect_targets(proxies, include_groups=args.include_groups)
        targets = filter_excluded_targets(targets, args, "当前配置")
        if not targets:
            log("没有在 /proxies 中找到可测试节点。")
            return finish(route_ok is False)
        results = run_delay_checks(controller, targets, args)
        print_results(results, args)
        return finish(route_ok is False)

    if route_ok is True:
        log("当前路由可用；按 --always-test-nodes 仅检查自有订阅，不执行切换。")
        owned_profiles = ensure_profiles_runtime(
            args,
            owned_profiles,
            discover_remote_profiles(args),
            "自有订阅",
        )
        owned_results = test_profiles_runtime(owned_profiles, args)
        print_results(owned_results, args)
        return finish(False)

    switch_outcome = auto_switch_if_needed(
        controller,
        proxies,
        mixed_port,
        route_ok,
        args,
        route_chain,
        allowed_source_types={"owned"},
        trusted_names=trusted_base_names,
    )
    if switch_outcome.route_ok is True:
        active_source_type = "owned"
        route_failure_streak = 0
        route_ok = True
        return finish(False)
    if switch_outcome.rollback_failed:
        return finish(True)

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
    trusted_has_available = any(
        result.ok
        and result.api_name in owned_profiles.targets
        and owned_profiles.targets[result.api_name].source_type == "owned"
        for result in owned_results
    )
    if trusted_has_available:
        switch_outcome = switch_to_full_subscription_node(
            controller,
            mixed_port,
            owned_results,
            owned_profiles,
            args,
            route_chain,
            active_config_path,
            allowed_source_types={"owned"},
        )
        if switch_outcome.source_type:
            active_source_type = switch_outcome.source_type
        if switch_outcome.route_ok is True:
            route_failure_streak = 0
            route_ok = True
            active_config_path = switch_outcome.config_path or active_config_path
            return finish(False)
        if switch_outcome.rollback_failed:
            return finish(True)
        if switch_outcome.route_ok is False:
            log("  自有订阅切换：发现可用节点，但切换后当前 ChatGPT 路由仍不可用")

    log("全部可信节点未恢复；现在检查当前已加载配置中的未知备用节点...")
    switch_outcome = auto_switch_if_needed(
        controller,
        proxies,
        mixed_port,
        route_ok,
        args,
        route_chain,
        allowed_source_types={"unknown"},
        trusted_names=trusted_base_names,
    )
    if switch_outcome.route_ok is True:
        active_source_type = switch_outcome.source_type or "unknown"
        route_failure_streak = 0
        route_ok = True
        return finish(False)
    if switch_outcome.rollback_failed:
        return finish(True)

    unknown_has_available = any(
        result.ok
        and result.api_name in owned_profiles.targets
        and owned_profiles.targets[result.api_name].source_type == "unknown"
        for result in owned_results
    )
    if unknown_has_available:
        log("可信节点仍不可用；开始尝试未列入可信白名单的本地订阅...")
        switch_outcome = switch_to_full_subscription_node(
            controller,
            mixed_port,
            owned_results,
            owned_profiles,
            args,
            route_chain,
            active_config_path,
            allowed_source_types={"unknown"},
        )
        if switch_outcome.source_type:
            active_source_type = switch_outcome.source_type
        if switch_outcome.route_ok is True:
            route_failure_streak = 0
            route_ok = True
            active_config_path = switch_outcome.config_path or active_config_path
            return finish(False)
        if switch_outcome.rollback_failed:
            return finish(True)

    if args.no_free_backup:
        log("免费备用已禁用，本轮不测试免费订阅。")
        return finish(True)

    log("可信和未知备用均不可用；现在才检查当前已加载配置中的免费节点...")
    switch_outcome = auto_switch_if_needed(
        controller,
        proxies,
        mixed_port,
        route_ok,
        args,
        route_chain,
        allowed_source_types={"free"},
        trusted_names=trusted_base_names,
    )
    if switch_outcome.route_ok is True:
        active_source_type = "free"
        route_failure_streak = 0
        route_ok = True
        return finish(False)
    if switch_outcome.rollback_failed:
        return finish(True)

    log("自有订阅暂未能恢复 ChatGPT，开始测试免费备用订阅...")
    free_profiles_list = discover_free_profiles(args)
    if not free_profiles_list:
        log("免费备用订阅不可用：没有可测试的免费源。")
        return finish(True)

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
            mixed_port,
            free_results,
            free_profiles,
            args,
            route_chain,
            active_config_path,
        )
        if switch_outcome.source_type:
            active_source_type = switch_outcome.source_type
        if switch_outcome.route_ok is True:
            route_failure_streak = 0
            route_ok = True
            active_config_path = switch_outcome.config_path or active_config_path
            return finish(False)
        if switch_outcome.rollback_failed:
            return finish(True)
        if switch_outcome.route_ok is False:
            log("  免费备用切换：发现可用节点，但切换后当前 ChatGPT 路由仍不可用")

    return finish(True)



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
    parser.add_argument(
        "--base-config",
        help="节点注入时使用的本地基础配置；默认使用 Clash Verge 的 clash-verge.yaml",
    )
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
        "--failure-threshold",
        type=positive_int,
        default=2,
        help="连续多少轮综合探测失败后才允许自动切换，默认 2",
    )
    parser.add_argument(
        "--startup-timeout",
        type=positive_int,
        default=15,
        help="等待临时全订阅核心启动的秒数",
    )
    parser.add_argument("--url", default=DEFAULT_TEST_URL, help="连通性测试地址")
    parser.add_argument("--codex-auth-mode", choices=("auto", "chatgpt", "api-key"), default="auto", help="默认优先使用本机 Codex 登录，未登录时读取 API Key")
    parser.add_argument("--codex-auth-file", help="Codex auth.json 路径；只读，不刷新或写回凭据")
    parser.add_argument("--probe-mode", choices=("network", "generation"), default="network", help="默认 network：鉴权/API/WS ping-pong，无模型生成；generation：真实 SSE 和 WS 生成")
    parser.add_argument("--stream-timeout", type=positive_int, default=45, help="每项真实 API/SSE/WS 探测的总时限秒数")
    parser.add_argument("--openai-api-url", help="真实模型列表接口；默认按 Codex 认证方式选择官方后端")
    parser.add_argument("--openai-sse-url", help="真实 SSE 接口；默认按 Codex 认证方式选择官方后端")
    parser.add_argument("--openai-websocket-url", help="真实 WebSocket 接口；默认按 Codex 认证方式选择官方后端")
    parser.add_argument(
        "--openai-stream-model",
        default=os.getenv("OPENAI_STREAM_TEST_MODEL"),
        help="generation 模式的模型；必须显式选择，避免自动使用桌面端的昂贵模型",
    )
    parser.add_argument(
        "--openai-api-key-env",
        default="OPENAI_API_KEY",
        help="API Key 登录模式读取的环境变量名",
    )
    parser.add_argument(
        "--strict-stream-probes",
        action="store_true",
        help="兼容参数，等同 --probe-mode generation；需要显式指定探测模型",
    )
    parser.add_argument("--quick-route-only", action="store_true", help="已停用；传入时报告配置错误，不能绕过真实验证")
    parser.add_argument("--skip-api-probe", action="store_true", help="已停用；不能绕过真实 API 验证")
    parser.add_argument("--skip-sse-probe", action="store_true", help="已停用；不能绕过真实 SSE 验证")
    parser.add_argument("--skip-websocket-probe", action="store_true", help="已停用；不能绕过真实 WebSocket 验证")
    parser.add_argument(
        "--deep-probe-fast-ms",
        type=nonnegative_int,
        default=0,
        help="优先探测不高于该延迟的节点（不是淘汰线）；0 表示使用 --slow",
    )
    parser.add_argument(
        "--deep-probe-max-candidates",
        type=nonnegative_int,
        default=8,
        help="免费/不可信候选每轮最多综合探测数量；可信候选不受限；0 表示不限制",
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
    parser.add_argument(
        "--trusted-profile-uid",
        action="append",
        help=(
            "明确视为可信的 Clash Verge 订阅 UID，可重复；"
            "设置后，未列入的本地订阅仅作为未知备用"
        ),
    )
    parser.add_argument("--show-errors", action="store_true", help="打印失败错误明细")
    parser.add_argument("--always-test-nodes", action="store_true", help="即使当前 ChatGPT 路由可用，也强制测试节点")
    parser.add_argument("--free-url", action="append", help="额外免费订阅地址，可重复传入")
    parser.add_argument(
        "--no-free-backup",
        action="store_true",
        help="禁用所有免费备用节点和免费订阅",
    )
    parser.add_argument("--free-cache-ttl", type=positive_int, default=1800, help="免费订阅缓存有效期秒数，默认 1800 秒")
    parser.add_argument("--free-fetch-timeout", type=positive_int, default=30, help="抓取免费订阅的超时时间秒数")
    parser.add_argument("--free-abandon-threshold", type=positive_int, default=5, help="免费源连续失败提醒阈值")
    parser.add_argument("--clear", action="store_true", help="每次刷新前清屏")
    parser.add_argument("--once", action="store_true", help="只检查一次后退出")
    parser.add_argument("--log-file", help="日志文件名或绝对路径；相对路径位于脚本目录的 logs/ 下，默认 clash-monitor.log；每份 5 MiB，保留 5 份备份")
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
    except (ApiError, ProfileError) as exc:
        log(f"错误：无法连接 Clash 控制接口：{zh_error(str(exc))}", file=sys.stderr)
        close_log_file()
        return 2

    owned_profiles: AllProfilesRuntime | None = None
    free_profiles: AllProfilesRuntime | None = None
    active_source_type = "owned"
    route_failure_streak = 0
    active_config_path = ""

    stop = False
    exit_code = 0

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
                    route_failure_streak=route_failure_streak,
                    active_config_path=active_config_path,
                )
                owned_profiles = outcome.owned_profiles
                free_profiles = outcome.free_profiles
                active_source_type = outcome.active_source_type or active_source_type
                route_failure_streak = outcome.route_failure_streak
                active_config_path = outcome.active_config_path
                fast_retry = outcome.no_available_chatgpt_node
                exit_code = 0 if outcome.route_ok is True else 5 if outcome.route_ok is None else 1
            except codex_probe.ProbeError as exc:
                exit_code = 5
                log(f"{codex_probe.probe_label(args)}受阻：{exc}")
                log("已停止本轮节点切换，下一周期重新读取登录状态。")
            except ApiError as exc:
                log("=" * 88)
                exit_code = 2
                log(f"控制接口错误：{zh_error(str(exc))}")
                try:
                    refreshed_hints = [
                        hint
                        for path in config_paths()
                        if (hint := read_config_hint(path))
                    ]
                    controller, version, _errors = find_controller(
                        args,
                        refreshed_hints,
                    )
                    hints = refreshed_hints
                    log(f"已重新发现 Clash 控制接口：{controller.label}")
                except (ApiError, ProfileError) as rediscover_exc:
                    log(
                        "控制接口重新发现失败，将在下个检查周期重试："
                        f"{zh_error(str(rediscover_exc))}"
                    )
                fast_retry = True
            except ProfileError as exc:
                log("=" * 88)
                exit_code = 3
                log(f"全订阅检查错误：{zh_error(str(exc))}")
                log("将在下个检查周期重试。")
                fast_retry = True
            except Exception as exc:  # Keep the monitor alive for transient local issues.
                log("=" * 88)
                exit_code = 5
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

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
