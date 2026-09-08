from __future__ import annotations

import importlib.util
import stat
import sys
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "monitor_clash_verge.py"
MODULE_NAME = "monitor_clash_verge_under_test"

SPEC = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - import setup guard
    raise RuntimeError(f"Unable to load {MODULE_PATH}")
monitor = importlib.util.module_from_spec(SPEC)
sys.modules[MODULE_NAME] = monitor
SPEC.loader.exec_module(monitor)


def args_for_candidates(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "deep_probe_fast_ms": 0,
        "slow": 1200,
        "deep_probe_max_candidates": 8,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def args_for_switch(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "no_auto_switch": False,
        "dry_run_switch": False,
        "deep_probe_settle": 0,
        "switch_group": None,
        "exclude_regex": "",
        "skip_candidate_regex": "",
        "deep_probe_fast_ms": 0,
        "slow": 1200,
        "deep_probe_max_candidates": 8,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def delay_result(
    api_name: str,
    delay_ms: int,
    *,
    subscription: str,
    ok: bool = True,
) -> object:
    return monitor.DelayResult(
        api_name=api_name,
        name=api_name,
        proxy_type="VLESS",
        subscription=subscription,
        ok=ok,
        delay_ms=delay_ms,
    )


def node_target(
    api_name: str,
    *,
    source_type: str,
    name: str | None = None,
) -> object:
    return monitor.NodeTarget(
        api_name=api_name,
        name=name or api_name,
        proxy_type="VLESS",
        subscription="paid" if source_type == "owned" else "public",
        source_type=source_type,
    )


class OfflineTestCase(unittest.TestCase):
    """Fail immediately if a pure-policy test attempts external I/O."""

    def setUp(self) -> None:
        super().setUp()

        def forbidden(operation: str):
            def fail(*_args: object, **_kwargs: object) -> object:
                raise AssertionError(f"offline unit test attempted {operation}")

            return fail

        patchers = [
            mock.patch.object(monitor, "log", return_value=None),
            mock.patch.object(monitor, "api_json", side_effect=forbidden("Clash API access")),
            mock.patch.object(monitor, "urlopen", side_effect=forbidden("network access")),
            mock.patch.object(
                monitor.socket,
                "create_connection",
                side_effect=forbidden("socket access"),
            ),
            mock.patch.object(
                monitor.subprocess,
                "Popen",
                side_effect=forbidden("subprocess creation"),
            ),
            mock.patch.object(
                monitor.subprocess,
                "run",
                side_effect=forbidden("subprocess execution"),
            ),
            mock.patch.object(
                monitor,
                "load_runtime_config",
                side_effect=forbidden("Clash configuration mutation"),
            ),
            mock.patch.object(
                monitor,
                "switch_group",
                side_effect=forbidden("Clash selector mutation"),
            ),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)


class MergeRuntimeSwitchConfigTests(OfflineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.base_config = {
            "mode": "rule",
            "mixed-port": 7897,
            "port": 7898,
            "socks-port": 7899,
            "redir-port": 0,
            "tproxy-port": 0,
            "external-controller": "127.0.0.1:9097",
            "external-controller-unix": "/tmp/verge/verge-mihomo.sock",
            "secret": "local-controller-secret",
            "dns": {
                "enable": True,
                "enhanced-mode": "fake-ip",
                "fake-ip-range": "198.18.0.1/16",
                "nameserver": ["https://dns.alidns.com/dns-query"],
                "fake-ip-filter": ["+.lan", "+.local"],
            },
            "tun": {
                "enable": True,
                "stack": "gvisor",
                "auto-route": True,
                "strict-route": False,
                "dns-hijack": ["any:53"],
            },
            "rules": [
                "DOMAIN-SUFFIX,openai.com,OpenAI",
                "DOMAIN-SUFFIX,chatgpt.com,OpenAI",
                "MATCH,Default",
            ],
            "rule-providers": {
                "private": {
                    "type": "http",
                    "behavior": "domain",
                    "url": "https://example.invalid/private.yaml",
                }
            },
            "sniffer": {"enable": True, "parse-pure-ip": True},
            "proxies": [
                {
                    "name": "Existing",
                    "type": "ss",
                    "server": "existing.invalid",
                    "port": 443,
                    "cipher": "aes-128-gcm",
                    "password": "existing",
                }
            ],
            "proxy-groups": [
                {
                    "name": "OpenAI",
                    "type": "select",
                    "proxies": ["Existing", "DIRECT"],
                },
                {
                    "name": "Default",
                    "type": "select",
                    "proxies": ["Existing", "DIRECT"],
                },
            ],
        }
        self.source_profile = {
            "mixed-port": 9999,
            "port": 9998,
            "socks-port": 9997,
            "redir-port": 9996,
            "tproxy-port": 9995,
            "external-controller": "0.0.0.0:9990",
            "external-controller-unix": "/tmp/untrusted.sock",
            "secret": "untrusted-secret",
            "dns": {
                "enable": False,
                "nameserver": ["203.0.113.53"],
            },
            "tun": {
                "enable": False,
                "stack": "system",
                "dns-hijack": [],
            },
            "rules": ["MATCH,Untrusted"],
            "proxies": [
                {
                    "name": "Trusted JP",
                    "type": "vless",
                    "server": "trusted.invalid",
                    "port": 443,
                    "uuid": "00000000-0000-0000-0000-000000000001",
                    "tls": True,
                },
                {
                    "name": "Decoy",
                    "type": "vless",
                    "server": "decoy.invalid",
                    "port": 443,
                    "uuid": "00000000-0000-0000-0000-000000000002",
                    "tls": True,
                },
            ],
            "proxy-groups": [
                {
                    "name": "Untrusted",
                    "type": "select",
                    "proxies": ["Trusted JP", "Decoy"],
                }
            ],
        }
        self.target = node_target(
            "paid / Trusted JP",
            source_type="owned",
            name="Trusted JP",
        )

    def merge(self) -> tuple[dict[str, object], str]:
        return monitor.merge_runtime_switch_config(
            self.base_config,
            self.source_profile,
            self.target,
            ["OpenAI"],
        )

    def test_untrusted_top_level_network_and_routing_settings_cannot_override_base(self) -> None:
        merged, _ = self.merge()

        protected_keys = (
            "rules",
            "dns",
            "tun",
            "mixed-port",
            "port",
            "socks-port",
            "redir-port",
            "tproxy-port",
            "external-controller",
            "external-controller-unix",
            "secret",
            "rule-providers",
            "sniffer",
        )
        for key in protected_keys:
            with self.subTest(key=key):
                self.assertEqual(self.base_config[key], merged[key])

    def test_only_target_proxy_is_injected_into_requested_selector(self) -> None:
        merged, injected_name = self.merge()

        proxy_names = [proxy["name"] for proxy in merged["proxies"]]
        self.assertCountEqual(["Existing", injected_name], proxy_names)
        self.assertNotIn("Decoy", proxy_names)

        groups = {group["name"]: group for group in merged["proxy-groups"]}
        self.assertIn(injected_name, groups["OpenAI"]["proxies"])
        self.assertNotIn(injected_name, groups["Default"]["proxies"])
        self.assertNotIn("Untrusted", groups)

        injected = next(
            proxy for proxy in merged["proxies"] if proxy["name"] == injected_name
        )
        self.assertEqual("trusted.invalid", injected["server"])

    def test_merge_does_not_mutate_either_input(self) -> None:
        base_before = deepcopy(self.base_config)
        source_before = deepcopy(self.source_profile)

        self.merge()

        self.assertEqual(base_before, self.base_config)
        self.assertEqual(source_before, self.source_profile)


class RuntimeConfigIntegrityTests(OfflineTestCase):
    @staticmethod
    def reference_config() -> dict[str, object]:
        return {
            "mode": "rule",
            "mixed-port": 7897,
            "port": 0,
            "socks-port": 0,
            "redir-port": 0,
            "tproxy-port": 0,
            "allow-lan": False,
            "ipv6": False,
            "dns": {
                "enable": True,
                "enhanced-mode": "fake-ip",
                "fake-ip-range": "198.18.0.1/16",
            },
            "tun": {
                "enable": True,
                "stack": "gvisor",
                "dns-hijack": ["any:53"],
            },
            "proxies": [
                {"name": "NodeA", "type": "vless"},
                {"name": "NodeB", "type": "vless"},
            ],
            "proxy-groups": [
                {
                    "name": "OpenAI",
                    "type": "select",
                    "proxies": ["NodeA", "NodeB"],
                },
                {
                    "name": "Default",
                    "type": "select",
                    "proxies": ["NodeB", "NodeA"],
                },
            ],
            "rules": [
                "DOMAIN-SUFFIX,openai.com,OpenAI",
                "MATCH,Default",
            ],
        }

    @staticmethod
    def matching_live_proxies() -> dict[str, object]:
        return {
            "OpenAI": {
                "type": "Selector",
                "all": ["NodeA", "NodeB"],
                "now": "NodeA",
            },
            "Default": {
                "type": "Selector",
                "all": ["NodeB", "NodeA"],
                "now": "NodeB",
            },
            "GLOBAL": {
                "type": "Selector",
                "all": ["DIRECT", "OpenAI"],
                "now": "DIRECT",
            },
            "NodeA": {"type": "VLESS"},
            "NodeB": {"type": "VLESS"},
            "DIRECT": {"type": "Direct"},
        }

    @staticmethod
    def matching_live_rules() -> list[dict[str, object]]:
        return [
            {
                "type": "DomainSuffix",
                "payload": "openai.com",
                "proxy": "OpenAI",
            },
            {
                "type": "Match",
                "payload": "",
                "proxy": "Default",
            },
        ]

    @staticmethod
    def matching_live_configs() -> dict[str, object]:
        return {
            "mode": "rule",
            "mixed-port": 7897,
            "port": 0,
            "socks-port": 0,
            "redir-port": 0,
            "tproxy-port": 0,
            "allow-lan": False,
            "ipv6": False,
        }

    @classmethod
    def candidate_config(cls) -> dict[str, object]:
        candidate = cls.reference_config()
        injected_name = "可信备用 / paid / candidate [12345678]"
        candidate["proxies"].append(
            {"name": injected_name, "type": "vless"}
        )
        candidate["proxy-groups"][0]["proxies"].append(injected_name)
        return candidate

    @classmethod
    def candidate_live_proxies(cls) -> dict[str, object]:
        live = cls.matching_live_proxies()
        injected_name = "可信备用 / paid / candidate [12345678]"
        live["OpenAI"]["all"].append(injected_name)
        live[injected_name] = {"type": "VLESS"}
        return live

    def identify_live_state(
        self,
        live_proxies: dict[str, object],
        live_rules: list[dict[str, object]],
        live_configs: dict[str, object],
    ) -> tuple[str, dict[str, object], str]:
        controller = monitor.Controller(base_url="http://127.0.0.1:9090")

        def api_response(
            _controller: object,
            path: str,
            **_kwargs: object,
        ) -> object:
            if path == "/configs":
                return live_configs
            if path == "/rules":
                return {"rules": live_rules}
            raise AssertionError(f"unexpected offline API path: {path}")

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                return_value=live_proxies,
            ),
            mock.patch.object(
                monitor,
                "api_json",
                side_effect=api_response,
            ),
        ):
            return monitor.identify_live_config(
                controller,
                self.candidate_config(),
                self.reference_config(),
            )

    def test_strip_runtime_injections_only_removes_monitor_nodes_and_group_references(
        self,
    ) -> None:
        trusted_injected = "可信备用 / paid / node [11111111]"
        free_injected = "免费备用 / public / node [22222222]"
        unknown_injected = "候选备用 / local / node [33333333]"
        user_trusted_prefix = "可信备用 / 用户自定义"
        user_free_prefix = "免费备用 / 用户自定义"
        config = self.reference_config()
        config["proxies"] = [
            *config["proxies"],
            {"name": trusted_injected, "type": "vless"},
            {"name": free_injected, "type": "vless"},
            {"name": unknown_injected, "type": "vless"},
            {"name": user_trusted_prefix, "type": "vless"},
            {"name": user_free_prefix, "type": "vless"},
            {"name": "免费源 OrdinaryName", "type": "vless"},
        ]
        config["proxy-groups"][0]["proxies"].extend(
            [
                trusted_injected,
                free_injected,
                unknown_injected,
                user_trusted_prefix,
                user_free_prefix,
                "免费源 OrdinaryName",
            ]
        )
        config["proxy-groups"][1]["proxies"].extend(
            [
                trusted_injected,
                unknown_injected,
                user_trusted_prefix,
                user_free_prefix,
            ]
        )
        before = deepcopy(config)

        cleaned = monitor.strip_runtime_injections(config)

        self.assertEqual(before, config)
        self.assertEqual(
            [
                "NodeA",
                "NodeB",
                user_trusted_prefix,
                user_free_prefix,
                "免费源 OrdinaryName",
            ],
            [proxy["name"] for proxy in cleaned["proxies"]],
        )
        groups = {group["name"]: group for group in cleaned["proxy-groups"]}
        self.assertEqual(
            [
                "NodeA",
                "NodeB",
                user_trusted_prefix,
                user_free_prefix,
                "免费源 OrdinaryName",
            ],
            groups["OpenAI"]["proxies"],
        )
        self.assertEqual(
            [
                "NodeB",
                "NodeA",
                user_trusted_prefix,
                user_free_prefix,
            ],
            groups["Default"]["proxies"],
        )
        for key in (
            "rules",
            "dns",
            "tun",
            "mode",
            "mixed-port",
            "port",
            "socks-port",
            "redir-port",
            "tproxy-port",
        ):
            with self.subTest(key=key):
                self.assertEqual(before[key], cleaned[key])

    def test_trusted_profile_uid_allowlist_does_not_promote_other_profiles(
        self,
    ) -> None:
        args = SimpleNamespace(trusted_profile_uid=["paid-a", "paid-b"])

        self.assertEqual(
            "owned",
            monitor.local_profile_source_type(args, "paid-a"),
        )
        self.assertEqual(
            "unknown",
            monitor.local_profile_source_type(args, "public-c"),
        )
        self.assertEqual(
            "owned",
            monitor.local_profile_source_type(
                SimpleNamespace(trusted_profile_uid=None),
                "legacy-default",
            ),
        )

    def test_trusted_proxy_name_collision_is_treated_as_unknown(self) -> None:
        trusted = monitor.RemoteProfile(
            uid="paid-a",
            name="Paid",
            path=Path("/offline/paid.yaml"),
            source_type="owned",
        )
        unlisted = monitor.RemoteProfile(
            uid="public-b",
            name="Public",
            path=Path("/offline/public.yaml"),
            source_type="unknown",
        )

        def data_for(path: Path) -> dict[str, object]:
            if path == trusted.path:
                return {
                    "proxies": [
                        {"name": "PaidOnly"},
                        {"name": "SameName"},
                    ]
                }
            if path == unlisted.path:
                return {
                    "proxies": [
                        {"name": "PublicOnly"},
                        {"name": "SameName"},
                    ]
                }
            raise AssertionError(f"unexpected profile path: {path}")

        with (
            mock.patch.object(
                monitor,
                "discover_remote_profiles",
                return_value=[trusted, unlisted],
            ),
            mock.patch.object(
                monitor,
                "load_yaml_data",
                side_effect=data_for,
            ),
        ):
            names = monitor.explicitly_trusted_proxy_names(
                SimpleNamespace(trusted_profile_uid=["paid-a"])
            )

        self.assertEqual({"PaidOnly"}, names)

    def test_unreadable_unlisted_profile_disables_bare_name_trust(self) -> None:
        profiles = [
            monitor.RemoteProfile(
                uid="paid-a",
                name="Paid",
                path=Path("/offline/paid.yaml"),
                source_type="owned",
            ),
            monitor.RemoteProfile(
                uid="public-b",
                name="Public",
                path=Path("/offline/public.yaml"),
                source_type="unknown",
            ),
        ]

        def data_or_fail(path: Path) -> dict[str, object]:
            if path == profiles[0].path:
                return {"proxies": [{"name": "SameName"}]}
            raise monitor.ProfileError("offline unreadable profile")

        with (
            mock.patch.object(
                monitor,
                "discover_remote_profiles",
                return_value=profiles,
            ),
            mock.patch.object(
                monitor,
                "load_yaml_data",
                side_effect=data_or_fail,
            ),
        ):
            names = monitor.explicitly_trusted_proxy_names(
                SimpleNamespace(trusted_profile_uid=["paid-a"])
            )

        self.assertEqual(set(), names)

    def test_duplicate_or_missing_profile_demotes_all_allowlisted_profiles(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profiles_yaml = root / "profiles.yaml"
            profile_dir = root / "profiles"
            profile_dir.mkdir()
            profiles_yaml.write_text("items: []\n", encoding="utf-8")
            (profile_dir / "a.yaml").write_text("proxies: []\n", encoding="utf-8")
            (profile_dir / "b.yaml").write_text("proxies: []\n", encoding="utf-8")
            args = SimpleNamespace(
                profiles_yaml=str(profiles_yaml),
                trusted_profile_uid=["paid-a"],
            )

            cases = (
                [
                    {
                        "type": "remote",
                        "uid": "paid-a",
                        "file": "a.yaml",
                    },
                    {
                        "type": "remote",
                        "uid": "paid-a",
                        "file": "b.yaml",
                    },
                ],
                [
                    {
                        "type": "remote",
                        "uid": "paid-a",
                        "file": "a.yaml",
                    },
                    {
                        "type": "remote",
                        "uid": "public-b",
                        "file": "missing.yaml",
                    },
                ],
                [
                    {
                        "type": "remote",
                        "file": "a.yaml",
                    },
                ],
            )

            for items in cases:
                with (
                    self.subTest(items=items),
                    mock.patch.object(
                        monitor,
                        "load_yaml_data",
                        return_value={"items": items},
                    ),
                ):
                    profiles = monitor.discover_remote_profiles(args)
                    self.assertTrue(profiles)
                    self.assertEqual(
                        {"unknown"},
                        {profile.source_type for profile in profiles},
                    )

    def test_profile_file_stem_cannot_impersonate_allowlisted_uid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profile_dir = root / "profiles"
            profile_dir.mkdir()
            (profile_dir / "paid-a.yaml").write_text(
                "proxies:\n  - name: Public\n    type: vless\n",
                encoding="utf-8",
            )
            args = SimpleNamespace(
                profiles_yaml=str(root / "missing-profiles.yaml"),
                trusted_profile_uid=["paid-a"],
            )

            with mock.patch.object(
                monitor,
                "load_yaml_data",
                return_value={
                    "proxies": [
                        {"name": "Public", "type": "vless"},
                    ]
                },
            ):
                profiles = monitor.discover_remote_profiles(args)

        self.assertEqual(1, len(profiles))
        self.assertEqual("unknown", profiles[0].source_type)

    def test_source_classification_change_rebuilds_cached_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            profile_path = Path(temp_dir) / "profile.yaml"
            profile_path.write_text("proxies: []\n", encoding="utf-8")
            trusted = monitor.RemoteProfile(
                uid="paid-a",
                name="Paid",
                path=profile_path,
                source_type="owned",
            )
            demoted = monitor.RemoteProfile(
                uid="paid-a",
                name="Paid",
                path=profile_path,
                source_type="unknown",
            )
            trusted_fingerprint = monitor.profiles_fingerprint([trusted])
            demoted_fingerprint = monitor.profiles_fingerprint([demoted])
            self.assertNotEqual(trusted_fingerprint, demoted_fingerprint)

            process = mock.Mock()
            process.poll.return_value = None
            existing = SimpleNamespace(
                process=process,
                profiles_fingerprint=trusted_fingerprint,
                close=mock.Mock(),
            )
            replacement = object()

            with mock.patch.object(
                monitor,
                "start_all_profiles_runtime",
                return_value=replacement,
            ) as start:
                result = monitor.ensure_profiles_runtime(
                    SimpleNamespace(),
                    existing,
                    [demoted],
                    "offline",
                )

        existing.close.assert_called_once_with()
        start.assert_called_once()
        self.assertIs(replacement, result)

    def test_live_matches_identical_reference_and_normalizes_select_type(self) -> None:
        matches, reason = monitor.live_matches_reference_config(
            self.reference_config(),
            self.matching_live_proxies(),
            self.matching_live_rules(),
            self.matching_live_configs(),
        )

        self.assertTrue(matches)
        self.assertEqual("", reason)

    def test_live_match_rejects_rule_drift(self) -> None:
        rules = self.matching_live_rules()
        rules[0]["proxy"] = "Default"

        matches, reason = monitor.live_matches_reference_config(
            self.reference_config(),
            self.matching_live_proxies(),
            rules,
            self.matching_live_configs(),
        )

        self.assertFalse(matches)
        self.assertIn("规则", reason)

    def test_live_match_rejects_group_member_drift(self) -> None:
        live_proxies = self.matching_live_proxies()
        live_proxies["OpenAI"]["all"] = ["NodeA"]

        matches, reason = monitor.live_matches_reference_config(
            self.reference_config(),
            live_proxies,
            self.matching_live_rules(),
            self.matching_live_configs(),
        )

        self.assertFalse(matches)
        self.assertIn("成员", reason)

    def test_live_match_rejects_port_drift(self) -> None:
        live_configs = self.matching_live_configs()
        live_configs["mixed-port"] = 9000

        matches, reason = monitor.live_matches_reference_config(
            self.reference_config(),
            self.matching_live_proxies(),
            self.matching_live_rules(),
            live_configs,
        )

        self.assertFalse(matches)
        self.assertIn("mixed-port", reason)

    def test_live_match_rejects_dns_or_tun_drift_when_api_exposes_it(
        self,
    ) -> None:
        for key, live_value in (
            ("dns", {"enable": False}),
            ("tun", {"enable": False}),
        ):
            with self.subTest(key=key):
                live_configs = self.matching_live_configs()
                live_configs[key] = live_value

                matches, reason = monitor.live_matches_reference_config(
                    self.reference_config(),
                    self.matching_live_proxies(),
                    self.matching_live_rules(),
                    live_configs,
                )

                self.assertFalse(matches)
                self.assertIn(key, reason)

    def test_live_match_rejects_proxy_type_drift(self) -> None:
        live_proxies = self.matching_live_proxies()
        live_proxies["NodeA"]["type"] = "Trojan"

        matches, reason = monitor.live_matches_reference_config(
            self.reference_config(),
            live_proxies,
            self.matching_live_rules(),
            self.matching_live_configs(),
        )

        self.assertFalse(matches)
        self.assertIn("节点类型", reason)

    def test_identify_live_config_recognizes_candidate(self) -> None:
        state, live, reason = self.identify_live_state(
            self.candidate_live_proxies(),
            self.matching_live_rules(),
            self.matching_live_configs(),
        )

        self.assertEqual("candidate", state)
        self.assertEqual(self.candidate_live_proxies(), live)
        self.assertEqual("", reason)

    def test_identify_live_config_recognizes_reference_after_candidate_mismatch(
        self,
    ) -> None:
        state, live, reason = self.identify_live_state(
            self.matching_live_proxies(),
            self.matching_live_rules(),
            self.matching_live_configs(),
        )

        self.assertEqual("reference", state)
        self.assertEqual(self.matching_live_proxies(), live)
        self.assertEqual("", reason)

    def test_identify_live_config_recognizes_external_drift(self) -> None:
        external_rules = self.matching_live_rules()
        external_rules[0]["proxy"] = "External"

        state, live, reason = self.identify_live_state(
            self.matching_live_proxies(),
            external_rules,
            self.matching_live_configs(),
        )

        self.assertEqual("external", state)
        self.assertEqual(self.matching_live_proxies(), live)
        self.assertIn("候选不匹配", reason)
        self.assertIn("回滚参考不匹配", reason)

    def test_explicit_switch_group_is_rejected_when_route_is_unknown(self) -> None:
        with self.assertRaises(monitor.ProfileError):
            monitor.select_base_switch_groups(
                self.reference_config(),
                SimpleNamespace(switch_group="OpenAI"),
                "https://chatgpt.com/cdn-cgi/trace",
                route_chain=["rule", "未知"],
            )

    def test_runtime_slot_path_accepts_only_real_slot_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            slot_a = temp_path / "runtime-a.yaml"
            slot_a.write_text("slot: a\n", encoding="utf-8")
            real_b = temp_path / "real-b.yaml"
            real_b.write_text("slot: b\n", encoding="utf-8")
            slot_b_symlink = temp_path / "runtime-b.yaml"
            slot_b_symlink.symlink_to(real_b)
            unrelated = temp_path / "unrelated.yaml"
            unrelated.write_text("not: a-slot\n", encoding="utf-8")

            with mock.patch.object(
                monitor,
                "RUNTIME_CONFIG_SLOTS",
                (slot_a, slot_b_symlink),
            ):
                self.assertEqual(
                    slot_a.absolute(),
                    monitor.runtime_slot_path(str(slot_a)),
                )
                self.assertIsNone(
                    monitor.runtime_slot_path(str(slot_b_symlink))
                )
                self.assertIsNone(
                    monitor.runtime_slot_path(str(unrelated))
                )


class ConfigRuleTargetTests(OfflineTestCase):
    def test_exact_domain_rule_wins(self) -> None:
        rules = [
            "DOMAIN,chatgpt.com,ChatGPT",
            "MATCH,Default",
        ]
        self.assertEqual(
            "ChatGPT",
            monitor.config_rule_target_for_url(
                rules,
                "https://chatgpt.com/backend-api/conversation",
            ),
        )

    def test_domain_suffix_matches_subdomain(self) -> None:
        rules = [
            "DOMAIN-SUFFIX,openai.com,OpenAI",
            "MATCH,Default",
        ]
        self.assertEqual(
            "OpenAI",
            monitor.config_rule_target_for_url(
                rules,
                "https://api.openai.com/v1/responses",
            ),
        )

    def test_first_matching_rule_has_priority(self) -> None:
        rules = [
            "DOMAIN,api.openai.com,Specific",
            "DOMAIN-SUFFIX,openai.com,General",
            "MATCH,Default",
        ]
        self.assertEqual(
            "Specific",
            monitor.config_rule_target_for_url(
                rules,
                "https://api.openai.com/v1/models",
            ),
        )

    def test_match_rule_is_fallback(self) -> None:
        rules = [
            "DOMAIN-SUFFIX,openai.com,OpenAI",
            "MATCH,Default",
        ]
        self.assertEqual(
            "Default",
            monitor.config_rule_target_for_url(
                rules,
                "https://example.com/",
            ),
        )


class CandidatePolicyTests(OfflineTestCase):
    def test_owned_nodes_are_ranked_before_faster_free_nodes_without_1200ms_cutoff(
        self,
    ) -> None:
        results = [
            delay_result("free-fast", 250, subscription="public"),
            delay_result("owned-slow", 1600, subscription="paid"),
            delay_result("owned-fast", 900, subscription="paid"),
        ]
        targets = {
            "free-fast": node_target("free-fast", source_type="free"),
            "owned-slow": node_target("owned-slow", source_type="owned"),
            "owned-fast": node_target("owned-fast", source_type="owned"),
        }

        ranked = monitor.sorted_fast_switch_candidates(
            results,
            args_for_candidates(),
            targets,
        )

        self.assertEqual(
            ["owned-fast", "owned-slow", "free-fast"],
            [result.api_name for result in ranked],
        )

    def test_owned_node_above_1200ms_remains_eligible(self) -> None:
        results = [delay_result("owned-only", 1800, subscription="paid")]
        targets = {
            "owned-only": node_target("owned-only", source_type="owned"),
        }

        ranked = monitor.sorted_fast_switch_candidates(
            results,
            args_for_candidates(),
            targets,
        )

        self.assertEqual(["owned-only"], [result.api_name for result in ranked])


class RouteFailureStreakTests(OfflineTestCase):
    def test_failure_increments_streak(self) -> None:
        self.assertEqual(3, monitor.advance_route_failure_streak(False, 2))

    def test_success_resets_streak(self) -> None:
        self.assertEqual(0, monitor.advance_route_failure_streak(True, 7))

    def test_unknown_result_preserves_streak(self) -> None:
        self.assertEqual(4, monitor.advance_route_failure_streak(None, 4))


class RouteSwitchGroupTests(OfflineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.proxies = {
            "OpenAI": {
                "type": "Selector",
                "now": "Old",
                "all": ["Old", "Candidate"],
            },
            "Nested": {
                "type": "Selector",
                "now": "Old",
                "all": ["Old", "Candidate"],
            },
            "FallbackOnRoute": {
                "type": "Fallback",
                "now": "Old",
                "all": ["Old", "Candidate"],
            },
            "OffRoute": {
                "type": "Selector",
                "now": "Old",
                "all": ["Old", "Candidate"],
            },
            "MissingCandidate": {
                "type": "Selector",
                "now": "Old",
                "all": ["Old"],
            },
        }

    def test_only_selector_groups_on_resolved_route_are_returned(self) -> None:
        groups = monitor.route_switch_groups(
            self.proxies,
            [
                "rule",
                "OpenAI",
                "Nested",
                "FallbackOnRoute",
                "MissingCandidate",
                "Old",
            ],
            SimpleNamespace(switch_group=None),
            "Candidate",
        )

        self.assertEqual(["OpenAI", "Nested"], groups)

    def test_selector_outside_route_is_not_returned_even_if_explicitly_requested(
        self,
    ) -> None:
        groups = monitor.route_switch_groups(
            self.proxies,
            ["rule", "OpenAI", "Old"],
            SimpleNamespace(switch_group="OffRoute"),
            "Candidate",
        )

        self.assertEqual([], groups)

    def test_direct_route_has_no_switchable_group(self) -> None:
        groups = monitor.route_switch_groups(
            self.proxies,
            ["direct", "DIRECT"],
            SimpleNamespace(switch_group=None),
            "Candidate",
        )

        self.assertEqual([], groups)


class RuntimeSwitchTransactionTests(OfflineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.controller = monitor.Controller(base_url="http://127.0.0.1:9090")

    @staticmethod
    def route_proxies(*candidate_names: str) -> dict[str, object]:
        all_names = ["Old", *candidate_names]
        proxies: dict[str, object] = {
            "OpenAI": {
                "type": "Selector",
                "now": "Old",
                "all": all_names,
            },
            "Old": {"type": "VLESS"},
        }
        for candidate_name in candidate_names:
            proxies[candidate_name] = {"type": "VLESS"}
        return proxies

    @staticmethod
    def proxies_with_now(
        proxies: dict[str, object],
        node_name: str,
    ) -> dict[str, object]:
        live = deepcopy(proxies)
        group = live["OpenAI"]
        assert isinstance(group, dict)
        members = group["all"]
        assert isinstance(members, list)
        if node_name not in members:
            members.append(node_name)
        group["now"] = node_name
        live.setdefault(node_name, {"type": "VLESS"})
        return live

    def test_restore_runtime_config_orders_load_reapply_then_verify(self) -> None:
        events: list[tuple[str, object]] = []
        snapshot = {"OpenAI": "Old", "Default": "Existing"}
        rollback_path = Path("/offline/rollback.yaml")

        with (
            mock.patch.object(
                monitor,
                "load_runtime_config",
                side_effect=lambda controller, path: events.append(
                    ("load", (controller, path))
                ),
            ),
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                return_value={
                    "OpenAI": {
                        "type": "Selector",
                        "now": "Old",
                        "all": ["Old"],
                    },
                    "Default": {
                        "type": "Selector",
                        "now": "Existing",
                        "all": ["Existing"],
                    },
                },
            ),
            mock.patch.object(
                monitor,
                "reapply_selector_snapshot",
                side_effect=lambda controller, selections, **_kwargs: events.append(
                    ("reapply", (controller, selections))
                ),
            ),
            mock.patch.object(
                monitor,
                "verify_selector_selections",
                side_effect=lambda controller, selections: events.append(
                    ("verify", (controller, selections))
                ),
            ),
        ):
            monitor.restore_runtime_config(
                self.controller,
                rollback_path,
                snapshot,
                settle_seconds=0,
            )

        self.assertEqual(["load", "reapply", "verify"], [event[0] for event in events])
        for _, (controller, argument) in events:
            self.assertIs(self.controller, controller)
            self.assertEqual(
                rollback_path if isinstance(argument, Path) else snapshot,
                argument,
            )

    def test_restore_captures_baseline_before_settle_wait(self) -> None:
        events: list[str] = []
        baseline = {
            "OpenAI": {
                "type": "Selector",
                "now": "Default",
                "all": ["Default", "Old"],
            }
        }

        with (
            mock.patch.object(
                monitor,
                "load_runtime_config",
                side_effect=lambda *_args: events.append("load"),
            ),
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                side_effect=lambda *_args: (
                    events.append("baseline") or baseline
                ),
            ),
            mock.patch.object(
                monitor.time,
                "sleep",
                side_effect=lambda *_args: events.append("settle"),
            ),
            mock.patch.object(
                monitor,
                "reapply_selector_snapshot",
                side_effect=lambda _controller, _snapshot, **kwargs: (
                    self.assertIs(kwargs["baseline_proxies"], baseline),
                    events.append("reapply"),
                ),
            ),
            mock.patch.object(
                monitor,
                "verify_selector_selections",
                side_effect=lambda *_args: events.append("verify"),
            ),
        ):
            monitor.restore_runtime_config(
                self.controller,
                Path("/offline/rollback.yaml"),
                {"OpenAI": "Old"},
                settle_seconds=2,
            )

        self.assertEqual(
            ["load", "baseline", "settle", "reapply", "verify"],
            events,
        )

    def test_reapply_snapshot_stops_if_later_selector_changes(self) -> None:
        baseline = {
            "First": {
                "type": "Selector",
                "now": "OldFirst",
                "all": ["OldFirst", "Candidate"],
            },
            "Second": {
                "type": "Selector",
                "now": "OldSecond",
                "all": ["OldSecond", "Manual"],
            },
        }
        second_changed = deepcopy(baseline)
        second_changed["Second"]["now"] = "Manual"
        write_attempt = mock.Mock(
            side_effect=AssertionError("external selector change was overwritten")
        )

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                side_effect=[baseline, baseline, second_changed],
            ),
            mock.patch.object(monitor, "switch_group", new=write_attempt),
            self.assertRaises(monitor.ConcurrentSelectorChange),
        ):
            monitor.reapply_selector_snapshot(
                self.controller,
                {
                    "First": "OldFirst",
                    "Second": "OldSecond",
                },
            )

        write_attempt.assert_not_called()

    def test_allowlist_does_not_trust_a_forged_injected_name(self) -> None:
        forged = "可信备用 / public / node [deadbeef]"
        args = SimpleNamespace(trusted_profile_uid=["paid-a"])
        targets = [
            monitor.NodeTarget(
                api_name=forged,
                name=forged,
                proxy_type="VLESS",
            )
        ]

        classified = monitor.classify_live_targets(
            targets,
            args,
            trusted_names=set(),
        )

        self.assertEqual("unknown", classified[0].source_type)
        self.assertEqual(
            "unknown",
            monitor.route_source_type(
                ["rule", "OpenAI", forged],
                args,
                trusted_names=set(),
            ),
        )

    def test_unknown_route_recovers_to_trusted_when_allowlist_is_enabled(
        self,
    ) -> None:
        self.assertTrue(
            monitor.should_recover_trusted_route(
                "unknown",
                SimpleNamespace(trusted_profile_uid=["paid-a"]),
            )
        )
        self.assertFalse(
            monitor.should_recover_trusted_route(
                "unknown",
                SimpleNamespace(trusted_profile_uid=None),
            )
        )
        self.assertTrue(
            monitor.should_recover_trusted_route(
                "free",
                SimpleNamespace(trusted_profile_uid=None),
            )
        )

    def test_candidate_probe_failure_restores_original_selector(self) -> None:
        proxies = self.route_proxies("Candidate")
        candidate_live = self.proxies_with_now(proxies, "Candidate")
        results = [delay_result("Candidate", 400, subscription="paid")]
        switch_calls: list[tuple[object, str, str]] = []
        verify_calls: list[dict[str, str]] = []

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                side_effect=[proxies, proxies, candidate_live],
            ),
            mock.patch.object(
                monitor,
                "run_delay_checks",
                return_value=results,
            ),
            mock.patch.object(
                monitor,
                "switch_group",
                side_effect=lambda controller, group, node: switch_calls.append(
                    (controller, group, node)
                ),
            ),
            mock.patch.object(
                monitor,
                "verify_selector_selections",
                side_effect=lambda _controller, expected: verify_calls.append(
                    dict(expected)
                ),
            ),
            mock.patch.object(
                monitor,
                "guarded_comprehensive_route_check",
                return_value=(False, "offline probe failure"),
            ),
        ):
            outcome = monitor.auto_switch_if_needed(
                self.controller,
                proxies,
                mixed_port=7897,
                route_ok=False,
                args=args_for_switch(),
                route_chain=["rule", "OpenAI", "Old"],
            )

        self.assertEqual(
            [
                (self.controller, "OpenAI", "Candidate"),
                (self.controller, "OpenAI", "Old"),
            ],
            switch_calls,
        )
        self.assertEqual(
            [{"OpenAI": "Candidate"}, {"OpenAI": "Old"}],
            verify_calls,
        )
        self.assertFalse(outcome.switched)
        self.assertFalse(outcome.rollback_failed)
        self.assertIs(outcome.route_ok, False)

    def test_rollback_failure_stops_before_later_candidates(self) -> None:
        proxies = self.route_proxies("CandidateOne", "CandidateTwo")
        candidate_live = self.proxies_with_now(proxies, "CandidateOne")
        results = [
            delay_result("CandidateOne", 300, subscription="paid"),
            delay_result("CandidateTwo", 400, subscription="paid"),
        ]
        attempted_nodes: list[str] = []

        def switch_or_fail_rollback(
            _controller: object,
            _group: str,
            node: str,
        ) -> None:
            attempted_nodes.append(node)
            if node == "Old":
                raise monitor.ApiError("offline rollback failure")

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                side_effect=[proxies, proxies, candidate_live],
            ),
            mock.patch.object(
                monitor,
                "run_delay_checks",
                return_value=results,
            ),
            mock.patch.object(
                monitor,
                "switch_group",
                side_effect=switch_or_fail_rollback,
            ),
            mock.patch.object(
                monitor,
                "verify_selector_selections",
                return_value=None,
            ),
            mock.patch.object(
                monitor,
                "guarded_comprehensive_route_check",
                return_value=(False, "offline probe failure"),
            ),
        ):
            outcome = monitor.auto_switch_if_needed(
                self.controller,
                proxies,
                mixed_port=7897,
                route_ok=False,
                args=args_for_switch(),
                route_chain=["rule", "OpenAI", "Old"],
            )

        self.assertEqual(["CandidateOne", "Old"], attempted_nodes)
        self.assertNotIn("CandidateTwo", attempted_nodes)
        self.assertTrue(outcome.rollback_failed)
        self.assertFalse(outcome.switched)
        self.assertIs(outcome.route_ok, False)

    def test_dry_run_performs_no_main_controller_write(self) -> None:
        proxies = self.route_proxies("Candidate")
        results = [delay_result("Candidate", 350, subscription="paid")]
        write_attempt = mock.Mock(
            side_effect=AssertionError("dry-run attempted a main controller write")
        )
        verify_attempt = mock.Mock(
            side_effect=AssertionError("dry-run attempted selector verification")
        )
        route_probe = mock.Mock(
            side_effect=AssertionError("dry-run attempted a post-switch route probe")
        )

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                return_value=proxies,
            ),
            mock.patch.object(
                monitor,
                "run_delay_checks",
                return_value=results,
            ),
            mock.patch.object(monitor, "switch_group", new=write_attempt),
            mock.patch.object(
                monitor,
                "verify_selector_selections",
                new=verify_attempt,
            ),
            mock.patch.object(
                monitor,
                "guarded_comprehensive_route_check",
                new=route_probe,
            ),
        ):
            outcome = monitor.auto_switch_if_needed(
                self.controller,
                proxies,
                mixed_port=7897,
                route_ok=False,
                args=args_for_switch(dry_run_switch=True),
                route_chain=["rule", "OpenAI", "Old"],
            )

        write_attempt.assert_not_called()
        verify_attempt.assert_not_called()
        route_probe.assert_not_called()
        self.assertFalse(outcome.switched)
        self.assertFalse(outcome.rollback_failed)

    def test_external_manual_change_is_not_overwritten_during_rollback(self) -> None:
        proxies = self.route_proxies("Candidate")
        manually_changed_live = self.proxies_with_now(proxies, "Manual")
        results = [delay_result("Candidate", 375, subscription="paid")]
        switched_nodes: list[str] = []
        verify_calls: list[dict[str, str]] = []

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                side_effect=[proxies, proxies, manually_changed_live],
            ),
            mock.patch.object(
                monitor,
                "run_delay_checks",
                return_value=results,
            ),
            mock.patch.object(
                monitor,
                "switch_group",
                side_effect=lambda _controller, _group, node: switched_nodes.append(
                    node
                ),
            ),
            mock.patch.object(
                monitor,
                "verify_selector_selections",
                side_effect=lambda _controller, expected: verify_calls.append(
                    dict(expected)
                ),
            ),
            mock.patch.object(
                monitor,
                "guarded_comprehensive_route_check",
                return_value=(False, "offline probe failure"),
            ),
        ):
            outcome = monitor.auto_switch_if_needed(
                self.controller,
                proxies,
                mixed_port=7897,
                route_ok=False,
                args=args_for_switch(),
                route_chain=["rule", "OpenAI", "Old"],
            )

        self.assertEqual(["Candidate"], switched_nodes)
        self.assertNotIn("Old", switched_nodes)
        self.assertEqual(
            [{"OpenAI": "Candidate"}, {}],
            verify_calls,
        )
        self.assertFalse(outcome.rollback_failed)
        self.assertFalse(outcome.switched)
        self.assertIs(outcome.route_ok, False)

    def test_manual_change_during_candidate_test_aborts_before_commit(self) -> None:
        proxies = self.route_proxies("Candidate")
        manually_changed_live = self.proxies_with_now(proxies, "Manual")
        results = [delay_result("Candidate", 360, subscription="paid")]
        write_attempt = mock.Mock(
            side_effect=AssertionError("manual selection was overwritten")
        )
        route_probe = mock.Mock(
            side_effect=AssertionError("post-switch probe ran without a switch")
        )

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                return_value=manually_changed_live,
            ),
            mock.patch.object(
                monitor,
                "run_delay_checks",
                return_value=results,
            ),
            mock.patch.object(monitor, "switch_group", new=write_attempt),
            mock.patch.object(
                monitor,
                "guarded_comprehensive_route_check",
                new=route_probe,
            ),
        ):
            outcome = monitor.auto_switch_if_needed(
                self.controller,
                proxies,
                mixed_port=7897,
                route_ok=False,
                args=args_for_switch(),
                route_chain=["rule", "OpenAI", "Old"],
            )

        write_attempt.assert_not_called()
        route_probe.assert_not_called()
        self.assertTrue(outcome.rollback_failed)
        self.assertFalse(outcome.switched)

    def test_manual_change_between_group_commits_rolls_back_only_own_write(
        self,
    ) -> None:
        initial = {
            "First": {
                "type": "Selector",
                "now": "OldFirst",
                "all": ["OldFirst", "Candidate"],
            },
            "Second": {
                "type": "Selector",
                "now": "OldSecond",
                "all": ["OldSecond", "Candidate", "Manual"],
            },
            "OldFirst": {"type": "VLESS"},
            "OldSecond": {"type": "VLESS"},
            "Candidate": {"type": "VLESS"},
            "Manual": {"type": "VLESS"},
        }
        after_external_change = deepcopy(initial)
        after_external_change["First"]["now"] = "Candidate"
        after_external_change["Second"]["now"] = "Manual"
        results = [delay_result("Candidate", 365, subscription="paid")]
        switch_calls: list[tuple[str, str]] = []
        route_probe = mock.Mock(
            side_effect=AssertionError("route probe ran after an aborted commit")
        )

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                side_effect=[
                    initial,
                    initial,
                    after_external_change,
                    after_external_change,
                    after_external_change,
                ],
            ),
            mock.patch.object(
                monitor,
                "run_delay_checks",
                return_value=results,
            ),
            mock.patch.object(
                monitor,
                "switch_group",
                side_effect=lambda _controller, group, node: switch_calls.append(
                    (group, node)
                ),
            ),
            mock.patch.object(
                monitor,
                "verify_selector_selections",
                return_value=None,
            ),
            mock.patch.object(
                monitor,
                "guarded_comprehensive_route_check",
                new=route_probe,
            ),
        ):
            outcome = monitor.auto_switch_if_needed(
                self.controller,
                initial,
                mixed_port=7897,
                route_ok=False,
                args=args_for_switch(),
                route_chain=[
                    "rule",
                    "First",
                    "Second",
                    "OldSecond",
                ],
            )

        self.assertEqual(
            [("First", "Candidate"), ("First", "OldFirst")],
            switch_calls,
        )
        route_probe.assert_not_called()
        self.assertTrue(outcome.rollback_failed)
        self.assertFalse(outcome.switched)

    def test_selector_rollback_refreshes_each_group_before_writing(self) -> None:
        first_snapshot = {
            "First": {
                "type": "Selector",
                "now": "Candidate",
                "all": ["OldFirst", "Candidate"],
            },
            "Second": {
                "type": "Selector",
                "now": "Candidate",
                "all": ["OldSecond", "Candidate", "Manual"],
            },
        }
        second_snapshot = deepcopy(first_snapshot)
        second_snapshot["First"]["now"] = "OldFirst"
        second_snapshot["Second"]["now"] = "Manual"
        switch_calls: list[tuple[str, str]] = []

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                side_effect=[first_snapshot, second_snapshot],
            ),
            mock.patch.object(
                monitor,
                "switch_group",
                side_effect=lambda _controller, group, node: switch_calls.append(
                    (group, node)
                ),
            ),
            mock.patch.object(
                monitor,
                "verify_selector_selections",
                return_value=None,
            ),
        ):
            rollback_ok, externally_changed = (
                monitor.rollback_selectors_if_unchanged(
                    self.controller,
                    {
                        "First": "OldFirst",
                        "Second": "OldSecond",
                    },
                    "Candidate",
                )
            )

        self.assertTrue(rollback_ok)
        self.assertEqual(["Second"], externally_changed)
        self.assertEqual([("First", "OldFirst")], switch_calls)

    def test_healthy_free_route_can_probe_trusted_recovery_candidates(self) -> None:
        proxies = self.route_proxies("TrustedCandidate")
        results = [
            delay_result("TrustedCandidate", 420, subscription="paid")
        ]
        observed_targets: list[str] = []

        def record_delay_targets(
            _controller: object,
            targets: list[object],
            _args: object,
        ) -> list[object]:
            observed_targets.extend(target.api_name for target in targets)
            return results

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                return_value=proxies,
            ),
            mock.patch.object(
                monitor,
                "run_delay_checks",
                side_effect=record_delay_targets,
            ),
        ):
            outcome = monitor.auto_switch_if_needed(
                self.controller,
                proxies,
                mixed_port=7897,
                route_ok=True,
                args=args_for_switch(dry_run_switch=True),
                route_chain=["rule", "OpenAI", "Old"],
                allowed_source_types={"owned"},
                trusted_names={"TrustedCandidate"},
                recovering_from_fallback=True,
            )

        self.assertEqual(["TrustedCandidate"], observed_targets)
        self.assertFalse(outcome.switched)
        self.assertFalse(outcome.rollback_failed)

    def test_allowed_source_types_excludes_free_candidates(self) -> None:
        trusted_name = "TrustedCandidate"
        free_name = "免费源 Candidate"
        proxies = self.route_proxies(trusted_name, free_name)
        observed_targets: list[str] = []

        def record_delay_targets(
            _controller: object,
            targets: list[object],
            _args: object,
        ) -> list[object]:
            observed_targets.extend(target.api_name for target in targets)
            return [delay_result(trusted_name, 500, subscription="paid")]

        with (
            mock.patch.object(
                monitor,
                "fetch_live_proxies",
                return_value=proxies,
            ),
            mock.patch.object(
                monitor,
                "run_delay_checks",
                side_effect=record_delay_targets,
            ),
        ):
            outcome = monitor.auto_switch_if_needed(
                self.controller,
                proxies,
                mixed_port=7897,
                route_ok=False,
                args=args_for_switch(dry_run_switch=True),
                route_chain=["rule", "OpenAI", "Old"],
                allowed_source_types={"owned"},
                trusted_names={trusted_name},
            )

        self.assertEqual([trusted_name], observed_targets)
        self.assertNotIn(free_name, observed_targets)
        self.assertFalse(outcome.switched)
        self.assertFalse(outcome.rollback_failed)


class RunOncePriorityTests(OfflineTestCase):
    def test_route_failure_exhausts_trusted_before_unknown_and_free(
        self,
    ) -> None:
        args = SimpleNamespace(
            url="https://chatgpt.com/cdn-cgi/trace",
            failure_threshold=1,
            no_auto_switch=False,
            current_only=False,
            always_test_nodes=False,
            include_groups=False,
            no_free_backup=False,
            trusted_profile_uid=None,
            proxy_port=None,
        )
        controller = monitor.Controller(base_url="http://127.0.0.1:9090")
        trusted_target = node_target("TrustedRemote", source_type="owned")
        unknown_target = node_target("UnknownRemote", source_type="unknown")
        free_target = node_target("FreeRemote", source_type="free")
        owned_runtime = SimpleNamespace(
            version={"version": "offline"},
            profile_count=2,
            node_count=2,
            core_path=Path("/offline/mihomo"),
            targets={
                trusted_target.api_name: trusted_target,
                unknown_target.api_name: unknown_target,
            },
        )
        free_runtime = SimpleNamespace(
            version={"version": "offline"},
            profile_count=1,
            node_count=1,
            core_path=Path("/offline/mihomo"),
            targets={free_target.api_name: free_target},
        )
        owned_results = [
            delay_result("TrustedRemote", 300, subscription="paid"),
            delay_result("UnknownRemote", 350, subscription="local"),
        ]
        free_results = [
            delay_result("FreeRemote", 400, subscription="public"),
        ]
        events: list[tuple[str, frozenset[str]]] = []

        def api_response(
            _controller: object,
            path: str,
            **_kwargs: object,
        ) -> object:
            if path == "/configs":
                return {"mode": "rule", "mixed-port": 7897}
            if path == "/proxies":
                return {
                    "proxies": {
                        "OpenAI": {
                            "type": "Selector",
                            "now": "Old",
                            "all": ["Old"],
                        },
                        "Old": {"type": "VLESS"},
                    }
                }
            raise AssertionError(f"unexpected API path: {path}")

        def record_current_stage(
            *_args: object,
            **kwargs: object,
        ) -> object:
            allowed = kwargs.get("allowed_source_types") or set()
            events.append(("current", frozenset(allowed)))
            return monitor.SwitchOutcome(route_ok=False)

        def record_cross_stage(
            *_args: object,
            **kwargs: object,
        ) -> object:
            allowed = kwargs.get("allowed_source_types")
            if allowed is None:
                allowed = {"free"}
            events.append(("cross", frozenset(allowed)))
            return monitor.SwitchOutcome(route_ok=False)

        def runtime_for(
            _args: object,
            _existing: object,
            _profiles: object,
            label: str,
        ) -> object:
            return free_runtime if "免费" in label else owned_runtime

        with (
            mock.patch.object(monitor, "api_json", side_effect=api_response),
            mock.patch.object(
                monitor,
                "detect_active_runtime_config",
                return_value="",
            ),
            mock.patch.object(
                monitor,
                "base_proxy_names_for_args",
                return_value={"TrustedCurrent"},
            ),
            mock.patch.object(monitor, "print_header", return_value=None),
            mock.patch.object(
                monitor,
                "guarded_comprehensive_route_check",
                return_value=(False, "offline failure"),
            ),
            mock.patch.object(monitor, "fetch_rules", return_value=[]),
            mock.patch.object(
                monitor,
                "current_route_chain",
                return_value=["rule", "OpenAI", "Old"],
            ),
            mock.patch.object(
                monitor,
                "route_source_type",
                return_value="owned",
            ),
            mock.patch.object(
                monitor,
                "auto_switch_if_needed",
                side_effect=record_current_stage,
            ),
            mock.patch.object(
                monitor,
                "discover_remote_profiles",
                return_value=[],
            ),
            mock.patch.object(
                monitor,
                "discover_free_profiles",
                return_value=[
                    monitor.RemoteProfile(
                        uid="free",
                        name="Free",
                        path=Path("/offline/free.yaml"),
                        source_type="free",
                    )
                ],
            ),
            mock.patch.object(
                monitor,
                "ensure_profiles_runtime",
                side_effect=runtime_for,
            ),
            mock.patch.object(
                monitor,
                "test_profiles_runtime",
                side_effect=lambda runtime, _args: (
                    free_results if runtime is free_runtime else owned_results
                ),
            ),
            mock.patch.object(monitor, "print_results", return_value=None),
            mock.patch.object(
                monitor,
                "switch_to_full_subscription_node",
                side_effect=record_cross_stage,
            ),
            mock.patch.object(monitor, "update_free_stats", return_value=None),
        ):
            outcome = monitor.run_once(
                controller,
                version={"version": "offline"},
                args=args,
                hints=[],
            )

        self.assertEqual(
            [
                ("current", frozenset({"owned"})),
                ("cross", frozenset({"owned"})),
                ("current", frozenset({"unknown"})),
                ("cross", frozenset({"unknown"})),
                ("current", frozenset({"free"})),
                ("cross", frozenset({"free"})),
            ],
            events,
        )
        self.assertTrue(outcome.no_available_chatgpt_node)


class MinimalSecurityTests(OfflineTestCase):
    @staticmethod
    def fake_yaml_module_with_alias_events() -> ModuleType:
        yaml_module = ModuleType("yaml")

        class FakeYamlError(Exception):
            pass

        class FakeAliasEvent:
            pass

        class FakeSafeLoader:
            def __init__(self, text: str) -> None:
                self.text = text

            def check_event(self, event_type: object) -> bool:
                return event_type is FakeAliasEvent and "*defaults" in self.text

            def compose_node(self, _parent: object, _index: object) -> object:
                return {"unexpected": "alias accepted"}

        def fake_load(text: str, Loader: type[FakeSafeLoader]) -> object:
            return Loader(text).compose_node(None, None)

        yaml_module.YAMLError = FakeYamlError
        yaml_module.AliasEvent = FakeAliasEvent
        yaml_module.SafeLoader = FakeSafeLoader
        yaml_module.load = fake_load
        return yaml_module

    def test_load_yaml_data_rejects_yaml_alias(self) -> None:
        alias_yaml = (
            "defaults: &defaults\n"
            "  enabled: true\n"
            "copy: *defaults\n"
        )
        fake_yaml = self.fake_yaml_module_with_alias_events()

        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = Path(temp_dir) / "alias.yaml"
            fixture.write_text(alias_yaml, encoding="utf-8")
            with (
                mock.patch.dict(sys.modules, {"yaml": fake_yaml}),
                self.assertRaises(monitor.ProfileError) as raised,
            ):
                monitor.load_yaml_data(fixture)

        self.assertIn("aliases are disabled", str(raised.exception).lower())

    def test_strict_openai_destination_accepts_only_official_443_endpoints(
        self,
    ) -> None:
        accepted = (
            ("https://api.openai.com/v1/responses", "https"),
            ("https://api.openai.com:443/v1/responses", "https"),
            ("wss://api.openai.com/v1/responses", "wss"),
            ("wss://api.openai.com:443/v1/responses", "wss"),
        )
        rejected = (
            ("http://api.openai.com/v1/responses", "https"),
            ("ws://api.openai.com/v1/responses", "wss"),
            ("https://api.openai.com:8443/v1/responses", "https"),
            ("wss://api.openai.com:8443/v1/responses", "wss"),
            ("https://example.com/v1/responses", "https"),
            ("https://api.openai.com.evil.example/v1/responses", "https"),
            ("https://api.openai.com@evil.example/v1/responses", "https"),
            ("https://user@api.openai.com/v1/responses", "https"),
            ("https://user:password@api.openai.com/v1/responses", "https"),
            ("https://api.openai.com:not-a-port/v1/responses", "https"),
            ("https://api.openai.com/v1/responses", "wss"),
            ("wss://api.openai.com/v1/responses", "https"),
        )

        for url, scheme in accepted:
            with self.subTest(url=url, scheme=scheme):
                self.assertTrue(monitor.strict_openai_destination_ok(url, scheme))
        for url, scheme in rejected:
            with self.subTest(url=url, scheme=scheme):
                self.assertFalse(monitor.strict_openai_destination_ok(url, scheme))

    def test_secure_write_text_overwrites_target_with_mode_0600(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "runtime-config.json"
            target.write_text("old content", encoding="utf-8")
            target.chmod(0o644)

            monitor.secure_write_text(target, "first replacement")
            self.assertEqual("first replacement", target.read_text(encoding="utf-8"))
            self.assertEqual(0o600, stat.S_IMODE(target.stat().st_mode))

            monitor.secure_write_text(target, "final content")
            self.assertEqual("final content", target.read_text(encoding="utf-8"))
            self.assertEqual(0o600, stat.S_IMODE(target.stat().st_mode))
            self.assertEqual([target], list(target.parent.iterdir()))


if __name__ == "__main__":
    unittest.main()
