from __future__ import annotations

import io
import stat
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import monitor_clash_verge as monitor


class LogFileTests(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.log_dir = self.root / "logs"
        patcher = mock.patch.object(monitor, "DEFAULT_LOG_DIR", self.log_dir)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(monitor.close_log_file)

    def args(self, log_file=None, no_log_file=False):
        return SimpleNamespace(log_file=log_file, no_log_file=no_log_file)

    def test_default_appends_sessions_and_preserves_console_format(self):
        path = monitor.setup_log_file(self.args())
        with mock.patch.object(monitor.sys, "stdout", new_callable=io.StringIO) as console:
            monitor.log("\033[32m中文\033[0m", "ok", sep="|", end="")
            monitor.log("!")
            self.assertEqual(console.getvalue(), "\033[32m中文\033[0m|ok!\n")
        with mock.patch.object(monitor.sys, "stderr", new_callable=io.StringIO):
            monitor.log("error", file=monitor.sys.stderr)
        monitor.close_log_file()
        monitor.setup_log_file(self.args())
        monitor.close_log_file()
        self.assertEqual(path, self.log_dir / "clash-monitor.log")
        content = path.read_text(encoding="utf-8")
        self.assertIn("中文|ok!\nerror\n", content)
        self.assertNotIn("\033", content)
        self.assertEqual(content.count("会话开始："), 2)
        self.assertEqual(content.count("会话结束："), 2)
        self.assertFalse(list(self.root.glob("*.log")))

    def test_relative_filename_goes_under_logs_and_absolute_override_is_respected(self):
        path = monitor.setup_log_file(self.args("checks/validation.log"))
        self.assertEqual(path, self.log_dir / "checks/validation.log")
        self.assertTrue(path.is_file())
        custom = self.root / "external" / "custom.log"
        self.assertEqual(monitor.setup_log_file(self.args(str(custom))), custom)
        self.assertTrue(custom.is_file())

    def test_rotation_bounds_backups_and_keeps_files_private(self):
        self.log_dir.mkdir()
        archive = self.log_dir / "archive"
        archive.mkdir()
        historical = archive / "old.log"
        historical.write_text("historical", encoding="utf-8")
        path = self.log_dir / "clash-monitor.log"
        path.write_text("previous run\n", encoding="utf-8")
        path.chmod(0o644)
        with mock.patch.object(monitor, "MAX_LOG_BYTES", 256), mock.patch.object(
            monitor, "LOG_BACKUP_COUNT", 2,
        ), mock.patch.object(monitor.sys, "stdout", new_callable=io.StringIO):
            monitor.setup_log_file(self.args())
            for index in range(10):
                monitor.log(f"record-{index}: " + "x" * 160)
            monitor.close_log_file()
        files = sorted(self.log_dir.glob("clash-monitor.log*"))
        self.assertEqual([p.name for p in files], [
            "clash-monitor.log", "clash-monitor.log.1", "clash-monitor.log.2",
        ])
        content = "".join(p.read_text(encoding="utf-8") for p in files)
        self.assertIn("record-9:", content)
        self.assertNotIn("record-0:", content)
        for log_path in files:
            self.assertEqual(stat.S_IMODE(log_path.stat().st_mode), 0o600)
        self.assertEqual(historical.read_text(encoding="utf-8"), "historical")

    def test_disabled_logging_does_not_create_files_and_closes_previous_session(self):
        self.assertIsNone(monitor.setup_log_file(self.args("ignored.log", True)))
        self.assertFalse(self.log_dir.exists())
        path = monitor.setup_log_file(self.args())
        monitor.setup_log_file(self.args(no_log_file=True))
        self.assertIsNone(monitor.LOG_FILE)
        before = path.read_bytes()
        with mock.patch.object(monitor.sys, "stdout", new_callable=io.StringIO):
            monitor.log("console only")
        self.assertEqual(path.read_bytes(), before)


class CompactRoundTests(unittest.TestCase):
    def setUp(self) -> None:
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.stack = stack
        with mock.patch.object(sys, "argv", ["monitor"]):
            self.args = monitor.parse_args()
        self.controller = monitor.Controller()
        self.system_proxy = stack.enter_context(mock.patch.object(monitor, "system_proxy_status", return_value=(True, "系统代理：接入配置正常")))
        responses = {"/configs": {"mode": "rule", "mixed-port": 7897}, "/proxies": {"proxies": {}}}
        stack.enter_context(mock.patch.object(monitor, "api_json", side_effect=lambda _c, path, **_kw: responses[path]))
        stack.enter_context(mock.patch.object(monitor, "detect_active_runtime_config", return_value=""))
        stack.enter_context(mock.patch.object(monitor, "base_proxy_names_for_args", return_value=set()))
        stack.enter_context(mock.patch.object(monitor, "fetch_rules", return_value=[]))
        stack.enter_context(mock.patch.object(monitor, "route_source_type", return_value="owned"))
        self.chain = stack.enter_context(mock.patch.object(monitor, "current_route_chain", return_value=["rule", "OpenAI", "node"]))
        self.real_probe = monitor.guarded_comprehensive_route_check
        self.probe = stack.enter_context(mock.patch.object(monitor, "guarded_comprehensive_route_check", return_value=(True, "protocol details")))
        self.switch = stack.enter_context(mock.patch.object(monitor, "auto_switch_if_needed", side_effect=AssertionError("unexpected switch")))
        stack.enter_context(mock.patch.object(monitor, "run_delay_checks", side_effect=AssertionError("unexpected node test")))

    def run_round(self, previous=None):
        with mock.patch.object(sys, "stdout", new_callable=io.StringIO) as console:
            outcome = monitor.run_once(self.controller, {}, self.args, [], previous_log_state=previous)
        return outcome, console.getvalue()

    def test_system_proxy_warning_is_visible_without_switching_nodes(self):
        healthy, _ = self.run_round()
        self.system_proxy.return_value = (False, "系统代理：警告：未完整接入当前 Clash")
        warning, details = self.run_round(healthy.log_state)
        self.assertIn("系统代理：警告", details)
        self.assertTrue(warning.route_ok)
        self.assertTrue(warning.had_errors)
        self.assertEqual(0, warning.route_failure_streak)
        repeated, details = self.run_round(warning.log_state)
        self.assertGreater(len(details.strip().splitlines()), 1)
        self.assertIn("系统代理：警告", details)
        self.assertIn("protocol details", details)
        self.assertIn("rule => OpenAI => node", details)
        self.assertTrue(repeated.had_errors)
        self.switch.assert_not_called()

    def test_healthy_rounds_write_one_line_to_console_and_file(self):
        temp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(mock.patch.object(monitor, "DEFAULT_LOG_DIR", Path(temp)))
        path = monitor.setup_log_file(self.args)
        self.stack.callback(monitor.close_log_file)
        before = path.read_text(encoding="utf-8")
        first, output1 = self.run_round()
        _, output2 = self.run_round(first.log_state)
        self.assertFalse(first.had_errors)
        for output in (output1, output2):
            self.assertEqual(len(output.splitlines()), 1)
            self.assertRegex(output, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| Codex 免登录连通性验证：通过\n$")
            self.assertNotIn("当前路径", output)
            self.assertNotIn("系统代理", output)
            self.assertNotIn("protocol details", output)
        self.assertEqual(path.read_text(encoding="utf-8"), before + output1 + output2)
        self.switch.assert_not_called()

    def test_healthy_route_changes_stay_compact(self):
        first, _ = self.run_round()
        self.chain.return_value = ["rule", "OpenAI", "new node"]
        changed, output = self.run_round(first.log_state)
        self.assertFalse(changed.had_errors)
        self.assertEqual(len(output.splitlines()), 1)
        self.assertIn("Codex 免登录连通性验证：通过", output)
        self.assertNotIn("protocol details", output)
        self.assertNotIn("new node", output)
        _, stable = self.run_round(changed.log_state)
        self.assertEqual(len(stable.splitlines()), 1)

    def test_failures_and_blocks_keep_details_then_clean_recovery_is_compact(self):
        for status, reason in ((False, "network failure"), (None, "401 blocked")):
            with self.subTest(status=status):
                self.probe.return_value = (status, reason)
                failed, details = self.run_round()
                self.assertTrue(failed.had_errors)
                self.assertGreater(len(details.splitlines()), 1)
                self.assertIn(reason, details)
                self.probe.return_value = (True, "recovered details")
                recovered, output = self.run_round(failed.log_state)
                self.assertFalse(recovered.had_errors)
                self.assertEqual(len(output.splitlines()), 1)
                self.assertIn("Codex 免登录连通性验证：通过", output)
                self.assertNotIn("recovered details", output)
                _, stable = self.run_round(recovered.log_state)
                self.assertEqual(len(stable.splitlines()), 1)

    def test_retry_success_keeps_failure_context_then_next_clean_round_is_compact(self):
        self.probe.side_effect = self.real_probe
        self.args.route_retries = 2
        self.args.route_retry_delay = 0
        cfg = SimpleNamespace(sse_url="https://chatgpt.com/backend-api/codex/responses", mode="network", credentials=SimpleNamespace(mode="chatgpt"))
        with mock.patch.object(monitor.codex_probe, "settings", return_value=cfg), mock.patch.object(
            monitor, "check_via_mixed_proxy", side_effect=[(False, "first connection failed"), (True, "200"), (True, "200")],
        ), mock.patch.object(monitor.codex_probe, "run_step", return_value=(True, "pass")):
            retried, details = self.run_round()
            self.assertTrue(retried.had_errors)
            self.assertIn("第 2/2 次成功", details)
            self.assertIn("first connection failed", details)
            clean, stable = self.run_round(retried.log_state)
            self.assertFalse(clean.had_errors)
            self.assertEqual(len(stable.splitlines()), 1)

    def test_switch_actions_are_not_hidden(self):
        self.args.failure_threshold = 1
        self.probe.return_value = (False, "connection failed")

        def switch(*_args, **_kwargs):
            monitor.log("自动切换：已切换 => replacement")
            return monitor.SwitchOutcome(route_ok=True, switched=True, source_type="owned")

        self.switch.side_effect = switch
        outcome, details = self.run_round()
        self.assertTrue(outcome.route_ok)
        self.assertTrue(outcome.had_errors)
        self.assertIn("connection failed", details)
        self.assertIn("自动切换：已切换 => replacement", details)

    def test_wait_does_not_print_or_log_countdown_and_can_stop_early(self):
        for tty in (False, True):
            with self.subTest(tty=tty), mock.patch.object(sys, "stdout", new_callable=io.StringIO) as console, mock.patch.object(
                monitor.time, "monotonic", side_effect=[0, 0, 0.2, 0.4],
            ), mock.patch.object(monitor.time, "sleep") as sleep, mock.patch.object(monitor, "log") as log:
                console.isatty = lambda: tty
                monitor.wait_for_next_check(0.4, lambda: False)
                self.assertEqual(sleep.call_count, 2)
                self.assertEqual(console.getvalue(), "")
                log.assert_not_called()
        with mock.patch.object(monitor.time, "sleep") as sleep:
            monitor.wait_for_next_check(10, lambda: True)
            sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
