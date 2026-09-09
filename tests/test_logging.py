from __future__ import annotations

import io
import stat
import sys
import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
