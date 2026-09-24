import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import monitor_clash_verge as monitor


class SystemProxyTests(unittest.TestCase):
    def check(self, body, configs=None):
        with mock.patch.object(monitor.sys, "platform", "darwin"), mock.patch.object(
            monitor.subprocess, "run", return_value=mock.Mock(stdout="<dictionary> {\n" + body + "\n}")
        ):
            return monitor.system_proxy_status(7897, configs or {})

    def test_disabled_proxy_warns(self):
        ok, message = self.check("")
        self.assertFalse(ok)
        self.assertIn("警告", message)

    def test_http_https_required_socks_optional(self):
        body = "HTTPEnable : 1\nHTTPProxy : 127.0.0.1\nHTTPPort : 7897"
        self.assertFalse(self.check(body)[0])
        body += "\nHTTPSEnable : 1\nHTTPSProxy : localhost\nHTTPSPort : 7897"
        self.assertTrue(self.check(body)[0])
        self.assertFalse(self.check(body.replace("HTTPSPort : 7897", "HTTPSPort : 7890"))[0])
        self.assertFalse(self.check(body + "\nSOCKSEnable : 1\nSOCKSProxy : other\nSOCKSPort : 7897")[0])

    def test_scoped_settings_do_not_override_global(self):
        self.assertFalse(self.check("__SCOPED__ : <dictionary> {\nen0 : <dictionary> {\nHTTPEnable : 1\nHTTPSEnable : 1\nHTTPProxy : localhost\nHTTPSProxy : localhost\nHTTPPort : 7897\nHTTPSPort : 7897\n}\n}")[0])

    def test_tun_reports_unverified_takeover(self):
        ok, message = self.check("", {"tun": {"enable": True}})
        self.assertTrue(ok)
        self.assertIn("未验证实际接管", message)
        self.assertFalse(self.check("", {"tun": {"enable": False}})[0])

    def test_pac_is_unverified(self):
        self.assertFalse(self.check("ProxyAutoConfigEnable : 1")[0])

    def test_timeout_is_unknown(self):
        with mock.patch.object(monitor.sys, "platform", "darwin"), mock.patch.object(
            monitor.subprocess, "run", side_effect=subprocess.TimeoutExpired("scutil", 3)
        ):
            ok, message = monitor.system_proxy_status(7897, {})
        self.assertFalse(ok)
        self.assertIn("状态未知", message)

    def test_other_platforms_do_not_run_scutil(self):
        with mock.patch.object(monitor.sys, "platform", "linux"), mock.patch.object(monitor.subprocess, "run") as run:
            self.assertIn("未检查", monitor.system_proxy_status(7897, {})[1])
            run.assert_not_called()
