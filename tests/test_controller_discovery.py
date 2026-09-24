from __future__ import annotations

import os
import stat
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import monitor_clash_verge as monitor


class ControllerDiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.args = SimpleNamespace(api=None, unix_socket=None, secret=None)
        self.uid = 7123
        self.socket_metadata: dict[str, SimpleNamespace] = {}
        self.hints = [
            monitor.ConfigHint(
                path=Path("/offline/clash-verge.yaml"),
                unix_socket="/offline/old-verge-mihomo.sock",
                secret="test-controller-secret",
            )
        ]
        self.version = {"meta": True, "version": "test-version"}
        for patcher in (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch.object(monitor.os, "getuid", side_effect=lambda: self.uid),
            mock.patch.object(Path, "lstat", autospec=True, side_effect=self.lstat),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def lstat(self, path: Path) -> SimpleNamespace:
        if str(path) not in self.socket_metadata:
            raise FileNotFoundError(str(path))
        return self.socket_metadata[str(path)]

    def service_socket(self) -> str:
        return f"/var/run/clash-verge-service/users/{self.uid}/verge-mihomo.sock"

    def add_socket(self, path: str) -> None:
        self.socket_metadata[path] = SimpleNamespace(
            st_mode=stat.S_IFSOCK | 0o600, st_uid=self.uid,
        )

    def test_discovers_service_socket_when_config_still_points_to_missing_socket(self) -> None:
        for uid in (7123, 8456):
            with self.subTest(uid=uid):
                self.uid = uid
                self.socket_metadata.clear()
                service_socket = self.service_socket()
                self.add_socket(service_socket)

                def api_response(controller, path, **kwargs):
                    self.assertEqual(path, "/version")
                    if controller.unix_socket != service_socket:
                        raise monitor.ApiError("Connection refused")
                    if controller.secret != "test-controller-secret":
                        raise monitor.ApiError("Unauthorized", status=401)
                    return self.version

                with mock.patch.object(monitor, "api_json", side_effect=api_response):
                    controller, version, _errors = monitor.find_controller(self.args, self.hints)

                self.assertEqual(controller.unix_socket, service_socket)
                self.assertEqual(version, self.version)

    def test_unsafe_service_socket_is_not_contacted(self) -> None:
        service_socket = self.service_socket()
        for metadata in (
            None,
            SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=self.uid),
            SimpleNamespace(st_mode=stat.S_IFLNK | 0o777, st_uid=self.uid),
            SimpleNamespace(st_mode=stat.S_IFSOCK | 0o600, st_uid=self.uid + 1),
        ):
            with self.subTest(metadata=metadata):
                self.socket_metadata.clear()
                if metadata is not None:
                    self.socket_metadata[service_socket] = metadata
                with mock.patch.object(monitor, "api_json", return_value=self.version) as api:
                    controller, _version, _errors = monitor.find_controller(self.args, self.hints)
                self.assertIsNone(controller.unix_socket)
                self.assertTrue(all(
                    call.args[0].unix_socket != service_socket for call in api.call_args_list
                ))

    def test_explicit_controller_keeps_priority_over_service_socket(self) -> None:
        self.add_socket(self.service_socket())
        self.args.api = "http://127.0.0.1:19123"
        with mock.patch.object(monitor, "api_json", return_value=self.version):
            controller, _version, _errors = monitor.find_controller(self.args, self.hints)
        self.assertEqual(controller.base_url, self.args.api)

    def test_legacy_socket_still_works_without_service_socket(self) -> None:
        legacy_socket = "/tmp/verge/verge-mihomo.sock"
        self.add_socket(legacy_socket)
        with mock.patch.object(monitor, "api_json", return_value=self.version):
            controller, _version, _errors = monitor.find_controller(self.args, self.hints)
        self.assertEqual(controller.unix_socket, legacy_socket)


if __name__ == "__main__":
    unittest.main()
