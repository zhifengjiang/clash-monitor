from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import codex_probe as p
import monitor_clash_verge as m


def cfg() -> p.Settings:
    return p.Settings(p.Credentials("chatgpt", "test-secret", "test-account"), "test-model",
                      "https://chatgpt.com/backend-api/codex/models",
                      "https://chatgpt.com/backend-api/codex/responses",
                      "wss://chatgpt.com/backend-api/codex/responses", 5, "generation")


def events(text: str = "OK", output: bool = True) -> list[dict]:
    return [
        {"type": "response.created", "response": {"id": "r1"}},
        {"type": "response.output_text.delta", "delta": text},
        {"type": "response.completed", "response": {"id": "r1", "status": "completed", "output": [
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]}
        ] if output else []}},
    ]


def sse(items: list[dict]) -> bytes:
    return b"".join(("event: " + e["type"] + "\ndata: " + json.dumps(e) + "\n\n").encode() for e in items)


def frame(opcode: int, payload: bytes, fin: bool = True) -> bytes:
    first = bytes([(0x80 if fin else 0) | opcode])
    n = len(payload)
    if n < 126:
        return first + bytes([n]) + payload
    if n <= 65535:
        return first + b"\x7e" + struct.pack("!H", n) + payload
    return first + b"\x7f" + struct.pack("!Q", n) + payload


class FakeSocket:
    def __init__(self, data: bytes = b"") -> None:
        self.data = bytearray(data)
        self.sent: list[bytes] = []
        self.closed = False

    def recv(self, n: int) -> bytes:
        result = bytes(self.data[:n])
        del self.data[:n]
        return result

    def settimeout(self, value: float) -> None:
        pass

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)

    def close(self) -> None:
        self.closed = True


class Offline(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for patch in (
            mock.patch.dict(os.environ, {"CODEX_HOME": self.tmp.name, "OPENAI_API_KEY": ""}, clear=True),
            mock.patch.object(p.socket, "create_connection", side_effect=AssertionError("unexpected network")),
            mock.patch.object(p.http.client, "HTTPSConnection", side_effect=AssertionError("unexpected network")),
        ):
            patch.start()
            self.addCleanup(patch.stop)


class CredentialsTests(Offline):
    def args(self, **kw) -> SimpleNamespace:
        return SimpleNamespace(**{"codex_auth_mode": "auto", "probe_mode": "generation", "openai_stream_model": "test-model", **kw})

    def write_auth(self, **kw) -> None:
        data = {"auth_mode": "chatgpt", "tokens": {"access_token": "test-token", "account_id": "account"}, **kw}
        (Path(self.tmp.name) / "auth.json").write_text(json.dumps(data))

    def test_auto_uses_codex_login_before_unrelated_api_key(self):
        self.write_auth()
        with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "unrelated-key"}):
            result = p.load_credentials(self.args())
        self.assertEqual("chatgpt", result.mode)
        self.assertEqual("test-token", result.token)
        self.assertNotIn("test-token", repr(result))

    def test_reloads_credentials_without_modifying_file(self):
        self.write_auth()
        self.assertEqual("test-token", p.load_credentials(self.args()).token)
        self.write_auth(tokens={"access_token": "renewed", "account_id": "account"})
        path = Path(self.tmp.name) / "auth.json"
        before = path.read_bytes()
        self.assertEqual("renewed", p.load_credentials(self.args()).token)
        self.assertEqual(before, path.read_bytes())

    def test_missing_credentials_fails_before_network(self):
        with self.assertRaises(p.ProbeError) as cm:
            p.settings(self.args())
        self.assertEqual("auth", cm.exception.kind)

    def test_invalid_auth_json_is_not_logged(self):
        (Path(self.tmp.name) / "auth.json").write_text("secret-invalid-json")
        with self.assertRaises(p.ProbeError) as cm:
            p.load_credentials(self.args())
        self.assertNotIn("secret-invalid-json", str(cm.exception))

    def test_expired_token_is_blocked(self):
        claims = base64.urlsafe_b64encode(json.dumps({"exp": 1}).encode()).decode()
        self.write_auth(tokens={"access_token": "a." + claims + ".c", "account_id": "account"})
        with self.assertRaises(p.ProbeError) as cm:
            p.load_credentials(self.args())
        self.assertEqual("auth", cm.exception.kind)

    def test_api_key_mode_uses_api_backend(self):
        with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "real-test-key"}):
            result = p.settings(self.args(codex_auth_mode="api-key"))
        self.assertEqual("https://api.openai.com/v1/responses", result.sse_url)
        self.assertEqual("wss://api.openai.com/v1/responses", result.ws_url)

    def test_missing_model_fails_instead_of_guessing(self):
        self.write_auth()
        with self.assertRaises(p.ProbeError):
            p.settings(self.args(openai_stream_model=None))

    def test_generation_does_not_inherit_expensive_desktop_model(self):
        self.write_auth()
        (Path(self.tmp.name) / "config.toml").write_text('model = "actual-model"\n[profiles.other]\nmodel = "wrong"\n')
        with self.assertRaises(p.ProbeError):
            p.settings(self.args(openai_stream_model=None))

    def test_network_mode_needs_no_model_and_does_not_inherit_one(self):
        self.write_auth()
        (Path(self.tmp.name) / "config.toml").write_text('model = "expensive-model"\n')
        result = p.settings(self.args(probe_mode="network", openai_stream_model=None))
        self.assertEqual("network", result.mode)
        self.assertEqual("", result.model)

    def test_legacy_strict_flag_explicitly_selects_generation(self):
        self.write_auth()
        result = p.settings(self.args(probe_mode="network", strict_stream_probes=True))
        self.assertEqual("generation", result.mode)

    def test_every_skip_option_blocks_success(self):
        self.write_auth()
        for key in ("quick_route_only", "skip_api_probe", "skip_sse_probe", "skip_websocket_probe"):
            with self.subTest(key=key), self.assertRaises(p.ProbeError):
                p.settings(self.args(**{key: True}))

    def test_credentials_cannot_be_sent_to_wrong_host_or_scheme(self):
        for url in ("https://example.com/backend-api/codex/responses",
                    "https://api.openai.com/v1/responses", "http://chatgpt.com/backend-api/codex/responses",
                    "https://chatgpt.com:8443/backend-api/codex/responses",
                    "https://chatgpt.com@evil.example/backend-api/codex/responses",
                    "https://user@chatgpt.com/backend-api/codex/responses",
                    "https://chatgpt.com/other", "https://chatgpt.com/backend-api/codex/responses#fragment"):
            with self.subTest(url=url), self.assertRaises(p.ProbeError):
                p.validate_destination(url, cfg().credentials, "https")

    def test_header_injection_rejected(self):
        with self.assertRaises(p.ProbeError):
            p.Credentials("chatgpt", "secret\r\nX: injected").headers()

    def test_custom_backend_is_not_silently_replaced_with_official_backend(self):
        self.write_auth()
        (Path(self.tmp.name) / "config.toml").write_text('model_provider = "custom"\n[model_providers.custom]\nrequires_openai_auth = true\nbase_url = "https://example.com/v1"\n')
        with self.assertRaises(p.ProbeError) as cm:
            p.settings(self.args())
        self.assertEqual("config", cm.exception.kind)

    def test_custom_provider_using_default_official_auth_is_supported(self):
        self.write_auth()
        (Path(self.tmp.name) / "config.toml").write_text('model_provider = "custom"\n[model_providers.custom]\nrequires_openai_auth = true\nwire_api = "responses"\n')
        self.assertEqual("chatgpt", p.settings(self.args()).credentials.mode)


class StreamTests(Offline):
    def test_split_sse_events_with_crlf_are_verified(self):
        body = sse(events()).replace(b"\n", b"\r\n")
        chunks = (body[i:i+3] for i in range(0, len(body), 3))
        self.assertEqual(3, p.verify_events(p.sse_events(chunks), "OK"))

    def test_codex_completed_may_elide_already_streamed_output(self):
        self.assertEqual(3, p.verify_events(iter(events(output=False)), "OK"))

    def test_error_events_and_incomplete_generation_never_pass(self):
        for typ in ("error", "response.failed", "response.incomplete"):
            with self.subTest(typ=typ), self.assertRaises(p.ProbeError):
                p.verify_events(p.sse_events(iter([sse([{"type": typ}])])), "OK")

    def test_error_event_without_json_never_passes(self):
        with self.assertRaises(p.ProbeError):
            list(p.sse_events(iter([b"event: error\n\n"])))

    def test_sse_event_type_mismatch_is_rejected(self):
        with self.assertRaises(p.ProbeError):
            list(p.sse_events(iter([b'event: error\ndata: {"type":"response.completed"}\n\n'])))

    def test_first_event_delta_or_done_without_completion_is_failure(self):
        for stream in ([events()[0]], events()[:2]):
            with self.subTest(stream=stream), self.assertRaises(p.ProbeError):
                p.verify_events(p.sse_events(iter([sse(stream) + b"data: [DONE]\n\n"])), "OK")

    def test_truncated_completion_is_failure(self):
        with self.assertRaises(p.ProbeError):
            p.verify_events(p.sse_events(iter([sse(events()).rstrip(b"\n")])), "OK")

    def test_completed_without_deltas_or_wrong_nonce_is_failure(self):
        for stream in ([events()[-1]], events("incorrect")):
            with self.subTest(stream=stream), self.assertRaises(p.ProbeError):
                p.verify_events(iter(stream), "OK")

    def test_wrong_status_or_mismatched_response_id_is_failure(self):
        for key, value in (("status", "incomplete"), ("id", "different")):
            stream = events()
            stream[-1]["response"][key] = value
            with self.subTest(key=key), self.assertRaises(p.ProbeError):
                p.verify_events(iter(stream), "OK")

    def test_inconsistent_completed_text_is_failure(self):
        stream = events()
        stream[-1]["response"]["output"][0]["content"][0]["text"] = "wrong"
        with self.assertRaises(p.ProbeError):
            p.verify_events(iter(stream), "OK")

    def test_sse_limits_and_invalid_json(self):
        for body in (b"x" * (p.MAX_HEADER + 1), b"data: not-json\n\n", b"data: []\n\n"):
            with self.subTest(size=len(body)), self.assertRaises(p.ProbeError):
                list(p.sse_events(iter([body])))

    def test_deadline_is_total_even_with_continuous_data(self):
        with mock.patch.object(p.time, "monotonic", side_effect=[10, 11, 16]):
            deadline = p.Deadline(5)
            self.assertEqual(4, deadline.remaining())
            with self.assertRaises(p.ProbeError):
                deadline.remaining()

    def test_http_200_html_is_not_a_stream(self):
        response = mock.Mock()
        response.getheader.return_value = "text/html"
        with mock.patch.object(p, "open_http", return_value=(mock.Mock(), response, FakeSocket())):
            with self.assertRaises(p.ProbeError):
                p.probe_sse(cfg(), 7897)
        response.close.assert_called_once()


class WebSocketTests(Offline):
    def test_fragmented_text_and_interleaved_ping(self):
        message = json.dumps(events()[0]).encode()
        wire = frame(1, message[:12], False) + frame(9, b"ping") + frame(0, message[12:])
        sock = FakeSocket(wire)
        self.assertEqual(events()[0], next(p.websocket_events(sock, p.Deadline(5))))
        self.assertEqual(0x8a, sock.sent[0][0])
        self.assertTrue(sock.sent[0][1] & 0x80)

    def test_close_partial_frame_invalid_mask_and_bad_continuation_fail(self):
        for wire in (frame(8, b""), b"\x81\x05abc", b"\x81\x80", frame(0, b"{}")):
            with self.subTest(wire=wire), self.assertRaises(p.ProbeError):
                next(p.websocket_events(FakeSocket(wire), p.Deadline(5)))

    def test_excessive_frame_size_rejected_before_read(self):
        wire = b"\x81\x7f" + struct.pack("!Q", p.MAX_BYTES + 1)
        with self.assertRaises(p.ProbeError):
            next(p.websocket_events(FakeSocket(wire), p.Deadline(5)))

    def test_client_frames_are_masked_and_decode_to_original_payload(self):
        for length in (10, 130, 66000):
            with self.subTest(length=length):
                sock = FakeSocket()
                payload = b"x" * length
                p.send_frame(sock, 1, payload, p.Deadline(5))
                wire = sock.sent[0]
                offset = 2 if length < 126 else 4 if length <= 65535 else 10
                mask = wire[offset:offset+4]
                decoded = bytes(v ^ mask[i % 4] for i, v in enumerate(wire[offset+4:]))
                self.assertEqual(payload, decoded)

    def test_headers_do_not_consume_first_frame_and_allow_repeated_cookies(self):
        sock = FakeSocket(b"HTTP/1.1 101 Switching Protocols\r\nSet-Cookie: a\r\nSet-Cookie: b\r\n\r\nFRAME")
        status, _ = p.read_header(sock, p.Deadline(5))
        self.assertEqual(101, status)
        self.assertEqual(b"FRAME", bytes(sock.data))

    def test_full_handshake_and_generation_use_explicit_proxy(self):
        self.exercise_handshake(True)

    def test_101_with_invalid_accept_is_not_success(self):
        self.exercise_handshake(False)

    def test_network_handshake_and_ping_pong_send_no_generation_payload(self):
        self.exercise_handshake(True, network=True)

    def test_network_wrong_pong_or_early_close_never_pass(self):
        for wire in (frame(10, b"wrong") + frame(8, b""), frame(8, b""), b""):
            sock = FakeSocket(wire)
            with self.subTest(wire=wire), mock.patch.object(p, "open_websocket", return_value=sock), self.assertRaises(p.ProbeError):
                p.probe_websocket_network(replace(cfg(), mode="network"), 7897)
            self.assertEqual([0x89], [data[0] for data in sock.sent])
            self.assertTrue(sock.closed)

    def exercise_handshake(self, valid: bool, network: bool = False) -> None:
        class Server(FakeSocket):
            def sendall(sock, data):
                super().sendall(data)
                if data.startswith(b"CONNECT"):
                    sock.data.extend(b"HTTP/1.1 200 Connection established\r\n\r\n")
                elif data.startswith(b"GET"):
                    key = next(line.split(b": ", 1)[1] for line in data.split(b"\r\n") if line.startswith(b"Sec-WebSocket-Key:"))
                    accept = base64.b64encode(hashlib.sha1(key + p.WS_GUID.encode()).digest()) if valid else b"invalid"
                    sock.data.extend(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n")
                elif data[0] in (0x81, 0x89):
                    size = data[1] & 127
                    offset = 2 if size < 126 else 4 if size == 126 else 10
                    mask, raw = data[offset:offset+4], data[offset+4:]
                    decoded = bytes(v ^ mask[i % 4] for i, v in enumerate(raw))
                    if data[0] == 0x89:
                        sock.data.extend(frame(9, b"server-ping") + frame(10, decoded))
                        return
                    request = json.loads(decoded)
                    self.assertEqual("response.create", request["type"])
                    self.assertNotIn("stream", request)
                    expected = request["input"][0]["content"][0]["text"].split(": ", 1)[1]
                    for event in events(expected, output=False):
                        sock.data.extend(frame(1, json.dumps(event).encode()))
        sock = Server()
        context = mock.Mock()
        context.wrap_socket.return_value = sock
        with mock.patch.object(p.socket, "create_connection", return_value=sock) as connect, mock.patch.object(p, "tls_context", return_value=context), mock.patch.dict(os.environ, {"NO_PROXY": "*"}):
            if valid:
                if network:
                    self.assertIn("ping/pong", p.probe_websocket_network(replace(cfg(), mode="network"), 8123))
                    self.assertEqual([0x89, 0x8a, 0x88], [data[0] for data in sock.sent if not data.startswith((b"CONNECT", b"GET"))])
                else:
                    self.assertIn("response.completed", p.probe_websocket(cfg(), 8123))
            else:
                with self.assertRaises(p.ProbeError):
                    p.probe_websocket(cfg(), 8123)
            self.assertEqual(("127.0.0.1", 8123), connect.call_args.args[0])
            context.wrap_socket.assert_called_once_with(sock, server_hostname="chatgpt.com")
        self.assertTrue(sock.closed)


class HttpAndPolicyTests(Offline):
    def test_http_connect_cannot_bypass_proxy_via_no_proxy_or_redirect(self):
        conn = mock.Mock()
        conn.getresponse.return_value.status = 302
        with mock.patch.object(p.http.client, "HTTPSConnection", return_value=conn) as connect, mock.patch.dict(os.environ, {"NO_PROXY": "*"}):
            with self.assertRaises(p.ProbeError):
                p.open_http(cfg().sse_url, 8123, cfg(), p.Deadline(5), {})
        self.assertEqual(("127.0.0.1", 8123), connect.call_args.args)
        conn.set_tunnel.assert_called_once_with("chatgpt.com", 443)
        conn.close.assert_called_once()

    def test_models_http200_requires_a_real_model_list(self):
        for body, success in ((b'{"models":[{"slug":"test-model"}]}', True), (b'{}', False), (b'<html>challenge</html>', False)):
            response = mock.Mock()
            response.getheader.return_value = "application/json"
            with self.subTest(body=body), mock.patch.object(p, "open_http", return_value=(mock.Mock(), response, FakeSocket())), mock.patch.object(p, "http_chunks", return_value=iter([body])):
                if success:
                    self.assertIn("真实鉴权成功", p.probe_models(cfg(), 1))
                else:
                    with self.assertRaises(p.ProbeError):
                        p.probe_models(cfg(), 1)

    def test_auth_quota_config_and_service_failures_are_blocked_not_success(self):
        for status in (400, 401, 404, 429, 500):
            with self.subTest(status=status):
                probe = mock.Mock(side_effect=p.http_error(status))
                self.assertIsNone(p.run_step("SSE", probe, cfg(), 1)[0])

    def test_network_failure_returns_false_without_leaking_exception_details(self):
        result, message = p.run_step("WS", mock.Mock(side_effect=OSError("private-secret")), cfg(), 1)
        self.assertIs(result, False)
        self.assertNotIn("private-secret", message)

    def args(self):
        with mock.patch.object(sys, "argv", ["monitor", "--current-only"]):
            return m.parse_args()

    def test_comprehensive_check_requires_all_three_real_probes(self):
        with mock.patch.object(p, "settings", return_value=cfg()), mock.patch.object(m, "check_via_mixed_proxy", return_value=(True, "200")), mock.patch.object(p, "run_step", side_effect=[(True, "API"), (True, "SSE"), (False, "WS disconnected")]) as run:
            ok, _ = m.comprehensive_route_check(self.args(), 7897, attempts=1)
        self.assertIs(ok, False)
        self.assertEqual(3, run.call_count)

    def test_auth_failure_does_not_retry_or_start_generation(self):
        with mock.patch.object(p, "settings", return_value=cfg()), mock.patch.object(m, "check_via_mixed_proxy", return_value=(True, "200")), mock.patch.object(p, "run_step", return_value=(None, "401")) as run:
            ok, _ = m.comprehensive_route_check(self.args(), 7897, attempts=3)
        self.assertIsNone(ok)
        self.assertEqual(1, run.call_count)

    def test_default_network_pipeline_never_calls_generation_probes(self):
        args = self.args()
        self.assertEqual("network", args.probe_mode)
        with mock.patch.object(p, "settings", return_value=replace(cfg(), mode="network", model="")), mock.patch.object(m, "check_via_mixed_proxy", return_value=(True, "200")), mock.patch.object(p, "run_step", return_value=(True, "pass")) as run:
            ok, message = m.comprehensive_route_check(args, 7897, attempts=1)
        self.assertIs(ok, True)
        self.assertEqual([p.probe_models, p.probe_websocket_network], [call.args[1] for call in run.call_args_list])
        self.assertIn("不执行模型生成", message)
        self.assertNotIn("response.completed", message)

    def test_blocked_route_does_not_test_or_switch_nodes_even_in_current_only_mode(self):
        with mock.patch.object(m, "api_json", side_effect=[{"mixed-port": 7897}, {"proxies": {}}]), mock.patch.object(m, "base_proxy_names_for_args", return_value=set()), mock.patch.object(m, "guarded_comprehensive_route_check", return_value=(None, "401")), mock.patch.object(m, "fetch_rules", return_value=[]), mock.patch.object(m, "log"), mock.patch.object(m, "auto_switch_if_needed") as switch, mock.patch.object(m, "run_delay_checks") as delay:
            outcome = m.run_once(m.Controller(), {}, self.args(), [], route_failure_streak=1)
        switch.assert_not_called()
        delay.assert_not_called()
        self.assertEqual(1, outcome.route_failure_streak)
        self.assertFalse(outcome.no_available_chatgpt_node)

    def test_once_exit_code_distinguishes_pass_failure_and_blocked(self):
        for state, expected in ((True, 0), (False, 1), (None, 5)):
            outcome = m.RunOutcome(None, None, route_ok=state)
            with self.subTest(state=state), mock.patch.object(sys, "argv", ["monitor", "--once", "--no-log-file"]), mock.patch.object(m, "config_paths", return_value=[]), mock.patch.object(m, "find_controller", return_value=(m.Controller(), {}, [])), mock.patch.object(m, "run_once", return_value=outcome), mock.patch.object(m.signal, "signal"), mock.patch.object(m, "log"):
                self.assertEqual(expected, m.main())


if __name__ == "__main__":
    unittest.main()
