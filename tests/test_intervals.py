from __future__ import annotations

import io
import signal
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import monitor_clash_verge as monitor


class AdaptiveIntervalTests(unittest.TestCase):
    def run_rounds(self, rounds, *, options=(), durations=None):
        """Exercise the real main loop without network requests or real waits."""
        waits = []
        handlers = {}
        durations = durations if durations is not None else [0] * len(rounds)
        clock = [value for i, elapsed in enumerate(durations) for value in (i * 1000, i * 1000 + elapsed)]

        def wait(seconds, should_stop):
            self.assertFalse(should_stop())
            waits.append(seconds)
            if len(waits) == len(rounds):
                handlers[signal.SIGTERM](signal.SIGTERM, None)
                self.assertTrue(should_stop())

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(sys, "argv", ["monitor", "--no-log-file", *options]))
            stack.enter_context(mock.patch.object(monitor, "setup_log_file", return_value=None))
            stack.enter_context(mock.patch.object(monitor, "close_log_file"))
            stack.enter_context(mock.patch.object(monitor, "config_paths", return_value=[]))
            stack.enter_context(mock.patch.object(monitor, "find_controller", return_value=(monitor.Controller(), {}, [])))
            run = stack.enter_context(mock.patch.object(monitor, "run_once", side_effect=rounds))
            stack.enter_context(mock.patch.object(monitor, "log"))
            stack.enter_context(mock.patch.object(monitor.signal, "signal", side_effect=lambda sig, handler: handlers.update({sig: handler})))
            stack.enter_context(mock.patch.object(monitor.time, "monotonic", side_effect=clock))
            stack.enter_context(mock.patch.object(monitor, "wait_for_next_check", side_effect=wait))
            exit_code = monitor.main()
            self.assertEqual(run.call_count, len(rounds))
        return waits, exit_code

    def healthy(self, **kwargs):
        return monitor.RunOutcome(None, None, route_ok=True, **kwargs)

    def test_startup_ramps_to_five_minutes_and_stays_there(self):
        waits, code = self.run_rounds([self.healthy()] * 12)
        self.assertEqual(waits, [30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 300, 300])
        self.assertEqual(code, 0)

    def test_all_error_paths_reset_then_ramp_again(self):
        failures = [
            monitor.RunOutcome(None, None, route_ok=False),
            monitor.RunOutcome(None, None, route_ok=None),
            self.healthy(had_errors=True),  # Retry or switch recovered this round.
            self.healthy(no_available_chatgpt_node=True),
            monitor.codex_probe.ProbeError("blocked"),
            monitor.ApiError("controller unavailable"),
            monitor.ProfileError("profile unavailable"),
            RuntimeError("unexpected failure"),
        ]
        for failure in failures:
            with self.subTest(failure=failure):
                waits, code = self.run_rounds([self.healthy()] * 10 + [failure] + [self.healthy()] * 11)
                self.assertEqual(waits[9:], [300, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 300, 300])
                self.assertEqual(code, 0)

    def test_repeated_failures_stay_short_and_new_process_starts_short(self):
        failure = monitor.RunOutcome(None, None, route_ok=False)
        waits, code = self.run_rounds([self.healthy()] * 3 + [failure] * 3)
        self.assertEqual(waits, [30, 60, 90, 30, 30, 30])
        self.assertEqual(code, 1)
        self.assertEqual(self.run_rounds([self.healthy()])[0], [30])

    def test_custom_intervals_and_start_above_cap(self):
        waits, _ = self.run_rounds([self.healthy()] * 5, options=("--interval", "100", "--retry-interval", "20", "--interval-step", "35"))
        self.assertEqual(waits, [20, 55, 90, 100, 100])
        waits, _ = self.run_rounds([self.healthy()] * 3, options=("--interval", "10"))
        self.assertEqual(waits, [10, 10, 10])

    def test_check_duration_is_deducted_and_long_round_does_not_wait(self):
        waits, _ = self.run_rounds([self.healthy()] * 3, durations=[4, 80, 15])
        self.assertEqual(waits, [26, 0, 75])

    def test_once_never_waits_and_retains_exit_codes(self):
        for status, expected in ((True, 0), (False, 1), (None, 5)):
            with self.subTest(status=status):
                waits, code = self.run_rounds([monitor.RunOutcome(None, None, route_ok=status)], options=("--once",))
                self.assertEqual(waits, [])
                self.assertEqual(code, expected)

    def test_interval_options_must_be_positive(self):
        for option in ("--interval", "--retry-interval", "--interval-step"):
            for value in ("0", "-1"):
                with self.subTest(option=option, value=value), mock.patch.object(sys, "argv", ["monitor", option, value]), mock.patch.object(sys, "stderr", new_callable=io.StringIO):
                    with self.assertRaises(SystemExit) as error:
                        monitor.parse_args()
                    self.assertEqual(error.exception.code, 2)

    def test_later_success_does_not_erase_earlier_probe_error(self):
        with mock.patch.object(sys, "argv", ["monitor"]):
            args = monitor.parse_args()
        for status in (False, None):
            with self.subTest(status=status), mock.patch.object(monitor, "comprehensive_route_check", side_effect=[(status, "failure"), (True, "recovered")]):
                args._round_probe_had_errors = False
                monitor.guarded_comprehensive_route_check(args, 7897)
                monitor.guarded_comprehensive_route_check(args, 7897)
                self.assertTrue(args._round_probe_had_errors)


if __name__ == "__main__":
    unittest.main()
