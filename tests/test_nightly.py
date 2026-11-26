"""Daily diaries (live --until-midnight) and the nightly run."""

import contextlib
import io
import json
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime
from unittest import mock

from mmstack import days, main as m
from tests.test_runner import write_day_diary, write_exp, HOUR
from tests.helpers import T0

NY = days.new_york()


def ny(text):
    return datetime.fromisoformat(text).replace(tzinfo=NY).timestamp()


class TestNextMidnight(unittest.TestCase):
    def test_morning(self):
        self.assertEqual(m.next_midnight(ny("2026-11-24T09:17:00"), NY), ny("2026-11-25T00:00:00"))

    def test_one_second_before(self):
        self.assertEqual(m.next_midnight(ny("2026-11-24T23:59:59"), NY), ny("2026-11-25T00:00:00"))

    def test_just_after_midnight_is_the_next_one(self):
        self.assertEqual(m.next_midnight(ny("2026-11-25T00:00:01"), NY), ny("2026-11-26T00:00:00"))

    def test_not_just_24_hours_later(self):
        # the day before spring-forward: from 9 am to the next midnight is 15 h on the clock
        self.assertEqual(m.next_midnight(ny("2026-03-07T09:00:00"), NY), ny("2026-03-08T00:00:00"))

    def test_daylight_saving_days(self):
        # spring forward (23 h day) and fall back (25 h day): still local midnight
        self.assertEqual(m.next_midnight(ny("2026-03-08T12:00:00"), NY) - ny("2026-03-08T00:00:00"), 23 * 3600)
        self.assertEqual(m.next_midnight(ny("2026-11-01T12:00:00"), NY) - ny("2026-11-01T00:00:00"), 25 * 3600)

    def test_is_new_york_not_utc(self):
        # 8 pm New York = 01:00 UTC next day; the next New York midnight is 4 h away, not 23
        self.assertEqual(m.next_midnight(ny("2026-11-24T20:00:00"), NY) - ny("2026-11-24T20:00:00"), 4 * 3600)


class FakeReactor:
    """Stands in for the network: writes one market message, then waits."""

    def __init__(self, writer, on_start=None):
        self.writer = writer
        self.on_start = on_start

    def run(self, stop):
        self.writer.write("kraken", "ws_in", {"text": '{"channel":"heartbeat"}'})
        if self.on_start:
            self.on_start()
        while not stop.is_set():
            time.sleep(0.01)


class TestUntilMidnight(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.handlers = {}

    def tearDown(self):
        self.dir.cleanup()

    def live(self, *flags, stop_in=0.3, press_ctrl_c=False):
        def reactor(clients, writer):
            press = (lambda: self.handlers[m.signal.SIGINT](None, None)) if press_ctrl_c else None
            return FakeReactor(writer, on_start=press)
        patches = [mock.patch.object(m, "build_clients", lambda *a, **k: []),
                   mock.patch.object(m, "build_reactor", reactor),
                   mock.patch.object(m.signal, "signal", lambda sig, h: self.handlers.__setitem__(sig, h)),
                   mock.patch.object(m, "next_midnight", lambda now, tz: now + stop_in)]
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            err = stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
            # watchdog: if it never stops by itself, press Ctrl+C after 5 s so the test fails, not hangs
            watchdog = threading.Timer(5, lambda: self.handlers[m.signal.SIGINT](None, None))
            watchdog.start()
            t0 = time.time()
            try:
                m.main(["live", "--data-dir", self.dir.name, *flags])
                code = None
            except SystemExit as e:
                code = e.code
            finally:
                watchdog.cancel()
            return code, time.time() - t0, err.getvalue()

    def test_stops_by_itself_at_midnight_with_exit_code_0(self):
        code, secs, err = self.live("--until-midnight")
        self.assertIsNone(code)  # normal exit: recorder.bat starts the next diary
        self.assertLess(secs, 5)
        self.assertIn("--until-midnight: stops at", err)
        logs = [n for n in os.listdir(self.dir.name) if n.endswith(".jsonl")]
        self.assertEqual(len(logs), 1)
        with open(os.path.join(self.dir.name, logs[0]), encoding="utf-8") as f:
            types = [json.loads(line)["type"] for line in f]
        self.assertEqual(types[:2], ["session", "ws_in"])

    def test_ctrl_c_exits_with_code_3_so_the_bat_stops(self):
        code, _, _ = self.live("--until-midnight", stop_in=60, press_ctrl_c=True)
        self.assertEqual(code, m.STOPPED_BY_USER)

    def test_without_the_flag_ctrl_c_is_a_plain_exit(self):
        code, _, err = self.live(stop_in=0.01, press_ctrl_c=True)
        self.assertIsNone(code)
        self.assertNotIn("until-midnight", err)

    def test_without_the_flag_it_does_not_stop_at_midnight(self):
        stopper = threading.Timer(0.5, lambda: self.handlers[m.signal.SIGINT](None, None))
        stopper.start()
        code, secs, _ = self.live(stop_in=0.01)  # "midnight" passes at once, but nobody watches it
        stopper.join()
        self.assertGreaterEqual(secs, 0.45)


class TestNightly(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = self.dir.name
        self.data = os.path.join(root, "data")
        self.results = os.path.join(root, "results")
        self.exps = os.path.join(root, "experiments")
        os.mkdir(self.data)
        os.mkdir(self.exps)
        write_day_diary(os.path.join(self.data, "20251112-000000.jsonl"), start=T0)
        write_day_diary(os.path.join(self.data, "20251113-000000.jsonl"), start=T0 + 24 * HOUR)
        write_exp(self.exps, {
            "question": "Does a 1% spread change the fills?",
            "base": ["--paper-mm", "BTC/USD", "--mm-fills", "through"],
            "change": ["--mm-spread", "1"],
            "judge": {"number": "fills", "better": "higher"},
            "needs": ["kraken"], "tuned_on": []}, "a-spread.json")
        with open(os.path.join(self.exps, "notes.txt"), "w") as f:
            f.write("not an experiment")

    def tearDown(self):
        self.dir.cleanup()

    def nightly(self):
        buf = io.StringIO()
        code = None
        with contextlib.redirect_stdout(buf):
            try:
                m.main(["nightly", "--experiments", self.exps, "--data", self.data,
                        "--results", self.results])
            except SystemExit as e:
                code = e.code
        logs = [n for n in os.listdir(self.results) if n.startswith("nightly-")]
        with open(os.path.join(self.results, logs[0]), encoding="utf-8") as f:
            return code, buf.getvalue(), f.read()

    def test_scan_run_compress_in_order(self):
        code, screen, log = self.nightly()
        self.assertIsNone(code)
        self.assertEqual(screen.splitlines()[:-1], log.splitlines())  # the log is what was shown
        order = [log.index(x) for x in ("scanning 20251112-000000.jsonl", "--- a-spread.json ---",
                                        "BTC/USD   weekday  NOT ENOUGH EVIDENCE",
                                        "compressed ", "=== done in")]
        self.assertEqual(order, sorted(order))
        self.assertIn("0 problem(s)", log)
        self.assertIn("usable days: 2 weekday, 0 weekend", log)  # the day list is in the morning log
        self.assertNotIn("notes.txt", log)
        self.assertEqual(sorted(os.listdir(self.data)),
                         ["20251112-000000.jsonl.gz", "20251113-000000.jsonl.gz", "days_index.json"])

    def test_second_night_reuses_everything(self):
        self.nightly()
        _, _, log = self.nightly()
        self.assertNotIn("scanning", log.split("=== nightly")[-1])  # the .gz files keep their scans
        self.assertIn("4 replays: 4 saved from earlier runs, 0 to run", log.split("=== nightly")[-1])

    def test_a_broken_experiment_doesnt_stop_the_rest(self):
        write_exp(self.exps, {"question": "no judge"}, "0-broken.json")
        code, _, log = self.nightly()
        self.assertEqual(code, 1)
        self.assertIn("FAILED 0-broken.json", log)
        self.assertIn("--- a-spread.json ---", log)
        self.assertIn("compressed ", log)
        self.assertIn("1 problem(s)", log)

    def test_recording_diary_is_left_alone(self):
        live = os.path.join(self.data, "20251114-000000.jsonl")
        write_day_diary(live, start=T0 + 48 * HOUR)
        os.utime(live, None)  # just written: still recording
        _, _, log = self.nightly()
        self.assertIn("not compressed (still recording):", log)
        self.assertTrue(os.path.exists(live))


class TestShippedFiles(unittest.TestCase):
    def test_bat_files_use_the_right_commands(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(m.__file__)))
        with open(os.path.join(root, "scripts", "recorder.bat"), encoding="utf-8") as f:
            rec = f.read()
        self.assertIn("python -m mmstack.main live --paper-mm --data-dir E:\\mmdata --until-midnight", rec)
        self.assertIn(f"if %errorlevel%=={m.STOPPED_BY_USER} goto end", rec)
        self.assertNotIn("--out", rec)
        with open(os.path.join(root, "scripts", "nightly.bat"), encoding="utf-8") as f:
            self.assertIn("python -m mmstack.main nightly", f.read())

    def test_binance_alarm_experiment(self):
        from mmstack import runner
        root = os.path.dirname(os.path.dirname(os.path.abspath(m.__file__)))
        exp = runner.load_experiment(os.path.join(root, "experiments", "binance-vs-coinbase-alarm-link.json"))
        self.assertEqual(exp["change"], ["--mm-ref", "binance"])
        self.assertIn("binance", exp["needs"])


if __name__ == "__main__":
    unittest.main()
