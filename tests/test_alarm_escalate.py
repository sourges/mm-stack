"""Escalating alarm hold (--mm-alarm-max-hold-ms): an alarm that keeps ringing
holds its side off longer, so a fast market can't turn it into a stream of
cancel + re-place (each ~9 points of Kraken's rate budget)."""

import contextlib
import io
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack.main import main
from mmstack.paper_mm import ESCALATE_CALM_NS, ESCALATE_SOON_NS
from mmstack.state import StateMachine
from tests.helpers import T0, sample_session
from tests.test_paper_alarm import AlarmCase
from tests.test_paper_mm import MS, SEC, mm
from tests.test_replay import write_log


class TestHoldSteps(unittest.TestCase):
    """alarm_hold() alone: base 1 s, up to 8 s."""

    def setUp(self):
        self.m = mm(alarm_bps=D("2"), alarm_max_hold_ns=8 * SEC)

    def ring(self, t, side="ask"):
        self.m._alarm(t, side)
        return self.m.alarm_until[side] - t

    def test_first_alarm_holds_the_base_time(self):
        self.assertEqual(self.ring(T0), SEC)

    def test_ringing_again_soon_doubles_up_to_the_cap(self):
        t, holds = T0, []
        for _ in range(6):
            holds.append(self.ring(t))
            t = self.m.alarm_until["ask"] + 2 * SEC  # rings 2 s after each hold ends
        self.assertEqual(holds, [SEC, 2 * SEC, 4 * SEC, 8 * SEC, 8 * SEC, 8 * SEC])
        self.assertEqual(self.m.stats["alarm_escalated"], 5)
        self.assertEqual(self.m.stats["alarm_longest_hold_ms"], 8000)

    def test_soon_means_under_5_seconds(self):
        self.ring(T0)
        self.assertEqual(self.ring(self.m.alarm_until["ask"] + ESCALATE_SOON_NS - 1), 2 * SEC)
        m = mm(alarm_bps=D("2"), alarm_max_hold_ns=8 * SEC)
        m._alarm(T0, "ask")
        m._alarm(m.alarm_until["ask"] + ESCALATE_SOON_NS, "ask")  # exactly 5 s: not soon
        self.assertEqual(m.stats["alarm_escalated"], 0)

    def test_between_soon_and_calm_the_step_stays(self):
        self.ring(T0)
        self.ring(self.m.alarm_until["ask"] + SEC)  # 2 s hold
        self.assertEqual(self.ring(self.m.alarm_until["ask"] + 30 * SEC), 2 * SEC)

    def test_a_calm_minute_resets(self):
        self.ring(T0)
        self.ring(self.m.alarm_until["ask"] + SEC)
        self.ring(self.m.alarm_until["ask"] + SEC)  # 4 s
        self.assertEqual(self.ring(self.m.alarm_until["ask"] + ESCALATE_CALM_NS), SEC)

    def test_ringing_during_a_hold_keeps_the_step(self):
        self.ring(T0)
        self.ring(self.m.alarm_until["ask"] + SEC)  # step 1: 2 s
        t = self.m.alarm_until["ask"] - 500 * MS  # still running
        self.ring(t)
        self.assertEqual(self.m.alarm_until["ask"], t + 2 * SEC)
        self.assertEqual(self.m.stats["alarm_escalated"], 1)

    def test_sides_escalate_separately(self):
        self.ring(T0, "ask")
        self.ring(self.m.alarm_until["ask"] + SEC, "ask")  # ask at 2 s
        self.assertEqual(self.ring(self.m.alarm_until["ask"] + SEC, "bid"), SEC)  # bid's first
        self.assertEqual(self.ring(self.m.alarm_until["ask"] + SEC, "ask"), 4 * SEC)  # ask carries on

    def test_cap_that_is_not_a_power_of_two(self):
        m = mm(alarm_bps=D("2"), alarm_max_hold_ns=3 * SEC)
        t, holds = T0, []
        for _ in range(4):
            m._alarm(t, "ask")
            holds.append(m.alarm_until["ask"] - t)
            t = m.alarm_until["ask"] + SEC
        self.assertEqual(holds, [SEC, 2 * SEC, 3 * SEC, 3 * SEC])

    def test_off_by_default_and_when_cap_is_not_above_the_base(self):
        for m in (mm(alarm_bps=D("2")), mm(alarm_bps=D("2"), alarm_max_hold_ns=SEC),
                  mm(alarm_bps=D("2"), alarm_max_hold_ns=500 * MS)):  # a cap below the base never shortens it
            t = T0
            for _ in range(4):
                m._alarm(t, "ask")
                self.assertEqual(m.alarm_until["ask"] - t, SEC)
                t = m.alarm_until["ask"] + SEC
            self.assertEqual(m.stats["alarm_escalated"], 0)


class TestEscalateInTheBooth(AlarmCase):
    """Through real Coinbase jumps, as in the alarm tests."""

    def test_repeated_jumps_hold_longer_and_off_time_counts_it(self):
        self.m.alarm_max_hold_ns = 8 * SEC
        self.jump("100.02", after=50 * MS)  # alarm 1: hold 1 s, ends at +1.05 s
        self.jump("100.02", after=3 * SEC)  # no jump (nothing within 100 ms before it)
        self.jump("100.05", after=3 * SEC + 50 * MS)  # alarm 2, 2 s after alarm 1 ended: hold 2 s
        self.assertEqual(self.m.stats["alarms_up"], 2)
        self.assertEqual(self.m.alarm_until["ask"], self.t + 3 * SEC + 50 * MS + 2 * SEC)
        self.assertEqual(self.m.alarm_off_ns["ask"], SEC + 2 * SEC)

    def test_summary_line(self):
        self.m.alarm_max_hold_ns = 8 * SEC
        self.jump()
        text = "\n".join(self.m.summary_lines(10 * SEC))
        self.assertIn("fills_during_alarm=0 escalate_to=8000ms escalated=0 longest_hold=1000ms", text)

    def test_summary_line_without_escalation_unchanged(self):
        self.jump()
        line = [x for x in self.m.summary_lines(10 * SEC) if "coinbase_alarm" in x][0]
        self.assertTrue(line.endswith("fills_during_alarm=0"))


class TestWiring(unittest.TestCase):
    def test_setting_reaches_the_booth(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01", "alarm_bps": "2",
                                    "alarm_max_hold_ms": 8000})
        self.assertEqual(sm.paper["BTC/USD"].alarm_max_hold_ns, 8 * SEC)
        self.assertEqual(StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01"})
                         .paper["BTC/USD"].alarm_max_hold_ns, 0)

    def test_command_line(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            write_log(path, sample_session())
            outs = {}
            for name, flags in (("on", ["--mm-alarm-max-hold-ms", "8000"]), ("off", [])):
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    main(["replay", path, "--paper-mm", "BTC/USD", "--mm-alarm-bps", "2", *flags])
                outs[name] = buf.getvalue()
        self.assertIn("alarm=2bps/100ms/1000ms_up_to_8000ms", outs["on"])
        self.assertIn("alarm=2bps/100ms/1000ms\n", outs["off"])
        self.assertNotIn("up_to", outs["off"])

    def test_config_only_written_when_used(self):
        import argparse
        from mmstack import main as m
        p = argparse.ArgumentParser()
        m.add_paper_args(p)  # it defines --paper-mm itself
        self.assertNotIn("alarm_max_hold_ms", m.paper_config(p.parse_args(["--paper-mm", "BTC/USD"])))
        cfg = m.paper_config(p.parse_args(["--paper-mm", "BTC/USD", "--mm-alarm-max-hold-ms", "8000"]))
        self.assertEqual(cfg["alarm_max_hold_ms"], 8000)


if __name__ == "__main__":
    unittest.main()
