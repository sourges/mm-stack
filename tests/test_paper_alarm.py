"""Paper market maker: the Coinbase alarm (a fast Coinbase jump takes one side off)."""

import contextlib
import io
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack.main import main
from mmstack.state import StateMachine
from tests.helpers import T0, event, sample_session
from tests.test_paper_mm import FLAT, LAT, MS, SEC, book, mm, started
from tests.test_replay import write_log


class AlarmCase(unittest.TestCase):
    """Kraken mid 100, our quotes live at 99.00 / 101.00, Coinbase at 100.
    Alarm: 2 bps (0.02) within 100 ms, side off for 1 s. Latency 100 ms."""

    def setUp(self):
        self.m = mm(alarm_bps=D("2"))
        self.bk = started(self.m)
        self.t = T0 + LAT
        self.m.on_reference(self.t, D("100"))

    def jump(self, price="100.02", after=50 * MS):
        self.m.on_reference(self.t + after, D(price))
        return self.t + after


class TestAlarm(AlarmCase):
    def test_jump_up_cancels_the_ask_only(self):
        t = self.jump("100.02")  # exactly 2 bps
        self.assertEqual(self.m.cur["ask"].dead_at, t + LAT)
        self.assertIsNone(self.m.cur["bid"].dead_at)
        self.assertEqual((self.m.stats["alarms_up"], self.m.stats["alarms_down"]), (1, 0))

    def test_jump_down_cancels_the_bid_only(self):
        t = self.jump("99.98")
        self.assertEqual(self.m.cur["bid"].dead_at, t + LAT)
        self.assertIsNone(self.m.cur["ask"].dead_at)
        self.assertEqual((self.m.stats["alarms_up"], self.m.stats["alarms_down"]), (0, 1))

    def test_small_move_does_nothing(self):
        self.jump("100.019")
        self.assertIsNone(self.m.cur["ask"].dead_at)
        self.assertEqual(self.m.stats["alarms_up"], 0)

    def test_slow_move_does_nothing(self):
        self.jump("100.02", after=101 * MS)  # same size, but outside the 100 ms window
        self.assertIsNone(self.m.cur["ask"].dead_at)

    def test_move_in_steps_inside_the_window_counts(self):
        self.jump("100.01", after=40 * MS)
        self.assertIsNone(self.m.cur["ask"].dead_at)
        self.jump("100.02", after=80 * MS)  # 2 bps above the lowest price in the window
        self.assertIsNotNone(self.m.cur["ask"].dead_at)

    def test_measured_from_the_extreme_not_the_oldest_price(self):
        self.jump("99.99", after=30 * MS)
        self.jump("100.01", after=60 * MS)  # only 1 bp above the start, but 2 bps off the low
        self.assertEqual(self.m.stats["alarms_up"], 1)

    def test_side_stays_off_then_comes_back(self):
        t = self.jump()
        self.m.on_book(t + 500 * MS, self.bk, True)
        self.assertIsNone(self.m.cur["ask"])  # cancelled, and not replaced during the hold
        self.assertIsNotNone(self.m.cur["bid"])
        self.m.on_book(t + SEC - 1, self.bk, True)
        self.assertIsNone(self.m.cur["ask"])
        self.m.on_book(t + SEC, self.bk, True)
        self.assertEqual(self.m.cur["ask"].price, D("101.00"))

    def test_can_still_be_hit_before_the_cancel_lands(self):
        t = self.jump()
        out = self.m.on_trade(t + 10 * MS, "buy", D("101.50"), D("1"))
        self.assertIn("stale", out[0])
        self.assertEqual(self.m.stats["alarm_fills"], 1)

    def test_fill_on_the_other_side_is_not_an_alarm_fill(self):
        t = self.jump()
        self.m.on_trade(t + 10 * MS, "sell", D("98.50"), D("1"))  # hits our bid
        self.assertEqual(self.m.stats["alarm_fills"], 0)

    def test_replacement_already_on_its_way_is_cancelled_when_it_arrives(self):
        moved = book("100.49", "100.51")  # mid 100.5: both quotes get replaced
        self.m.on_book(self.t + 10 * MS, moved, True)
        self.assertIsNotNone(self.m.nxt["ask"])
        self.jump("100.02", after=20 * MS)
        self.m.on_book(self.t + 10 * MS + LAT, moved, True)  # the replacement is now live
        self.assertEqual(self.m.cur["ask"].price, D("101.51"))
        self.assertIsNotNone(self.m.cur["ask"].dead_at)

    def test_second_jump_extends_without_counting_twice(self):
        t1 = self.jump("100.02", after=50 * MS)
        t2 = self.jump("100.05", after=90 * MS)  # a further 2 bps+
        self.assertEqual(self.m.stats["alarms_up"], 1)
        self.assertEqual(self.m.alarm_until["ask"], t2 + SEC)
        self.assertEqual(self.m.alarm_off_ns["ask"], t2 + SEC - t1)  # no double counting

    def test_same_jump_does_not_ring_again(self):
        self.jump("100.02", after=50 * MS)
        t2 = self.jump("100.03", after=60 * MS)  # still within 100 ms of the 100.00 price
        self.assertLess(self.m.alarm_until["ask"], t2 + SEC)

    def test_alarm_after_the_hold_is_a_new_alarm(self):
        self.jump("100.02", after=50 * MS)
        self.jump("100.02", after=2 * SEC)
        self.jump("100.05", after=2 * SEC + 50 * MS)
        self.assertEqual(self.m.stats["alarms_up"], 2)
        self.assertEqual(self.m.alarm_off_ns["ask"], 2 * SEC)

    def test_jump_across_a_coinbase_outage_is_ignored(self):
        self.m.on_reference(self.t + 10 * MS, None)
        self.jump("100.05", after=50 * MS)
        self.assertEqual(self.m.stats["alarms_up"], 0)

    def test_quiet_coinbase_ticks_do_not_cause_requotes(self):
        calcs = self.m.stats["quote_calcs"]
        self.jump("100.01")
        self.assertEqual(self.m.stats["quote_calcs"], calcs)

    def test_off_by_default(self):
        m = mm()
        started(m)
        m.on_reference(self.t, D("100"))
        m.on_reference(self.t + 50 * MS, D("101"))
        self.assertIsNone(m.cur["ask"].dead_at)
        self.assertIsNone(m.ref_mid)
        self.assertFalse(any("coinbase_alarm" in line for line in m.summary_lines(SEC)))

    def test_summary_line(self):
        self.jump()
        text = "\n".join(self.m.summary_lines(10 * SEC))
        self.assertIn("coinbase_alarm jump>=2bps within=100ms hold=1000ms alarms up=1 down=0 "
                      "bid_off=0.00% ask_off=10.00% fills_during_alarm=0", text)


class TestAlarmWiring(unittest.TestCase):
    def test_settings_reach_the_market_maker(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01", "alarm_bps": "2",
                                    "alarm_window_ms": 80, "alarm_hold_ms": 500})
        m = sm.paper["BTC/USD"]
        self.assertEqual((m.alarm_bps, m.alarm_window_ns, m.alarm_hold_ns), (D("2"), 80 * MS, 500 * MS))

    def test_old_logs_have_no_alarm(self):
        sm = StateMachine()
        sm.update(event(0, "main", "session", {"paper_mm": {"symbols": ["BTC/USD"], "spread": "0.01"}}))
        m = sm.paper["BTC/USD"]
        self.assertEqual((m.alarm_bps, m.alarm_window_ns, m.alarm_hold_ns), (D(0), 100 * MS, SEC))
        self.assertNotIn("alarm", "\n".join(sm.summary()))

    def test_command_line_settings(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            write_log(path, sample_session())
            for flags, expected in (
                    (["--mm-alarm-bps", "2"], "rate_limit=intermediate alarm=2bps/100ms/1000ms"),
                    (["--mm-alarm-bps", "1.5", "--mm-alarm-window-ms", "50", "--mm-alarm-hold-ms", "250"],
                     "alarm=1.5bps/50ms/250ms")):
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    main(["replay", path, "--paper-mm", "BTC/USD"] + flags)
                self.assertIn(expected, buf.getvalue())
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["replay", path, "--paper-mm", "BTC/USD"])
            self.assertNotIn("alarm", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
