"""Paper market maker: the imbalance alarm (a book that tips hard one way
takes the side it leans against off)."""

import contextlib
import io
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack.main import main
from mmstack.state import StateMachine
from tests.helpers import T0, event, sample_session
from tests.test_paper_mm import LAT, MS, SEC, book, mm, started
from tests.test_replay import write_log

EVEN = ([("99.99", "1")], [("100.01", "1")])         # reading 0
UP = ([("99.99", "9")], [("100.01", "1")])           # touch 0.8
DOWN = ([("99.99", "1")], [("100.01", "9")])         # touch -0.8


class TestReadings(unittest.TestCase):
    def test_touch(self):
        m = mm(imb_reading="touch")
        self.assertEqual(m.book_reading(book(*UP)), D("0.8"))
        self.assertEqual(m.book_reading(book([("99.99", "1"), ("99.98", "100")], [("100.01", "1")])), 0)

    def test_weighted_hand_worked(self):
        # tick 0.01, spread 0.02: next level weighs 1/(1+0.5)^2 = 4/9
        m = mm()
        b = book([("99.99", "1"), ("99.98", "9")], [("100.01", "1")])
        self.assertEqual(m.book_reading(b), (D(1) + D(4) - 1) / (D(1) + D(4) + 1))

    def test_weighted_levels(self):
        m = mm(imb_levels=1)
        self.assertEqual(m.book_reading(book([("99.99", "1"), ("99.98", "9")], [("100.01", "1")])), 0)

    def test_depth_window(self):
        m = mm(imb_reading="depth", imb_bps=D("5"))  # mid 100: 99.95 .. 100.05 count
        b = book([("99.99", "1"), ("99.95", "2"), ("99.94", "50")], [("100.01", "1"), ("100.06", "50")])
        self.assertEqual(m.book_reading(b), D(2) / D(4))  # bids 3, asks 1



class Alarm(unittest.TestCase):
    def setUp(self):
        self.m = mm(imb_threshold=D("0.6"), imb_reading="touch")
        started(self.m, *EVEN)
        self.t = T0 + LAT

    def at(self, ms, levels):
        t = self.t + ms * MS
        self.m.on_book(t, book(*levels), True)
        return t


class TestImbalanceAlarm(Alarm):
    def test_tipping_up_pulls_the_ask(self):
        t = self.at(10, UP)
        self.assertEqual(self.m.alarm_until["ask"], t + SEC)
        self.assertEqual(self.m.alarm_until["bid"], 0)
        self.assertIsNotNone(self.m.cur["ask"].dead_at)  # cancel sent at once
        self.assertIsNone(self.m.cur["bid"].dead_at)
        self.assertEqual((self.m.stats["imb_alarms_up"], self.m.stats["imb_alarms_down"]), (1, 0))

    def test_tipping_down_pulls_the_bid(self):
        t = self.at(10, DOWN)
        self.assertEqual(self.m.alarm_until["bid"], t + SEC)
        self.assertEqual(self.m.stats["imb_alarms_down"], 1)

    def test_standing_lean_rings_once(self):
        t = self.at(10, UP)
        self.at(500, UP)
        self.at(2000, UP)
        self.assertEqual(self.m.stats["imb_alarms_up"], 1)
        self.assertEqual(self.m.alarm_until["ask"], t + SEC)  # not extended
        self.assertEqual(self.m.stats["imb_past_threshold"], 3)

    def test_rings_again_after_easing(self):
        self.at(10, UP)
        self.at(20, EVEN)
        t = self.at(30, UP)
        self.assertEqual(self.m.alarm_until["ask"], t + SEC)
        self.assertEqual(self.m.stats["imb_alarms_up"], 1)  # still inside the first hold: an extension
        self.at(3000, EVEN)
        self.at(3010, UP)
        self.assertEqual(self.m.stats["imb_alarms_up"], 2)

    def test_flip_straight_to_the_other_side(self):
        self.at(10, UP)
        self.at(20, DOWN)
        self.assertEqual((self.m.stats["imb_alarms_up"], self.m.stats["imb_alarms_down"]), (1, 1))

    def test_below_threshold_does_nothing(self):
        self.at(10, ([("99.99", "3")], [("100.01", "1")]))  # 0.5
        self.assertEqual(self.m.alarm_until["ask"], 0)
        self.assertEqual(self.m.stats["imb_readings"], 3)  # 2 in started() + this one

    def test_exactly_at_threshold_rings(self):
        self.at(10, ([("99.99", "4")], [("100.01", "1")]))  # 0.6
        self.assertEqual(self.m.stats["imb_alarms_up"], 1)

    def test_side_comes_back_after_the_hold(self):
        t = self.at(10, UP)
        self.at(500, UP)
        self.assertIsNone(self.m.cur["ask"])
        self.m.on_book(t + SEC, book(*UP), True)
        self.assertIsNotNone(self.m.cur["ask"] or self.m.nxt["ask"])

    def test_off_time(self):
        self.at(10, UP)
        self.at(20, EVEN)
        self.at(510, UP)  # extends to 1.51 s: 0.5 s more
        self.assertEqual(self.m.imb_off_ns["ask"], SEC + 500 * MS)

    def test_does_not_shorten_the_coinbase_alarm(self):
        self.m.alarm_until["ask"] = self.t + 5 * SEC
        self.at(10, UP)
        self.assertEqual(self.m.alarm_until["ask"], self.t + 5 * SEC)
        self.assertEqual(self.m.stats["imb_alarms_up"], 0)  # the side was already off
        self.assertEqual(self.m.imb_off_ns["ask"], 0)

    def test_coinbase_alarm_does_not_shorten_it(self):
        self.m.alarm_hold_ns = 100 * MS
        t = self.at(10, UP)
        self.m._alarm(t + 1, "ask")
        self.assertEqual(self.m.alarm_until["ask"], t + SEC)
        self.assertEqual(self.m.alarm_off_ns["ask"], 0)

    def test_summary(self):
        self.at(10, UP)
        text = "\n".join(self.m.summary_lines(10 * SEC))
        self.assertIn("imbalance_alarm touch at>=0.6 hold=1000ms alarms up=1 down=0 "
                      "bid_off=0.00% ask_off=10.00% books_past_threshold=33.3%", text)

    def test_summary_names(self):
        self.assertIn("imbalance_alarm weighted10 at>=0.8", "\n".join(mm(imb_threshold=D("0.8")).summary_lines(SEC)))
        self.assertIn("imbalance_alarm depth2.5bps at>=0.8", "\n".join(
            mm(imb_threshold=D("0.8"), imb_reading="depth", imb_bps=D("2.5")).summary_lines(SEC)))


class TestOff(unittest.TestCase):
    def test_off_by_default(self):
        m = mm()
        started(m, *EVEN)
        m.on_book(T0 + LAT + 10 * MS, book(*UP), True)
        self.assertEqual((m.stats["imb_readings"], m.alarm_until["ask"]), (0, 0))
        self.assertNotIn("imbalance_alarm", "\n".join(m.summary_lines(SEC)))


class TestWiring(unittest.TestCase):
    def test_settings_reach_the_market_maker(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01", "imb_alarm": "0.8",
                                    "imb_hold_ms": 250, "imb_reading": "depth", "imb_levels": 5,
                                    "imb_bps": "2"})
        m = sm.paper["BTC/USD"]
        self.assertEqual((m.imb_threshold, m.imb_hold_ns, m.imb_reading, m.imb_levels, m.imb_bps),
                         (D("0.8"), 250 * MS, "depth", 5, D(2)))
        self.assertIn("imb_alarm=0.8/depth2bps/250ms", "\n".join(sm.summary()))

    def test_old_logs_have_none(self):
        sm = StateMachine()
        sm.update(event(0, "main", "session", {"paper_mm": {"symbols": ["BTC/USD"], "spread": "0.01"}}))
        m = sm.paper["BTC/USD"]
        self.assertEqual((m.imb_threshold, m.imb_hold_ns, m.imb_reading, m.imb_levels, m.imb_bps),
                         (D(0), SEC, "weighted", 10, D(5)))
        self.assertNotIn("imb_alarm", "\n".join(sm.summary()))

    def test_command_line(self):
        import argparse
        from mmstack.main import add_paper_args, paper_config
        p = argparse.ArgumentParser()
        add_paper_args(p)
        self.assertNotIn("imb_alarm", paper_config(p.parse_args(["--paper-mm"])))
        cfg = paper_config(p.parse_args(["--paper-mm", "--mm-imb-alarm", "0.9", "--mm-imb-hold-ms", "300",
                                         "--mm-imb-reading", "touch", "--mm-imb-levels", "3",
                                         "--mm-imb-bps", "4"]))
        self.assertEqual({k: cfg[k] for k in ("imb_alarm", "imb_hold_ms", "imb_reading", "imb_levels", "imb_bps")},
                         {"imb_alarm": "0.9", "imb_hold_ms": 300, "imb_reading": "touch", "imb_levels": 3,
                          "imb_bps": "4"})
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            write_log(path, sample_session())
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["replay", path, "--paper-mm", "BTC/USD", "--mm-imb-alarm", "0.8"])
        self.assertIn("imb_alarm=0.8/weighted10/1000ms", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
