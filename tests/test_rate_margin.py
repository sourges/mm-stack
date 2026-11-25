"""Rate safety margin (--mm-rate-margin): new orders and re-quotes stop short of
Kraken's limit, in case Kraken's live counter differs a little from our model.
Cancels may still use the full limit: they only ever reduce risk."""

import argparse
import contextlib
import io
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack import main as m
from mmstack.paper_mm import ADD_COST, MAX_CANCEL_COST, PaperQuote
from mmstack.state import StateMachine
from tests.helpers import T0, sample_session
from tests.test_paper_mm import SEC, mm
from tests.test_replay import write_log


def at_level(booth, level):
    c = booth.rate["intermediate"]
    c.counter, c.last_t = D(level), T0
    return c


class TestMargin(unittest.TestCase):
    def test_new_orders_stop_below_the_margin(self):
        # one order resting after this one: needs level + 1 + 8 within the ceiling
        for margin, level, ok in ((0, 116, True), (0, 117, False),  # ceiling 125
                                  (15, 101, True), (15, 102, False)):  # ceiling 110
            b = mm(rate_tier="intermediate", rate_margin=D(margin))
            at_level(b, level)
            self.assertEqual(b._affordable(T0, "bid", ADD_COST, 1), ok, (margin, level))

    def test_held_back_counted_once(self):
        b = mm(rate_tier="intermediate", rate_margin=D(15))
        at_level(b, 105)
        b._affordable(T0, "bid", ADD_COST, 1)
        b._affordable(T0, "bid", ADD_COST, 1)
        self.assertEqual(b.stats["held_back"], 1)

    def test_cancels_may_use_the_full_limit(self):
        b = mm(rate_tier="intermediate", rate_margin=D(15))
        c = at_level(b, 115)  # above the 110 ceiling, below 125
        q = PaperQuote(D("99"), D("1"), T0, T0)
        self.assertTrue(b._cancel(T0, q))  # 115 + 8 = 123 <= 125
        self.assertEqual(c.counter, 115 + MAX_CANCEL_COST)
        self.assertEqual(b.stats["cancel_blocked"], 0)

    def test_bad_margins(self):
        with self.assertRaises(ValueError):
            mm(rate_tier="intermediate", rate_margin=D(-1))
        with self.assertRaises(ValueError):
            mm(rate_tier="intermediate", rate_margin=D(125))
        mm(rate_tier="starter", rate_margin=D(59))  # 1 point left: allowed
        mm(rate_tier=None, rate_margin=D(500))  # no limit modelled: nothing to check

    def test_summary_shows_the_margin(self):
        b = mm(rate_tier="intermediate", rate_margin=D(15))
        line = [x for x in b.summary_lines(SEC) if "rate_limit enforced" in x][0]
        self.assertIn("rate_limit enforced=intermediate margin=15 held_back=0", line)
        self.assertIn("/125", line)  # the real limit, so the runner's ceiling check still means 125
        plain = [x for x in mm(rate_tier="intermediate").summary_lines(SEC) if "rate_limit enforced" in x][0]
        self.assertNotIn("margin", plain)


class TestWiring(unittest.TestCase):
    def parser(self):
        p = argparse.ArgumentParser()
        m.add_paper_args(p)
        return p

    def test_config_only_written_when_used(self):
        p = self.parser()
        self.assertNotIn("rate_margin", m.paper_config(p.parse_args(["--paper-mm", "BTC/USD"])))
        cfg = m.paper_config(p.parse_args(["--paper-mm", "BTC/USD", "--mm-rate-margin", "15"]))
        self.assertEqual(cfg["rate_margin"], 15)

    def test_command_line_refuses_a_margin_with_no_room(self):
        with self.assertRaises(SystemExit) as e:
            m.paper_config(self.parser().parse_args(["--paper-mm", "BTC/USD", "--mm-rate-margin", "125"]))
        self.assertIn("below the intermediate limit (125)", str(e.exception.code))

    def test_reaches_the_booth_and_the_settings_line(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01",
                                    "rate_tier": "intermediate", "rate_margin": 15})
        self.assertEqual(sm.paper["BTC/USD"].rate_margin, 15)
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            write_log(path, sample_session())
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                m.main(["replay", path, "--paper-mm", "BTC/USD", "--mm-rate-margin", "15"])
        self.assertIn("rate_limit=intermediate_margin_15", buf.getvalue())
        self.assertIn("margin=15 held_back=", buf.getvalue())

    def test_old_settings_have_no_margin(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01", "rate_tier": "intermediate"})
        self.assertEqual(sm.paper["BTC/USD"].rate_margin, 0)


if __name__ == "__main__":
    unittest.main()
