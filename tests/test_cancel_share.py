"""--mm-cancel-share: where cancels at our price come from (2026-11-26 review).

The size resting at our price drops by more than the trades explain: those
are cancels, but we can't see whether they were ahead of us or behind us.
  even   (default): spread evenly through the queue (the old rule)
  ahead  (optimistic): ahead of us first, so we move up
  behind (pessimistic): behind us first; only what doesn't fit behind
         comes from ahead
Running a test both ways shows whether a result depends on this guess."""

import argparse
import unittest
from decimal import Decimal as D

from mmstack import main as m
from mmstack.paper_mm import PaperMarketMaker
from mmstack.state import StateMachine
from tests.test_paper_mm import MS, SEC, T0, book, mm, started

BIDS = [("99.99", "1"), ("99.00", "5")]  # 5 resting at our bid price: all ahead of us


def queue_after(share, joins, cancels):
    """5 ahead; `joins` join behind us; then `cancels` cancel. Returns size ahead."""
    m_ = mm(cancel_share=share) if share else mm()
    bk = started(m_, BIDS, "100.01")
    q = m_.bid
    bk.bids.set(D("99.00"), D(5 + joins))
    m_.on_book(T0 + SEC, bk, True)
    bk.bids.set(D("99.00"), D(5 + joins - cancels))
    m_.on_book(T0 + 2 * SEC, bk, True)
    return q.ahead


class TestShares(unittest.TestCase):
    def test_even_is_the_default_and_unchanged(self):
        self.assertEqual(queue_after(None, 5, 4), D(3))  # 5 - 4 * 5/10, as before
        self.assertEqual(queue_after("even", 5, 4), D(3))

    def test_ahead_moves_us_up_by_all_of_it(self):
        self.assertEqual(queue_after("ahead", 5, 4), D(1))  # 5 - 4

    def test_ahead_cant_go_below_zero(self):
        self.assertEqual(queue_after("ahead", 5, 7), D(0))

    def test_behind_doesnt_move_us(self):
        self.assertEqual(queue_after("behind", 5, 4), D(5))  # all 4 fit in the 5 behind

    def test_behind_overflow_comes_from_ahead(self):
        self.assertEqual(queue_after("behind", 2, 4), D(3))  # only 2 behind: 2 more from ahead

    def test_bad_share_refused(self):
        with self.assertRaises(ValueError):
            PaperMarketMaker("TEST", cancel_share="sideways")


class TestPlumbing(unittest.TestCase):
    def parser(self):
        p = argparse.ArgumentParser()
        m.add_paper_args(p)
        return p

    def test_flag_and_config(self):
        cfg = m.paper_config(self.parser().parse_args(["--paper-mm", "BTC/USD"]))
        self.assertNotIn("cancel_share", cfg)  # only written when used
        cfg = m.paper_config(self.parser().parse_args(["--paper-mm", "BTC/USD",
                                                       "--mm-cancel-share", "behind"]))
        self.assertEqual(cfg["cancel_share"], "behind")

    def test_reaches_the_booth_and_settings_line(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01", "cancel_share": "behind"})
        self.assertEqual(sm.paper["BTC/USD"].cancel_share, "behind")
        self.assertIn(" cancels=behind", "\n".join(sm.summary()))

    def test_default_settings_line_unchanged(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01"})
        self.assertEqual(sm.paper["BTC/USD"].cancel_share, "even")
        self.assertNotIn("cancels=", "\n".join(sm.summary()))


if __name__ == "__main__":
    unittest.main()
