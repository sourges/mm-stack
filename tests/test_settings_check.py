"""Bad paper-booth settings are refused with a readable message, not a crash
mid-replay (2026-11-26 review: --mm-max-inv 0 and a 0 ms alarm window both
crashed with a division error)."""

import argparse
import unittest
from decimal import Decimal as D

from mmstack import main as m
from mmstack.paper_mm import PaperMarketMaker

GOOD = ["--paper-mm", "BTC/USD"]


def parser():
    p = argparse.ArgumentParser()
    m.add_paper_args(p)
    return p


class TestCommandLine(unittest.TestCase):
    def refused(self, extra, words):
        with self.assertRaises(SystemExit) as e:
            m.paper_config(parser().parse_args(GOOD + extra))
        self.assertIn(words, str(e.exception.code))

    def test_defaults_pass(self):
        self.assertIsNotNone(m.paper_config(parser().parse_args(GOOD)))

    def test_each_bad_value(self):
        cases = [
            (["--mm-max-inv", "0"], "--mm-max-inv must be 1 or more"),
            (["--mm-spread", "0"], "--mm-spread must be above 0"),
            (["--mm-spread", "-0.05"], "--mm-spread must be above 0"),
            (["--mm-spread", "abc"], "--mm-spread must be a number"),
            (["--mm-requote", "-1"], "--mm-requote must be 0 or more"),
            (["--mm-latency-ms", "-5"], "--mm-latency-ms must be 0 or more"),
            (["--mm-coinbase", "1.5"], "--mm-coinbase must be from 0 to 1"),
            (["--mm-alarm-bps", "-2"], "--mm-alarm-bps must be 0 or more"),
            (["--mm-alarm-window-ms", "0"], "--mm-alarm-window-ms must be 1 or more"),
            (["--mm-alarm-hold-ms", "-1"], "--mm-alarm-hold-ms must be 0 or more"),
            (["--mm-alarm-max-hold-ms", "-1"], "--mm-alarm-max-hold-ms must be 0 or more"),
            (["--mm-alarm-vol", "-1"], "--mm-alarm-vol must be 0 or more"),
            (["--mm-alarm-vol-tau-s", "0"], "--mm-alarm-vol-tau-s must be 1 or more"),
            (["--mm-imb-alarm", "1.2"], "--mm-imb-alarm must be from 0 to 1"),
            (["--mm-imb-hold-ms", "-1"], "--mm-imb-hold-ms must be 0 or more"),
            (["--mm-imb-levels", "0"], "--mm-imb-levels must be 1 or more"),
            (["--mm-imb-bps", "0"], "--mm-imb-bps must be above 0"),
        ]
        for extra, words in cases:
            with self.subTest(extra=extra):
                self.refused(extra, words)

    def test_all_problems_listed_at_once(self):
        with self.assertRaises(SystemExit) as e:
            m.paper_config(parser().parse_args(GOOD + ["--mm-max-inv", "0", "--mm-spread", "0"]))
        self.assertIn("--mm-max-inv", str(e.exception.code))
        self.assertIn("--mm-spread", str(e.exception.code))

    def test_edges_allowed(self):
        ok = ["--mm-max-inv", "1", "--mm-requote", "0", "--mm-latency-ms", "0",
              "--mm-coinbase", "1", "--mm-alarm-window-ms", "1", "--mm-imb-alarm", "1"]
        self.assertIsNotNone(m.paper_config(parser().parse_args(GOOD + ok)))


class TestBoothItself(unittest.TestCase):
    """A second guard in the booth, for anything that builds it without the
    command line."""

    def test_refuses(self):
        for kw, words in [({"max_inv_multiple": 0}, "max inventory"),
                          ({"spread": D(0)}, "spread"),
                          ({"alarm_window_ns": 0}, "alarm window"),
                          ({"alarm_vol_tau_ns": 0}, "volatility average"),
                          ({"imb_levels": 0}, "imbalance levels"),
                          ({"latency_ns": -1}, "latency")]:
            with self.subTest(kw=kw):
                with self.assertRaises(ValueError) as e:
                    PaperMarketMaker("TEST", **kw)
                self.assertIn(words, str(e.exception))


if __name__ == "__main__":
    unittest.main()
