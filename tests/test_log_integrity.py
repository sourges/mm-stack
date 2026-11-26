"""Our own line numbers (seq) are checked on replay, and a cut-off last line
is reported, never silently dropped (2026-11-26 review). A missing line in
the middle leaves the day out of experiments."""

import contextlib
import io
import os
import tempfile
import unittest

from mmstack import main as m
from mmstack import runner
from mmstack.eventlog import EventTailer
from tests.helpers import sample_session
from tests.test_replay import write_log


def drain(tailer):
    evs = []
    while True:
        batch = tailer.read_available()
        if not batch:
            return evs
        evs += batch


class TestTailerSeq(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "s.jsonl")

    def tearDown(self):
        self.dir.cleanup()

    def write(self, seqs, conns=None, tail=b""):
        with open(self.path, "wb") as f:
            for i, s in enumerate(seqs):
                c = (conns or ["kraken"] * len(seqs))[i]
                f.write(b'{"seq":%d,"t":%d,"conn":"%s","type":"x","data":{}}\n' % (s, 1000 + i, c.encode()))
            f.write(tail)

    def test_complete_log_has_no_gaps(self):
        self.write([1, 2, 3, 4])
        t = EventTailer(self.path)
        self.assertEqual(len(drain(t)), 4)
        self.assertEqual(t.seq_gaps, [])

    def test_missing_lines_found(self):
        self.write([1, 2, 5, 6])  # the reviewer's example: 3 and 4 missing
        t = EventTailer(self.path)
        drain(t)
        self.assertEqual(t.seq_gaps, [(2, 5)])

    def test_backwards_counts_too(self):
        self.write([1, 2, 3, 2])
        t = EventTailer(self.path)
        drain(t)
        self.assertEqual(t.seq_gaps, [(3, 2)])

    def test_skipped_lines_are_not_gaps(self):
        """--skip-conns drops lines before decoding: their numbers must still
        count, or every skipped Binance line would look missing."""
        self.write([1, 2, 3, 4, 5], conns=["kraken", "binance", "binance", "kraken", "binance"])
        t = EventTailer(self.path, skip_conns=["binance"])
        self.assertEqual([e["seq"] for e in drain(t)], [1, 4])
        self.assertEqual(t.seq_gaps, [])

    def test_gap_across_skipped_lines_still_found(self):
        self.write([1, 2, 7], conns=["kraken", "binance", "kraken"])
        t = EventTailer(self.path, skip_conns=["binance"])
        drain(t)
        self.assertEqual(t.seq_gaps, [(2, 7)])

    def test_cut_off_last_line(self):
        self.write([1, 2], tail=b'{"seq":3,"t":9,"conn":"kra')
        t = EventTailer(self.path)
        self.assertEqual(len(drain(t)), 2)
        self.assertGreater(t.pending_partial(), 0)
        self.assertTrue(t.at_end)


class TestReplayReports(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "s.jsonl")

    def tearDown(self):
        self.dir.cleanup()

    def replay(self, events, tail=b"", extra=()):
        write_log(self.path, events)
        if tail:
            with open(self.path, "ab") as f:
                f.write(tail)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            m.main(["replay", self.path, "--summary-only", *extra])
        return buf.getvalue()

    def test_clean_log(self):
        out = self.replay(sample_session())
        self.assertIn("summary data log_gaps=0 missing_lines=0 cut_off_last_line=no", out)
        self.assertNotIn("LOG GAP", out)

    def test_gap_reported_as_a_problem(self):
        evs = sample_session()
        del evs[5:8]  # three lines lost
        out = self.replay(evs)
        self.assertIn(f"LOG GAP seq {evs[4]['seq']} -> {evs[5]['seq']} (3 lines missing)", out)
        self.assertIn("summary data log_gaps=1 missing_lines=3 cut_off_last_line=no", out)

    def test_cut_off_last_line_reported(self):
        out = self.replay(sample_session(), tail=b'{"seq":999,"t":1,"co')
        self.assertIn("cut_off_last_line=yes", out)

    def test_runner_leaves_a_gap_day_out(self):
        exp = {"judge": {"number": "pnl_fee0", "better": "higher"}}
        day = {"diary": "d.jsonl", "piece": 1}
        text = "LOG GAP seq 4 -> 8 (3 lines missing)\nsummary paper BTC/USD pnl fee0.00%=0.1\n"
        jobs = [{"day": day, "side": s, "output": text} for s in ("base", "change")]
        rows, left_out = runner.compare(exp, jobs)
        self.assertEqual(rows, [])
        self.assertIn("LOG GAP", left_out[0][2])


if __name__ == "__main__":
    unittest.main()
