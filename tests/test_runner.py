"""Experiment runner: experiment file, planning, saved replays, judging, the
whole run end to end (with real replay processes on small diaries)."""

import contextlib
import csv
import io
import json
import os
import tempfile
import time
import unittest

from mmstack import days, runner
from mmstack.eventlog import EventTailer, WindowTailer
from mmstack.main import Output, main
from tests.helpers import T0, event
from tests.test_replay import write_log
from tests.test_replay_window import market

NS = 1_000_000_000
HOUR = 3600 * NS

SATURDAY = """\
summary binance.trades=6707874
summary coinbase.trades=2068754
summary kraken.trades=263145
summary window from=2026-11-21T05:00:00.000000Z to=2026-11-22T05:00:00.000000Z paper_start=2026-11-21T05:00:00.005966Z last_event=2026-11-22T04:59:59.999506Z hours=24.00
summary paper settings spread=0.0500% max_inv=5x requote=0.1 latency=150ms fills=queue skew=on coinbase=0 rate_limit=intermediate
summary paper BTC/USD fills=950 (at_price=12 through=938 stale=59) buys=474 sells=476 volume=3334.70 inventory=-0.00012
summary paper BTC/USD pnl fee0.00%=-0.2649 fee0.10%=-3.5596 fee0.40%=-13.6037 breakeven_fee=-0.008%
summary paper BTC/USD pnl_split spread_earned=0.1903 inventory_drift=-0.4552
summary paper BTC/USD markout 1s=-1.02bps 5s=-1.04bps 30s=-1.25bps (n=950)
summary paper BTC/USD markout_1s_by_type at_price=-0.99bps through=-1.02bps stale=-1.99bps
summary paper BTC/USD requotes=24795 per_min=17.2 stale_pulls=2 quoted_time=100.0% queue_visible=4%
summary paper BTC/USD rate_limit_blocked starter=34.3% intermediate=0.0% pro=0.0% (order actions=50489)
summary paper BTC/USD rate_limit enforced=intermediate held_back=16035 cancel_blocked=0 peak=120/125
summary paper LINK/USD fills=444 (at_price=1 through=443 stale=16) buys=221 sells=223 volume=1306.51 inventory=-0.7
summary paper LINK/USD pnl fee0.00%=-0.2364 fee0.10%=-1.5430 fee0.40%=-5.4625 breakeven_fee=-0.018%
summary paper LINK/USD pnl_split spread_earned=0.0806 inventory_drift=-0.3170
summary paper LINK/USD markout 1s=-2.04bps 5s=-2.87bps 30s=-2.33bps (n=444)
summary paper LINK/USD markout_1s_by_type at_price=4.10bps through=-2.05bps stale=-4.28bps
summary paper LINK/USD requotes=26310 per_min=18.3 stale_pulls=8 quoted_time=100.0% queue_visible=36%
summary paper LINK/USD rate_limit_blocked starter=35.6% intermediate=0.0% pro=0.0% (order actions=53080)
summary paper LINK/USD rate_limit enforced=intermediate held_back=18599 cancel_blocked=0 peak=120/125
"""

GOOD = {"question": "q?", "base": ["--paper-mm"], "change": ["--mm-alarm-bps", "2"],
        "judge": {"number": "markout_1s", "better": "higher"}}


def write_exp(folder, exp, name="exp.json"):
    path = os.path.join(folder, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(exp, f)
    return path


def summary_text(sym="BTC/USD", fills=100, markout=-1.0, hours=24.0, extra=""):
    m = "n/a" if markout is None else f"{markout:.2f}bps"
    return (f"summary window from=- to=- paper_start=- last_event=- hours={hours:.2f}\n"
            f"summary paper {sym} fills={fills} (at_price=0 through={fills} stale=0) buys=1 sells=1 "
            f"volume=1 inventory=0\n"
            f"summary paper {sym} pnl fee0.00%=0.1 fee0.10%=0 fee0.40%=-1 breakeven_fee=0.001%\n"
            f"summary paper {sym} markout 1s={m} 5s={m} 30s={m} (n={fills})\n" + extra)


def a_day(i, kind="weekday", venues=("coinbase", "kraken"), gaps=None, usable=True, diary=None):
    start = T0 + i * 24 * HOUR
    return {"diary": diary or f"2026111{i}-000000.jsonl.gz", "piece": 1, "from_ns": start,
            "to_ns": start + 24 * HOUR, "last_ns": start + 24 * HOUR, "hours": 24.0, "kind": kind,
            "venues": list(venues), "drops": {}, "gaps": gaps or {},
            "flags": [], "usable": usable}


def row(diff, hiding=False):
    return {"diff": diff, "hiding": hiding}


class TestExperimentFile(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.dir.cleanup()

    def load(self, exp):
        return runner.load_experiment(write_exp(self.dir.name, exp, "my-test.json"))

    def refused(self, exp, words):
        with self.assertRaises(runner.ExperimentError) as e:
            self.load(exp)
        self.assertIn(words, str(e.exception))

    def test_defaults(self):
        exp = self.load(GOOD)
        self.assertEqual((exp["name"], exp["needs"], exp["tuned_on"]),
                         ("my-test", ["kraken", "coinbase"], []))

    def test_prediction_must_be_written_first(self):
        for key in ("question", "base", "change", "judge"):
            self.refused({k: v for k, v in GOOD.items() if k != key}, f'missing "{key}"')

    def test_change_must_change_something(self):
        self.refused(dict(GOOD, change=[]), "non-empty")

    def test_base_needs_the_paper_booth(self):
        self.refused(dict(GOOD, base=["--mm-spread", "0.1"]), "--paper-mm")

    def test_judge(self):
        self.refused(dict(GOOD, judge={"number": "vibes", "better": "higher"}), "one of")
        self.refused(dict(GOOD, judge={"number": "fills", "better": "more"}), '"higher" or "lower"')

    def test_needs_and_tuned_on(self):
        self.refused(dict(GOOD, needs=["ftx"]), "needs")
        self.refused(dict(GOOD, needs=[]), "needs")
        self.refused(dict(GOOD, tuned_on="20261114-203102"), "tuned_on")

    def test_not_json(self):
        path = os.path.join(self.dir.name, "bad.json")
        with open(path, "w") as f:
            f.write("{oops")
        with self.assertRaises(runner.ExperimentError) as e:
            runner.load_experiment(path)
        self.assertIn("not valid JSON", str(e.exception))

    def test_shipped_experiment_loads(self):
        exp = runner.load_experiment(os.path.join(runner.REPO_DIR, "experiments", "coinbase-alarm-link.json"))
        self.assertEqual(exp["tuned_on"], ["20261114-203102"])


class TestFingerprint(unittest.TestCase):
    def test_line_endings_dont_matter_but_code_does(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            for d, nl in ((a, b"\n"), (b, b"\r\n")):
                with open(os.path.join(d, "x.py"), "wb") as f:
                    f.write(b"x = 1" + nl + b"y = 2" + nl)
                with open(os.path.join(d, "notes.txt"), "wb") as f:
                    f.write(d.encode())  # not code: ignored
            self.assertEqual(runner.code_fingerprint(a), runner.code_fingerprint(b))
            with open(os.path.join(b, "x.py"), "ab") as f:
                f.write(b"z = 3\n")
            self.assertNotEqual(runner.code_fingerprint(a), runner.code_fingerprint(b))


class TestParseSummary(unittest.TestCase):
    def test_saturday(self):
        r = runner.parse_summary(SATURDAY)
        self.assertEqual(r["hours"], 24.0)
        self.assertEqual(r["problems"], [])
        btc = r["symbols"]["BTC/USD"]
        self.assertEqual((btc["fills"], btc["through"], btc["stale"]), (950, 938, 59))
        self.assertEqual((btc["markout_1s"], btc["markout_5s"], btc["markout_30s"]), (-1.02, -1.04, -1.25))
        self.assertEqual((btc["pnl_fee0"], btc["pnl_fee0.10"], btc["pnl_fee0.40"]), (-0.2649, -3.5596, -13.6037))
        self.assertEqual(btc["breakeven_fee"], -0.008)
        self.assertEqual((btc["spread_earned"], btc["inventory_drift"]), (0.1903, -0.4552))
        self.assertEqual((btc["quoted_time"], btc["requotes"], btc["held_back"], btc["peak"]),
                         (100.0, 24795, 16035, 120))
        self.assertEqual(r["symbols"]["LINK/USD"]["markout_1s"], -2.04)
        self.assertEqual(sorted(r["symbols"]), ["BTC/USD", "LINK/USD"])

    def test_no_fills_and_problem_lines(self):
        r = runner.parse_summary("kraken BTC/USD CHECKSUM MISMATCH - reconnecting\n"
                                 + summary_text(fills=0, markout=None))
        self.assertEqual(r["problems"], ["kraken BTC/USD CHECKSUM MISMATCH - reconnecting"])
        self.assertIsNone(r["symbols"]["BTC/USD"]["markout_1s"])
        self.assertEqual(r["symbols"]["BTC/USD"]["fills"], 0)

    def test_without_rate_limit(self):
        r = runner.parse_summary("summary paper LINK/USD rate_limit not modeled (order actions=5)\n"
                                 + summary_text("LINK/USD"))
        self.assertIsNone(r["symbols"]["LINK/USD"]["held_back"])


class TestPlanning(unittest.TestCase):
    def test_choose_days(self):
        found = [a_day(0), a_day(1, venues=("kraken",)), a_day(2, gaps={"coinbase": 9}),
                 a_day(3, gaps={"binance": 30}), a_day(4, usable=False),
                 a_day(5, diary="20261114-203102.jsonl.gz")]
        use, out = runner.choose_days(found, ["kraken", "coinbase"], ["20261114-203102"])
        self.assertEqual([d["diary"] for d in use], [found[0]["diary"], found[3]["diary"]])
        self.assertEqual([why for _, why in out],
                         ["no coinbase", "gap coinbase 9m", "used to tune this setting"])

    def test_tuned_on_with_or_without_extension(self):
        d = a_day(0, diary="20261114-203102.jsonl")
        for name in ("20261114-203102", "20261114-203102.jsonl", "20261114-203102.jsonl.gz"):
            use, _ = runner.choose_days([d], ["kraken"], [name])
            self.assertEqual(use, [], name)

    def test_plan(self):
        exp = dict(GOOD, needs=["kraken", "coinbase"])
        jobs = runner.plan(exp, [a_day(0)], os.path.join("E:", "mmdata"), "code1")
        base, change = jobs
        self.assertEqual(base["flags"], ["--paper-mm"])
        self.assertEqual(change["flags"], ["--paper-mm", "--mm-alarm-bps", "2"])
        self.assertEqual(change["args"][:2], ["replay", os.path.join("E:", "mmdata", a_day(0)["diary"])])
        self.assertEqual(change["args"][-5:], ["--to", runner.iso(a_day(0)["to_ns"]), "--summary-only",
                                               "--skip-conns", "binanceus,binance"])
        self.assertIn("--summary-only", change["args"])
        self.assertEqual(change["args"][change["args"].index("--skip-conns") + 1], "binanceus,binance")
        self.assertEqual(change["args"][change["args"].index("--from") + 1], runner.iso(a_day(0)["from_ns"]))
        self.assertNotEqual(base["key"], change["key"])

    def test_nothing_skipped_when_every_venue_is_needed(self):
        exp = dict(GOOD, needs=list(days.VENUES))
        jobs = runner.plan(exp, [a_day(0)], "d", "c")
        self.assertNotIn("--skip-conns", jobs[0]["args"])

    def test_key_survives_compress_but_not_a_code_change(self):
        a, b = a_day(0, diary="x.jsonl"), a_day(0, diary="x.jsonl.gz")
        self.assertEqual(runner.job_key(a, ["--paper-mm"], [], "c1"), runner.job_key(b, ["--paper-mm"], [], "c1"))
        self.assertNotEqual(runner.job_key(a, ["--paper-mm"], [], "c1"), runner.job_key(a, ["--paper-mm"], [], "c2"))
        self.assertNotEqual(runner.job_key(a, ["--paper-mm"], [], "c1"), runner.job_key(a, ["--paper-mm", "x"], [], "c1"))


class TestRunJobs(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.cache = os.path.join(self.dir.name, "cache")

    def tearDown(self):
        self.dir.cleanup()

    def jobs(self):
        return runner.plan(GOOD | {"needs": ["kraken"]}, [a_day(0), a_day(1)], "d", "c")

    def test_saved_results_are_reused(self):
        calls = []

        def fake(args):
            calls.append(args)
            return True, summary_text()
        runner.run_jobs(self.jobs(), self.cache, runner=fake, say=lambda *_: None)
        self.assertEqual(len(calls), 4)
        jobs = self.jobs()
        runner.run_jobs(jobs, self.cache, runner=fake, say=lambda *_: None)
        self.assertEqual(len(calls), 4)
        self.assertTrue(all(j["cached"] and j["output"] == summary_text() for j in jobs))

    def test_failures_are_not_saved(self):
        jobs = self.jobs()
        runner.run_jobs(jobs, self.cache, runner=lambda a: (False, "Traceback: boom"), say=lambda *_: None)
        self.assertTrue(all(j["error"] == "Traceback: boom" for j in jobs))
        self.assertEqual(os.listdir(self.cache), [])


class TestJudging(unittest.TestCase):
    def jobs(self, pairs, exp=GOOD):
        """pairs: list of (base_text, change_text) per day."""
        jobs = []
        for i, (b, c) in enumerate(pairs):
            d = a_day(i)
            jobs.append({"day": d, "side": "base", "output": b})
            jobs.append({"day": d, "side": "change", "output": c})
        return jobs

    def test_diff_sign_follows_better(self):
        pairs = [(summary_text(markout=-1.0), summary_text(markout=-0.4))]
        rows, _ = runner.compare(GOOD, self.jobs(pairs))
        self.assertAlmostEqual(rows[0]["diff"], 0.6)
        lower = dict(GOOD, judge={"number": "markout_1s", "better": "lower"})
        rows, _ = runner.compare(lower, self.jobs(pairs))
        self.assertAlmostEqual(rows[0]["diff"], -0.6)

    def test_hiding(self):
        rows, _ = runner.compare(GOOD, self.jobs([(summary_text(fills=1031), summary_text(fills=25, markout=3.0)),
                                                  (summary_text(fills=100), summary_text(fills=50))]))
        self.assertEqual([r["hiding"] for r in rows], [True, False])

    def test_left_out(self):
        bad = summary_text(extra="ACTION MISMATCH: log has x, state machine produced y\n")
        pairs = [(summary_text(), bad), (summary_text(), summary_text(markout=None))]
        jobs = self.jobs(pairs)
        jobs.append({"day": a_day(9), "side": "base", "output": summary_text()})
        jobs.append({"day": a_day(9), "side": "change", "error": "boom"})
        rows, out = runner.compare(GOOD, jobs)
        self.assertEqual(rows, [])
        self.assertEqual([why for _, _, why in out],
                         ["ACTION MISMATCH (replay doesn't match the log)", "no markout_1s (no fills?)",
                          "replay failed"])

    def test_other_problems_are_counted_not_dropped(self):
        rows, _ = runner.compare(GOOD, self.jobs([(summary_text(extra="coinbase SEQ GAP 1 -> 5 - reconnecting\n"),
                                                   summary_text())]))
        self.assertEqual(rows[0]["problems"], 1)

    def test_symbols_filter(self):
        both = summary_text("BTC/USD") + summary_text("LINK/USD")
        rows, _ = runner.compare(dict(GOOD, symbols=["LINK/USD"]), self.jobs([(both, both)]))
        self.assertEqual([r["symbol"] for r in rows], ["LINK/USD"])

    def test_verdicts(self):
        V = runner.verdict
        self.assertEqual(V([])[0], "NOT ENOUGH EVIDENCE")
        word, why = V([row(1.0)] * 6)
        self.assertEqual(word, "NOT ENOUGH EVIDENCE")
        self.assertIn("6 days (need 7)", why)
        self.assertEqual(V([row(1.0), row(1.1), row(0.9), row(1.2), row(0.8), row(1.0), row(-0.1)])[0], "WORKS")
        self.assertEqual(V([row(-1.0), row(-1.1), row(-0.9), row(-1.2), row(-0.8), row(-1.0), row(0.1)])[0],
                         "DOESN'T WORK")
        word, why = V([row(1.0)] * 5 + [row(-1.0)] * 2)  # 5 of 7
        self.assertEqual(word, "NOT ENOUGH EVIDENCE")
        self.assertIn("better on 5 of 7", why)
        word, why = V([row(0.01)] * 6 + [row(-3.0)])  # 6 of 7 better, but one big loss
        self.assertEqual(word, "NOT ENOUGH EVIDENCE")
        self.assertTrue(why.startswith("too noisy"))
        word, why = V([row(-0.01)] * 6 + [row(3.0)])  # 6 of 7 worse, but one big gain
        self.assertEqual(word, "NOT ENOUGH EVIDENCE")
        self.assertTrue(why.startswith("too noisy"))

    def test_hiding_counts_as_worse(self):
        six = [row(1.0), row(1.1), row(0.9), row(1.2), row(0.8), row(1.0)]
        word, why = runner.verdict(six + [row(5.0, hiding=True)])
        self.assertEqual(word, "WORKS")  # still 6 of 7 ...
        self.assertIn("better on 6 of 7, worse on 1; avg +0.857", why)  # ... but its +5 isn't counted
        word, why = runner.verdict(six[:5] + [row(5.0, hiding=True), row(5.0, hiding=True)])
        self.assertEqual(word, "NOT ENOUGH EVIDENCE")
        self.assertIn("better on 5 of 7, worse on 2", why)
        self.assertIn("2 day(s) hiding", why)

    def test_more_days_keep_the_same_share(self):
        self.assertEqual(runner.verdict([row(1.0)] * 12 + [row(0.8)] * 2)[0], "WORKS")  # 14 of 14
        self.assertEqual(runner.verdict([row(1.0)] * 12 + [row(-0.1)] * 2)[0], "WORKS")  # 12 of 14 = 6/7
        self.assertEqual(runner.verdict([row(1.0)] * 11 + [row(-0.1)] * 3)[0], "NOT ENOUGH EVIDENCE")


class TestReplayHelpers(unittest.TestCase):
    def test_skip_conns_and_window(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            evs = []
            for i in range(9):
                evs.append(event(len(evs) + 1, ["binance", "binance", "kraken"][i % 3], "timer", t=T0 + i * NS))
            write_log(path, evs)
            w = WindowTailer(EventTailer(path), until_ns=T0 + 7 * NS, skip_conns=["binance"])
            got = []
            while True:
                batch = w.read_available()
                if not batch:
                    break
                got.extend(batch)
            w.close()
        self.assertEqual([(e["conn"], (e["t"] - T0) // NS) for e in got], [("kraken", 2), ("kraken", 5)])

    def test_all_skipped_batch_is_not_the_end(self):
        class Batches:
            def __init__(self):
                self.b = [[{"conn": "binance", "t": 1}], [{"conn": "kraken", "t": 2}], []]

            def read_available(self):
                return self.b.pop(0)
        w = WindowTailer(Batches(), skip_conns=["binance"])
        self.assertEqual(w.read_available(), [{"conn": "kraken", "t": 2}])
        self.assertEqual(w.read_available(), [])

    def test_summary_only_output(self):
        buf = io.StringIO()
        out = Output(stream=buf, summary_only=True)
        for line in ["2026 kraken BTC/USD bid=1 ask=2", "PAPER FILL x", "coinbase SEQ GAP 1 -> 3",
                     "summary paper BTC/USD fills=1", "connected kraken"]:
            out.emit(line)
        self.assertEqual(buf.getvalue().splitlines(), ["coinbase SEQ GAP 1 -> 3", "summary paper BTC/USD fills=1"])
        self.assertEqual(out.lines, 5)


def write_day_diary(path, hours=17, start=T0):
    """A Kraken-only diary of `hours` hours (fills every minute, see market())."""
    evs = market(int(hours * 60))
    shift = start - T0
    for ev in evs:
        ev["t"] += shift
    write_log(path, evs)
    old = time.time() - 3600
    os.utime(path, (old, old))


class TestEndToEnd(unittest.TestCase):
    """Real replay processes on two small 17-hour diaries."""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.data = os.path.join(self.dir.name, "data")
        self.results = os.path.join(self.dir.name, "results")
        os.mkdir(self.data)
        write_day_diary(os.path.join(self.data, "20251112-000000.jsonl"), start=T0)
        write_day_diary(os.path.join(self.data, "20251113-000000.jsonl"), start=T0 + 24 * HOUR)
        self.exp = write_exp(self.dir.name, {
            "question": "Does a 1% spread keep the markout?",
            "base": ["--paper-mm", "BTC/USD", "--mm-fills", "through", "--mm-latency-ms", "100"],
            "change": ["--mm-spread", "1"],
            "judge": {"number": "fills", "better": "higher"},
            "needs": ["kraken"]}, "spread-1.json")

    def tearDown(self):
        self.dir.cleanup()

    def run_it(self, *extra):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            main(["run", self.exp, "--data", self.data, "--results", self.results, *extra])
        return buf.getvalue()

    def test_whole_run(self):
        text = self.run_it()
        self.assertIn("spread-1: 2 days to use (2 weekday, 0 weekend), 0 left out", text)
        self.assertIn("4 replays: 0 saved from earlier runs, 4 to run", text)
        self.assertIn("BTC/USD   weekday  NOT ENOUGH EVIDENCE: 2 days (need 7)", text)
        with open(os.path.join(self.results, "spread-1", "report.txt"), encoding="utf-8") as f:
            self.assertIn("judged by:  fills, higher is better", f.read())
        with open(os.path.join(self.results, "results.csv"), encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 4)  # 2 days x base/change
        self.assertEqual({r["side"] for r in rows}, {"base", "change"})
        self.assertEqual({r["fills"] for r in rows}, {"1020.0"})  # one fill a minute for 17 h
        self.assertEqual({r["hours"] for r in rows}, {"16.99"})  # last fill at 16 h 59.5 min

        again = self.run_it()
        self.assertIn("4 replays: 4 saved from earlier runs, 0 to run", again)

    def test_newest_and_dry_run(self):
        text = self.run_it("--newest", "1", "--dry-run")
        self.assertIn("1 days to use", text)
        lines = [x for x in text.splitlines() if x.startswith("to run:")]
        self.assertEqual(len(lines), 2)
        self.assertIn("20251113-000000.jsonl", lines[0])
        self.assertIn("--summary-only --skip-conns coinbase,binanceus,binance", lines[0])
        self.assertFalse(os.path.exists(os.path.join(self.results, "results.csv")))

    def test_bad_experiment_explained(self):
        bad = write_exp(self.dir.name, {"question": "q"}, "bad.json")
        with self.assertRaises(SystemExit) as e:
            main(["run", bad, "--data", self.data, "--results", self.results])
        self.assertIn("missing", str(e.exception.code))


if __name__ == "__main__":
    unittest.main()
