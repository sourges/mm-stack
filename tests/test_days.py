"""Test days: cutting diaries into 24 h pieces, gaps, weekday/weekend, the index."""

import contextlib
import gzip
import io
import json
import os
import shutil
import tempfile
import time
import unittest
from unittest import mock

from mmstack import days
from mmstack.eventlog import EventWriter
from mmstack.main import main, parse_when

NS = 1_000_000_000
MIN = 60 * NS
HOUR = 3600 * NS


def line(seq, t, conn, type_, data=None):
    return json.dumps({"seq": seq, "t": t, "conn": conn, "type": type_, "data": data or {}},
                      separators=(",", ":")).encode() + b"\n"


def make_diary(path, start, hours, venues=("kraken", "coinbase"), step=MIN,
               silent=None, drops=(), gz=False):
    """A diary from `start` (ns) for `hours`: every venue sends one ws_in per
    `step`, plus a timer tick from "main". silent = {venue: [(from_ns, to_ns)]}
    (absolute times) when that venue sends nothing. drops = [(venue, t_ns)]."""
    silent = silent or {}
    end = start + int(hours * HOUR)
    out, seq = [], 0
    events = []
    t = start
    while t <= end:
        events.append((t, "main", "timer"))
        for v in venues:
            if not any(a <= t < b for a, b in silent.get(v, [])):
                events.append((t, v, "ws_in"))
        t += step
    events.extend((t, v, "conn_close") for v, t in drops)
    events.sort(key=lambda e: e[0])
    for t, conn, type_ in events:
        seq += 1
        out.append(line(seq, t, conn, type_))
    opener = gzip.open if gz else open
    with opener(path, "wb") as f:
        f.writelines(out)
    old = time.time() - 3600
    os.utime(path, (old, old))  # finished an hour ago, not "still recording"


MON_1931 = parse_when("2026-11-16T19:31:11-05:00")
FRI_1613 = parse_when("2026-11-20T16:13:28-05:00")
FRI_NOON = parse_when("2026-11-20T12:00:00-05:00")


class TestLineStart(unittest.TestCase):
    def test_reads_what_the_recorder_writes(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "x.jsonl")
            w = EventWriter(path, clock=lambda: 1_763_000_000_123_456_789)
            w.write("coinbase", "ws_in", {"text": '{"channel":"l2_data","t":"quoted \\"stuff\\""}'})
            w.close()
            with open(path, "rb") as f:
                m = days.LINE_START.match(f.readline())
        self.assertEqual((int(m.group(1)), m.group(2), m.group(3)),
                         (1_763_000_000_123_456_789, b"coinbase", b"ws_in"))


class TestScan(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "20261116-193111.jsonl")

    def tearDown(self):
        self.dir.cleanup()

    def test_times_counts_and_pieces(self):
        make_diary(self.path, MON_1931, 30, drops=[("kraken", MON_1931 + 25 * HOUR)])
        s = days.scan(self.path)
        self.assertEqual((s["first_t"], s["last_t"]), (MON_1931, MON_1931 + 30 * HOUR))
        self.assertEqual(len(s["slices"]), 2)
        self.assertEqual(s["slices"][0]["data"], {"kraken": 1440, "coinbase": 1440})
        self.assertEqual(s["slices"][1]["drops"], {"kraken": 1})
        self.assertEqual(s["gaps"], {})

    def test_gap_in_the_middle_and_at_the_end(self):
        a, b = MON_1931 + 2 * HOUR, MON_1931 + 3 * HOUR
        make_diary(self.path, MON_1931, 10, silent={"coinbase": [(a, b), (MON_1931 + 9 * HOUR, MON_1931 + 99 * HOUR)]})
        s = days.scan(self.path)
        self.assertEqual(s["gaps"]["coinbase"], [[a - MIN, b], [MON_1931 + 9 * HOUR - MIN, MON_1931 + 10 * HOUR]])
        self.assertNotIn("kraken", s["gaps"])

    def test_gap_threshold(self):
        start = MON_1931 + HOUR
        make_diary(self.path, MON_1931, 3, step=NS * 10,
                   silent={"kraken": [(start, start + 5 * MIN - 10 * NS)],  # silence exactly 5 min
                           "coinbase": [(start, start + 5 * MIN - 20 * NS)]})  # 4 min 50 s
        s = days.scan(self.path)
        self.assertEqual(s["gaps"]["kraken"], [[start - 10 * NS, start + 5 * MIN - 10 * NS]])
        self.assertNotIn("coinbase", s["gaps"])

    def test_half_written_last_line_ignored(self):
        make_diary(self.path, MON_1931, 1)
        with open(self.path, "ab") as f:
            f.write(b'{"seq":999,"t":%d,"conn":"kraken","type":"ws_in","da' % (MON_1931 + 5 * HOUR))
        self.assertEqual(days.scan(self.path)["last_t"], MON_1931 + HOUR)

    def test_reads_gz(self):
        make_diary(self.path + ".gz", MON_1931, 2, gz=True)
        self.assertEqual(days.scan(self.path + ".gz")["last_t"], MON_1931 + 2 * HOUR)

    def test_non_venue_events_make_pieces_too(self):
        make_diary(self.path, MON_1931, 30, venues=())
        s = days.scan(self.path)
        self.assertEqual(len(s["slices"]), 2)
        self.assertEqual(s["slices"][1]["data"], {})


class TestPieces(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.tz = days.new_york()

    def tearDown(self):
        self.dir.cleanup()

    def pieces(self, name, start, hours, **kw):
        path = os.path.join(self.dir.name, name)
        make_diary(path, start, hours, **kw)
        return days.days_from_scan(name, days.scan(path), self.tz)

    def test_old_style_evening_diary_is_one_short_weekday(self):
        (d,) = self.pieces("a.jsonl", MON_1931, 19.5)
        self.assertEqual((d["piece"], d["hours"], d["kind"], d["usable"]), (1, 19.5, "weekday", True))
        self.assertEqual(d["flags"], ["short"])
        self.assertEqual((d["from_ns"], d["to_ns"], d["last_ns"]),
                         (MON_1931, MON_1931 + 24 * HOUR, MON_1931 + int(19.5 * HOUR)))

    def test_long_diary_cut_into_24h_pieces(self):
        p = self.pieces("b.jsonl", FRI_1613, 46)
        self.assertEqual([d["hours"] for d in p], [24.0, 22.0])
        self.assertEqual([d["kind"] for d in p], ["weekend", "weekend"])  # Fri 4 pm -> Sat 4 pm: 16 h weekend
        self.assertEqual(p[1]["from_ns"], FRI_1613 + 24 * HOUR)
        self.assertEqual([d["flags"] for d in p], [[], []])

    def test_minimum_hours(self):
        (d,) = self.pieces("c.jsonl", MON_1931, 15.9)
        self.assertEqual((d["usable"], d["flags"]), (False, ["too short"]))
        (d,) = self.pieces("d.jsonl", MON_1931, 16)
        self.assertEqual((d["usable"], d["flags"]), (True, ["short"]))
        (d,) = self.pieces("e.jsonl", MON_1931, 20)
        self.assertEqual(d["flags"], [])

    def test_half_weekend_counts_as_weekday(self):
        d = self.pieces("f.jsonl", FRI_NOON, 24)[0]  # Fri noon -> Sat noon: 12 h each
        self.assertEqual(d["kind"], "weekday")
        d = self.pieces("g.jsonl", FRI_NOON + MIN, 24)[0]  # one minute more of Saturday
        self.assertEqual(d["kind"], "weekend")

    def test_weekend_in_new_york_time_not_utc(self):
        # Sun 8 pm New York = Mon 01:00 UTC. Sun 20:00 -> Mon 20:00 NY is 4 h weekend.
        d = self.pieces("h.jsonl", parse_when("2026-11-22T20:00:00-05:00"), 24)[0]
        self.assertEqual(d["kind"], "weekday")
        self.assertAlmostEqual(days.weekend_hours(d["from_ns"], d["last_ns"], self.tz), 4.0)

    def test_gap_flag_goes_to_the_piece_it_falls_in(self):
        a = FRI_1613 + 30 * HOUR
        p = self.pieces("i.jsonl", FRI_1613, 46, silent={"coinbase": [(a, a + 20 * MIN)]})
        self.assertEqual(p[0]["flags"], [])
        self.assertEqual(p[1]["flags"], ["gap coinbase 21m"])  # silence from the last message before
        self.assertEqual(p[1]["gaps"], {"coinbase": 21})

    def test_gap_across_the_cut_is_flagged_in_both(self):
        a = FRI_1613 + 24 * HOUR - 10 * MIN
        p = self.pieces("j.jsonl", FRI_1613, 46, silent={"kraken": [(a, a + 20 * MIN)]})
        self.assertEqual(p[0]["gaps"], {"kraken": 11})
        self.assertEqual(p[1]["gaps"], {"kraken": 10})

    def test_a_venue_missing_from_a_piece(self):
        p = self.pieces("m.jsonl", FRI_1613, 46, venues=("kraken", "binance"),
                        silent={"binance": [(FRI_1613, FRI_1613 + 30 * HOUR)]})
        self.assertEqual((p[0]["venues"], p[1]["venues"]), (["kraken"], ["binance", "kraken"]))
        self.assertEqual(p[0]["gaps"], {"binance": 1440})  # never connected in piece 1

    def test_venues_and_drops_per_piece(self):
        p = self.pieces("k.jsonl", FRI_1613, 46, venues=("kraken", "binance"),
                        drops=[("binance", FRI_1613 + 30 * HOUR)])
        self.assertEqual(p[0]["venues"], ["binance", "kraken"])
        self.assertEqual((p[0]["drops"], p[1]["drops"]), ({}, {"binance": 1}))

    def test_last_event_exactly_24h_later_starts_a_second_piece(self):
        p = self.pieces("l.jsonl", MON_1931, 24)
        self.assertEqual([d["hours"] for d in p], [24.0, 0.0])
        self.assertEqual(p[1]["flags"], ["too short"])

    def test_empty_diary(self):
        path = os.path.join(self.dir.name, "empty.jsonl")
        open(path, "wb").close()
        self.assertEqual(days.days_from_scan("empty.jsonl", days.scan(path), self.tz), [])


class TestIndex(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.folder = self.dir.name
        self.a = os.path.join(self.folder, "20261116-193111.jsonl")
        make_diary(self.a, MON_1931, 20)

    def tearDown(self):
        self.dir.cleanup()

    def find(self, **kw):
        real = days.scan
        calls = []

        def counting(path, *a, **k):
            calls.append(os.path.basename(path))
            return real(path, *a, **k)
        with mock.patch.object(days, "scan", counting):
            found, recording = days.find_days(self.folder, **kw)
        return found, recording, calls

    def test_each_diary_scanned_once(self):
        _, _, calls = self.find()
        self.assertEqual(calls, ["20261116-193111.jsonl"])
        found, _, calls = self.find()
        self.assertEqual(calls, [])
        self.assertEqual(found[0]["hours"], 20.0)
        self.assertTrue(os.path.exists(os.path.join(self.folder, days.INDEX_NAME)))

    def test_compressed_diary_reuses_its_scan(self):
        self.find()
        with open(self.a, "rb") as src, gzip.open(self.a + ".gz", "wb") as dst:
            shutil.copyfileobj(src, dst)
        os.remove(self.a)
        old = time.time() - 3600
        os.utime(self.a + ".gz", (old, old))
        found, _, calls = self.find()
        self.assertEqual(calls, [])
        self.assertEqual(found[0]["diary"], "20261116-193111.jsonl.gz")
        index = days.load_index(os.path.join(self.folder, days.INDEX_NAME))
        self.assertEqual(index["20261116-193111.jsonl"]["file"], "20261116-193111.jsonl.gz")
        _, _, calls = self.find()  # still no rescan once the index says .gz
        self.assertEqual(calls, [])

    def test_changed_file_is_rescanned(self):
        self.find()
        make_diary(self.a, MON_1931, 22)
        found, _, calls = self.find()
        self.assertEqual(calls, ["20261116-193111.jsonl"])
        self.assertEqual(found[0]["hours"], 22.0)

    def test_old_index_version_is_rescanned(self):
        self.find()
        path = os.path.join(self.folder, days.INDEX_NAME)
        index = days.load_index(path)
        index["20261116-193111.jsonl"]["version"] = days.INDEX_VERSION - 1
        days.save_index(path, index)
        _, _, calls = self.find()
        self.assertEqual(calls, ["20261116-193111.jsonl"])

    def test_only_a_plain_scan_carries_over(self):
        index = {"x.jsonl": {"version": days.INDEX_VERSION, "file": "x.jsonl.gz", "size": 5, "scan": {}}}
        self.assertIsNone(days.cached_scan(index, "x.jsonl", 5))  # a .jsonl back next to a scanned .gz
        self.assertEqual(days.cached_scan(index, "x.jsonl.gz", 5), {})

    def test_recording_diary_skipped(self):
        b = os.path.join(self.folder, "20261122-141620.jsonl")
        make_diary(b, FRI_1613, 1)
        os.utime(b, None)  # just written
        found, recording, calls = self.find()
        self.assertEqual(recording, ["20261122-141620.jsonl"])
        self.assertNotIn("20261122-141620.jsonl", calls)
        self.assertEqual([d["diary"] for d in found], ["20261116-193111.jsonl"])

    def test_a_just_compressed_gz_is_not_recording(self):
        make_diary(os.path.join(self.folder, "20261117-194922.jsonl.gz"), MON_1931, 20, gz=True)
        os.utime(os.path.join(self.folder, "20261117-194922.jsonl.gz"), None)  # written a moment ago
        found, recording, _ = self.find()
        self.assertEqual(recording, [])
        self.assertIn("20261117-194922.jsonl.gz", [d["diary"] for d in found])

    def test_jsonl_next_to_its_gz_is_skipped(self):
        make_diary(self.a + ".gz", MON_1931, 20, gz=True)
        found, _, _ = self.find()
        self.assertEqual([d["diary"] for d in found], ["20261116-193111.jsonl.gz"])

    def test_other_files_ignored(self):
        for name in ("notes.txt", "days_index.json.tmp"):
            open(os.path.join(self.folder, name), "w").close()
        os.mkdir(os.path.join(self.folder, "hl"))
        found, _, _ = self.find()
        self.assertEqual(len(found), 1)

    def test_broken_index_is_rebuilt(self):
        with open(os.path.join(self.folder, days.INDEX_NAME), "w") as f:
            f.write("{not json")
        found, _, calls = self.find()
        self.assertEqual((len(found), calls), (1, ["20261116-193111.jsonl"]))


class TestCommand(unittest.TestCase):
    def test_prints_the_days(self):
        with tempfile.TemporaryDirectory() as d:
            make_diary(os.path.join(d, "20261116-193111.jsonl"), MON_1931, 19.5)
            make_diary(os.path.join(d, "20261120-161328.jsonl"), FRI_1613, 46,
                       venues=("kraken", "coinbase", "binance"),
                       silent={"binance": [(FRI_1613 + 30 * HOUR, FRI_1613 + 31 * HOUR)]})
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["days", d])
                first = buf.getvalue()
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["days", d])
        text = buf.getvalue()
        self.assertIn("scanning 20261120-161328.jsonl", first)
        self.assertNotIn("scanning", text)  # second time: from the index
        self.assertIn("20261116-193111.jsonl       1  Mon 11-16 19:31  Tue 11-17 15:01   19.5  weekday", text)
        self.assertIn("gap binance 61m", text)
        self.assertIn("usable days: 1 weekday, 2 weekend (1 of them with a gap flag)", text)

    def test_missing_time_zone_explained(self):
        from zoneinfo import ZoneInfoNotFoundError
        with mock.patch("zoneinfo.ZoneInfo", side_effect=ZoneInfoNotFoundError("x")):
            with self.assertRaises(SystemExit) as e:
                days.new_york()
        self.assertIn("tzdata", str(e.exception.code))


if __name__ == "__main__":
    unittest.main()
