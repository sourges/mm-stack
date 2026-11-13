import json
import os
import tempfile
import unittest

from mmstack.eventlog import EventTailer, EventWriter


class FakeClock:
    def __init__(self, start=1_000_000_000_000_000_000, step=1000):
        self.t = start
        self.step = step

    def __call__(self):
        self.t += self.step
        return self.t


class TestEventLog(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "log.jsonl")

    def tearDown(self):
        self.dir.cleanup()

    def test_writer_lines_have_seq_time_and_data(self):
        w = EventWriter(self.path, clock=FakeClock())
        w.write("kraken", "conn_open", {"host": "x"})
        w.write("kraken", "ws_in", {"text": "hi"})
        w.close()
        with open(self.path, "rb") as f:
            lines = f.read().split(b"\n")
        self.assertEqual(lines[-1], b"")  # file ends with a newline
        first, second = json.loads(lines[0]), json.loads(lines[1])
        self.assertEqual([first["seq"], second["seq"]], [1, 2])
        self.assertLess(first["t"], second["t"])
        self.assertEqual(second["data"], {"text": "hi"})
        self.assertNotIn(b"\r", b"".join(lines))  # no Windows line endings

    def test_writer_refuses_existing_file(self):
        EventWriter(self.path).close()
        with self.assertRaises(FileExistsError):
            EventWriter(self.path)

    def test_writer_flushes_each_event(self):
        w = EventWriter(self.path, clock=FakeClock())
        t = EventTailer(self.path)
        w.write("c", "a")
        self.assertEqual(len(t.read_available()), 1)  # visible before close()
        w.close()
        t.close()

    def test_tailer_waits_for_complete_line(self):
        line = json.dumps({"seq": 1, "t": 5, "conn": "c", "type": "x", "data": {}}).encode()
        with open(self.path, "wb") as f:
            t = EventTailer(self.path)
            f.write(line[:10])
            f.flush()
            self.assertEqual(t.read_available(), [])
            self.assertEqual(t.pending_partial(), 10)
            f.write(line[10:] + b"\n")
            f.flush()
            events = t.read_available()
        t.close()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["seq"], 1)

    def test_tailer_handles_utf8_split_across_reads(self):
        # 'é' is 2 bytes; split it between two writes.
        line = json.dumps({"seq": 1, "t": 5, "conn": "c", "type": "x",
                           "data": {"text": "café"}}, ensure_ascii=False).encode("utf-8")
        cut = line.index("é".encode("utf-8")) + 1
        with open(self.path, "wb") as f:
            t = EventTailer(self.path)
            f.write(line[:cut])
            f.flush()
            self.assertEqual(t.read_available(), [])
            f.write(line[cut:] + b"\n")
            f.flush()
            events = t.read_available()
        t.close()
        self.assertEqual(events[0]["data"]["text"], "café")

    def test_tailer_reads_many_lines_across_chunks(self):
        w = EventWriter(self.path, clock=FakeClock())
        for i in range(3000):
            w.write("c", "ws_in", {"text": "x" * 500})
        w.close()
        t = EventTailer(self.path)
        t.CHUNK = 4096  # force many small reads
        events, batches = [], 0
        while True:
            batch = t.read_available()
            if not batch:
                break
            batches += 1
            self.assertLessEqual(len(batch), 10)  # ~one 4 KB chunk per call, not the whole file
            events.extend(batch)
        t.close()
        self.assertEqual([e["seq"] for e in events], list(range(1, 3001)))
        self.assertGreater(batches, 100)

    def test_tailer_line_longer_than_chunk(self):
        w = EventWriter(self.path, clock=FakeClock())
        w.write("c", "ws_in", {"text": "x" * 50_000})  # e.g. a big book snapshot
        w.write("c", "ws_in", {"text": "y"})
        w.close()
        t = EventTailer(self.path)
        t.CHUNK = 1024
        first = t.read_available()
        rest = t.read_available()
        t.close()
        self.assertEqual(len(first[0]["data"]["text"]), 50_000)
        self.assertEqual([e["data"]["text"] for e in first + rest][-1], "y")


if __name__ == "__main__":
    unittest.main()
