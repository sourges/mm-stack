"""Step 3: live and replay must print exactly the same thing for the same log."""

import io
import json
import os
import tempfile
import threading
import time
import unittest

from mmstack.eventlog import EventTailer
from mmstack.main import Output, consume, main
from mmstack.state import StateMachine
from tests.helpers import sample_session


def write_log(path, events):
    with open(path, "wb") as f:
        for ev in events:
            f.write(json.dumps(ev, separators=(",", ":")).encode() + b"\n")


def replay_lines(path):
    buf = io.StringIO()
    tailer = EventTailer(path)
    sm = StateMachine()
    out = Output(stream=buf)
    consume(tailer, sm, out, follow=False)
    for line in sm.summary():
        out.emit(line)
    tailer.close()
    return buf.getvalue().splitlines()


class TestReplay(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "session.jsonl")

    def tearDown(self):
        self.dir.cleanup()

    def test_replay_twice_identical(self):
        write_log(self.path, sample_session())
        self.assertEqual(replay_lines(self.path), replay_lines(self.path))

    def test_live_follow_matches_replay(self):
        """Writer thread appends slowly (even half lines); the follower must
        produce exactly what a later replay produces."""
        events = sample_session()
        open(self.path, "wb").close()
        done = threading.Event()

        def writer():
            with open(self.path, "ab") as f:
                for ev in events:
                    line = json.dumps(ev, separators=(",", ":")).encode() + b"\n"
                    half = len(line) // 2
                    f.write(line[:half])
                    f.flush()
                    time.sleep(0.002)
                    f.write(line[half:])
                    f.flush()
            done.set()

        buf = io.StringIO()
        tailer = EventTailer(self.path)
        sm = StateMachine()
        out = Output(stream=buf)
        t = threading.Thread(target=writer)
        t.start()
        consume(tailer, sm, out, follow=True, should_stop=done.is_set, poll=0.001)
        t.join()
        consume(tailer, sm, out, follow=False)  # drain, like run_live does
        for line in sm.summary():
            out.emit(line)
        tailer.close()

        self.assertEqual(buf.getvalue().splitlines(), replay_lines(self.path))

    def test_replay_command_writes_out_file(self):
        write_log(self.path, sample_session())
        out_path = os.path.join(self.dir.name, "out.txt")
        with open(os.devnull, "w") as devnull:
            import contextlib
            with contextlib.redirect_stdout(devnull):
                main(["replay", self.path, "--out", out_path])
        with open(out_path, encoding="utf-8") as f:
            self.assertEqual(f.read().splitlines(), replay_lines(self.path))


if __name__ == "__main__":
    unittest.main()
