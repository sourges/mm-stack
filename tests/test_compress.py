import contextlib
import gzip
import io
import json
import os
import tempfile
import time
import unittest
from unittest import mock

from mmstack import eventlog
from mmstack.eventlog import CompressError, EventTailer, compress_log
from mmstack.main import RECENT_SECONDS, logs_to_compress, main
from tests.helpers import sample_session
from tests.test_replay import replay_lines, write_log


class CompressTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "s.jsonl")
        write_log(self.path, sample_session())
        with open(self.path, "rb") as f:
            self.original = f.read()

    def tearDown(self):
        self.dir.cleanup()

    def files(self):
        return sorted(os.listdir(self.dir.name))


class TestCompressLog(CompressTestBase):
    def test_creates_identical_gz_and_keeps_original(self):
        before, after = compress_log(self.path)
        with gzip.open(self.path + ".gz", "rb") as f:
            self.assertEqual(f.read(), self.original)
        self.assertEqual(self.files(), ["s.jsonl", "s.jsonl.gz"])
        self.assertEqual(before, len(self.original))
        self.assertEqual(after, os.path.getsize(self.path + ".gz"))

    def test_delete_original(self):
        compress_log(self.path, delete_original=True)
        self.assertEqual(self.files(), ["s.jsonl.gz"])

    def test_refuses_to_overwrite_existing_gz(self):
        with open(self.path + ".gz", "wb") as f:
            f.write(b"keep me")
        with self.assertRaises(CompressError):
            compress_log(self.path, delete_original=True)
        with open(self.path + ".gz", "rb") as f:
            self.assertEqual(f.read(), b"keep me")
        self.assertIn("s.jsonl", self.files())

    def test_refuses_unfinished_file(self):
        with open(self.path, "ab") as f:
            f.write(b'{"seq": 99, "t":')  # half a line
        with self.assertRaises(CompressError):
            compress_log(self.path, delete_original=True)
        self.assertEqual(self.files(), ["s.jsonl"])  # no .gz, no .part left behind

    def test_refuses_empty_file(self):
        empty = os.path.join(self.dir.name, "e.jsonl")
        open(empty, "wb").close()
        with self.assertRaises(CompressError):
            compress_log(empty)

    def test_failed_verification_keeps_original(self):
        hashes = iter(["aaa", "bbb"])
        with mock.patch.object(eventlog, "_sha256_of", side_effect=lambda f: next(hashes)):
            with self.assertRaises(CompressError):
                compress_log(self.path, delete_original=True)
        self.assertEqual(self.files(), ["s.jsonl"])


class TestReadGz(CompressTestBase):
    def test_tailer_reads_gz_same_events(self):
        compress_log(self.path)
        plain = EventTailer(self.path)
        packed = EventTailer(self.path + ".gz")
        a, b = [], []
        for t, out in ((plain, a), (packed, b)):
            while True:
                batch = t.read_available()
                if not batch:
                    break
                out.extend(batch)
            t.close()
        self.assertEqual(a, b)
        self.assertEqual(len(a), len(sample_session()))

    def test_replay_gz_matches_plain(self):
        compress_log(self.path)
        self.assertEqual(replay_lines(self.path + ".gz"), replay_lines(self.path))


class TestCompressCommand(CompressTestBase):
    def test_folder_mode_skips_recent_and_non_logs(self):
        old = os.path.join(self.dir.name, "old.jsonl")
        write_log(old, sample_session())
        past = time.time() - RECENT_SECONDS - 10
        os.utime(old, (past, past))
        open(os.path.join(self.dir.name, "notes.txt"), "w").close()
        files, skipped = logs_to_compress([self.dir.name])
        self.assertEqual(files, [old])
        self.assertEqual(skipped, [self.path])  # just written = maybe still recording

    def test_command_compresses_and_deletes(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            main(["compress", self.path, "--delete"])
        self.assertEqual(self.files(), ["s.jsonl.gz"])
        self.assertIn("original deleted", buf.getvalue())

    def test_command_reports_failure_and_exit_code(self):
        with open(self.path + ".gz", "wb") as f:
            f.write(b"x")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), self.assertRaises(SystemExit) as cm:
            main(["compress", self.path, "--delete"])
        self.assertEqual(cm.exception.code, 1)
        self.assertIn("FAIL", buf.getvalue())
        self.assertIn("s.jsonl", self.files())


if __name__ == "__main__":
    unittest.main()
