"""Event log: one JSON object per line, appended and flushed straight away.

Every line looks like:
    {"seq": 12, "t": 1763000000123456789, "conn": "kraken", "type": "ws_in", "data": {...}}

seq  - our own counter, +1 per event (lets us spot a missing line)
t    - local wall-clock time in nanoseconds when the event was logged
conn - which connection it belongs to ("kraken", "coinbase", ...)
type - what happened (conn_open, tcp_connected, ws_in, ws_out, conn_close, ...)
data - details for that type

Only the network thread writes. Anything else (the state machine thread,
replay) reads the file with EventTailer.
"""

import gzip
import hashlib
import json
import os
import shutil
import threading
import time


class EventWriter:
    """Appends events to a NEW log file. One log file = one session."""

    def __init__(self, path, clock=time.time_ns):
        # "xb" = create, fail if the file already exists, so we never mix two
        # sessions (and two seq counters) in one file.
        self._f = open(path, "xb")
        self._clock = clock
        self._seq = 0
        self._lock = threading.Lock()

    def write(self, conn, type_, data=None):
        """Log one event and flush it to disk. Returns the event dict."""
        with self._lock:
            self._seq += 1
            event = {
                "seq": self._seq,
                "t": self._clock(),
                "conn": conn,
                "type": type_,
                "data": data if data is not None else {},
            }
            line = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
            # Always "\n" (binary mode), so Windows and Linux files are identical.
            self._f.write(line.encode("utf-8") + b"\n")
            self._f.flush()
            return event

    def close(self):
        with self._lock:
            if not self._f.closed:
                self._f.close()


def open_log(path):
    """Open a log for reading: plain .jsonl or compressed .jsonl.gz."""
    if str(path).endswith(".gz"):
        return gzip.open(path, "rb")
    return open(path, "rb")


class EventTailer:
    """Reads events from a log file, including one that is still growing.

    read_available() returns the next batch of COMPLETE lines (about one
    1 MB chunk's worth, so a 16 GB log never has to fit in memory). It returns
    [] only when there is nothing complete left to read right now. A line the
    writer has only half written stays in the buffer until its newline
    arrives, so we never parse half an event.

    Works on .jsonl.gz too (replay only; a compressed log never grows).
    """

    CHUNK = 1 << 20  # read 1 MB at a time

    def __init__(self, path):
        self._f = open_log(path)
        self._buf = b""

    def read_available(self):
        events = []
        while not events:
            chunk = self._f.read(self.CHUNK)
            if not chunk:
                break
            parts = (self._buf + chunk).split(b"\n")
            self._buf = parts.pop()  # last piece has no newline yet (may be b"")
            for line in parts:
                if line.strip():
                    events.append(json.loads(line))
        return events

    def pending_partial(self):
        """Bytes of an unfinished last line (normally empty)."""
        return len(self._buf)

    def close(self):
        self._f.close()


class CompressError(Exception):
    pass


def _sha256_of(f):
    h = hashlib.sha256()
    for block in iter(lambda: f.read(1 << 20), b""):
        h.update(block)
    return h.hexdigest()


def compress_log(path, delete_original=False, level=6):
    """Gzip a FINISHED log to <path>.gz, check it, optionally delete the original.

    Safety checks:
      - refuses if <path>.gz already exists (never overwrite)
      - refuses if the file doesn't end with a newline (still being written,
        or the program crashed mid-line)
      - after writing, decompresses the .gz and compares a SHA-256 hash with
        the original; only then is the original deleted (if asked)
    Returns (original_bytes, compressed_bytes).
    """
    gz_path = str(path) + ".gz"
    if os.path.exists(gz_path):
        raise CompressError(f"{gz_path} already exists")
    size = os.path.getsize(path)
    if size == 0:
        raise CompressError(f"{path} is empty")
    with open(path, "rb") as f:
        f.seek(-1, os.SEEK_END)
        if f.read(1) != b"\n":
            raise CompressError(f"{path} doesn't end with a newline (still being written?)")

    tmp_path = gz_path + ".part"
    try:
        with open(path, "rb") as src, gzip.open(tmp_path, "wb", compresslevel=level) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        with open(path, "rb") as f:
            original_hash = _sha256_of(f)
        with gzip.open(tmp_path, "rb") as f:
            packed_hash = _sha256_of(f)
        if original_hash != packed_hash:
            raise CompressError(f"verification failed for {path}; original kept")
        os.replace(tmp_path, gz_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    if delete_original:
        os.remove(path)
    return size, os.path.getsize(gz_path)
