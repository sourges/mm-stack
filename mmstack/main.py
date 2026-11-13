"""Run the stack.

  live:   network thread records Kraken + Coinbase to a new log file, while this
          thread follows the file and feeds every event to the state machine.
  replay: no network; read an old log file (.jsonl or .jsonl.gz) and feed the
          same state machine.
  compress: gzip finished logs (about 12x smaller), checked before anything
          is deleted.

Both modes print the same lines for the same log. That is the step 3 test:
    python -m mmstack.main live   --out out_live.txt
    python -m mmstack.main replay data/<that log>.jsonl --out out_replay.txt
    then compare the two files (they must be identical).
"""

import argparse
import os
import signal
import sys
import threading
import time
from datetime import datetime

from . import venues
from .eventlog import CompressError, EventTailer, EventWriter, compress_log
from .net import Reactor, WsClient
from .state import StateMachine

POLL_SECONDS = 0.01  # how often the follower checks the file for new lines


class Output:
    """Prints lines and, if asked, also writes them to a file."""

    def __init__(self, path=None, stream=None):
        self._stream = stream or sys.stdout
        self._f = open(path, "w", encoding="utf-8", newline="\n") if path else None

    def emit(self, line):
        print(line, file=self._stream)
        if self._f:
            self._f.write(line + "\n")

    def close(self):
        if self._f:
            self._f.close()


def consume(tailer, sm, out, follow, should_stop=lambda: False, poll=POLL_SECONDS):
    """Feed every complete event in the log to the state machine.

    follow=False: stop at the end of the file (replay).
    follow=True:  keep waiting for new lines until should_stop() is true.
    We only check should_stop() between batches, never in the middle of an
    event, so a Ctrl+C can't leave the state machine half-updated.
    """
    while True:
        events = tailer.read_available()
        for ev in events:
            for line in sm.update(ev):
                out.emit(line)
        if not events:
            if not follow or should_stop():
                return
            time.sleep(poll)


def build_clients(writer, kraken_symbols, coinbase_products):
    clients = []
    specs = []
    if kraken_symbols:
        specs.append(venues.kraken(kraken_symbols))
    if coinbase_products:
        specs.append(venues.coinbase(coinbase_products))
    for s in specs:
        clients.append(WsClient(s["name"], s["host"], s["port"], s["path"],
                                s["subscribe"], writer))
    return clients


def run_live(args):
    os.makedirs(args.data_dir, exist_ok=True)
    log_path = args.log or os.path.join(
        args.data_dir, datetime.now().strftime("%Y%m%d-%H%M%S") + ".jsonl")
    writer = EventWriter(log_path)
    clients = build_clients(writer, _split(args.kraken), _split(args.coinbase))
    reactor = Reactor(clients)
    net_stop = threading.Event()

    def net_main():
        try:
            reactor.run(net_stop)
        except Exception as e:  # log it so the file explains what happened
            writer.write("net", "net_error", {"error": repr(e)})
            raise

    net_thread = threading.Thread(target=net_main, name="net", daemon=True)

    stop_requested = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_requested.set())

    out = Output(args.out)
    print(f"logging to {log_path}  (Ctrl+C to stop)", file=sys.stderr)
    net_thread.start()
    tailer = EventTailer(log_path)
    sm = StateMachine()
    try:
        consume(tailer, sm, out, follow=True,
                should_stop=lambda: stop_requested.is_set() or not net_thread.is_alive())
    finally:
        net_stop.set()
        net_thread.join(timeout=10)
        writer.close()
        consume(tailer, sm, out, follow=False)  # drain the last lines
        for line in sm.summary():
            out.emit(line)
        out.close()
        tailer.close()
        print(f"log saved: {log_path}", file=sys.stderr)


def run_replay(args):
    tailer = EventTailer(args.log)
    sm = StateMachine()
    out = Output(args.out)
    consume(tailer, sm, out, follow=False)
    for line in sm.summary():
        out.emit(line)
    out.close()
    tailer.close()


RECENT_SECONDS = 120  # in folder mode, skip logs changed this recently (probably live)


def logs_to_compress(paths, now=None):
    """Files given directly are taken as-is. For a folder, take its *.jsonl
    files, skipping any modified in the last RECENT_SECONDS (likely still
    being recorded). Returns (files, skipped)."""
    now = time.time() if now is None else now
    files, skipped = [], []
    for p in paths:
        if os.path.isdir(p):
            for name in sorted(os.listdir(p)):
                full = os.path.join(p, name)
                if not name.endswith(".jsonl") or not os.path.isfile(full):
                    continue
                if now - os.path.getmtime(full) < RECENT_SECONDS:
                    skipped.append(full)
                else:
                    files.append(full)
        else:
            files.append(p)
    return files, skipped


def run_compress(args):
    files, skipped = logs_to_compress(args.paths)
    for path in skipped:
        print(f"skip  {path} (changed in the last {RECENT_SECONDS}s - still recording?)")
    failed = 0
    for path in files:
        try:
            before, after = compress_log(path, delete_original=args.delete)
        except (CompressError, OSError) as e:
            print(f"FAIL  {path}: {e}")
            failed += 1
            continue
        note = "original deleted" if args.delete else "original kept"
        print(f"ok    {path} {before / 1e6:.1f} MB -> {after / 1e6:.1f} MB "
              f"({before / max(after, 1):.0f}x), {note}")
    if failed:
        sys.exit(1)


def _split(text):
    return [s.strip() for s in text.split(",") if s.strip()] if text else []


def main(argv=None):
    p = argparse.ArgumentParser(prog="mmstack")
    sub = p.add_subparsers(dest="mode", required=True)

    live = sub.add_parser("live", help="record live market data and run the state machine")
    live.add_argument("--kraken", default="BTC/USD,LINK/USD", help="Kraken symbols, comma separated ('' = off)")
    live.add_argument("--coinbase", default="BTC-USD,LINK-USD", help="Coinbase products, comma separated ('' = off)")
    live.add_argument("--data-dir", default="data")
    live.add_argument("--log", help="log file path (must not exist yet)")
    live.add_argument("--out", help="also write output lines to this file")
    live.set_defaults(func=run_live)

    rep = sub.add_parser("replay", help="run the state machine over a recorded log")
    rep.add_argument("log")
    rep.add_argument("--out", help="also write output lines to this file")
    rep.set_defaults(func=run_replay)

    comp = sub.add_parser("compress", help="gzip finished logs (file or folder)")
    comp.add_argument("paths", nargs="+", help="log files and/or folders of logs")
    comp.add_argument("--delete", action="store_true",
                      help="delete each original after its .gz is verified")
    comp.set_defaults(func=run_compress)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
