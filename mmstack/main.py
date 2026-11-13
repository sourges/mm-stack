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

Actions (step 4): in live mode every action the state machine wants is written
to the log FIRST, then handed to the network thread. In replay the actions are
produced again but never sent; the state machine checks them against the
logged ones and prints ACTION MISMATCH if they differ.
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
from .kraken_auth import load_keys
from .kraken_private import KrakenPrivateClient
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


def consume(tailer, sm, out, follow, should_stop=lambda: False, poll=POLL_SECONDS,
            on_action=None):
    """Feed every complete event in the log to the state machine.

    follow=False: stop at the end of the file (replay).
    follow=True:  keep waiting for new lines until should_stop() is true.
    on_action(action) is called for every action the state machine produces
    (live: log it, then send it). In replay it is None: actions are dropped.
    We only check should_stop() between batches, never in the middle of an
    event, so a Ctrl+C can't leave the state machine half-updated.
    """
    while True:
        events = tailer.read_available()
        for ev in events:
            for line in sm.update(ev):
                out.emit(line)
            for action in sm.take_actions():
                if on_action is not None:
                    on_action(action)
        if not events:
            if not follow or should_stop():
                return
            time.sleep(poll)


def action_sink(writer, reactor):
    """Live: every action is logged FIRST, then handed to the network thread."""
    def log_then_send(action):
        writer.write("sm", "action", action)  # 1. log it
        reactor.submit(action)  # 2. then send it
    return log_then_send


def build_clients(writer, kraken_symbols, coinbase_products, keys=None,
                  private=False, trading_enabled=False):
    clients = []
    specs = []
    if kraken_symbols:
        specs.append(venues.kraken(kraken_symbols))
    if coinbase_products:
        specs.append(venues.coinbase(coinbase_products))
    for s in specs:
        clients.append(WsClient(s["name"], s["host"], s["port"], s["path"],
                                s["subscribe"], writer))
    if private:
        clients.append(KrakenPrivateClient(keys, writer, trading_enabled=trading_enabled))
    return clients


def run_live(args):
    os.makedirs(args.data_dir, exist_ok=True)
    log_path = args.log or os.path.join(
        args.data_dir, datetime.now().strftime("%Y%m%d-%H%M%S") + ".jsonl")
    private = args.private or args.enable_trading
    keys = load_keys() if private else None
    if private and keys is None:
        sys.exit("No Kraken API keys found. Set KRAKEN_API_KEY and KRAKEN_API_SECRET, "
                 "or create secrets\\kraken.json (see README).")
    if args.test_order and not args.enable_trading:
        print("note: --test-order without --enable-trading: the order is produced but "
              "the network layer will refuse to send it", file=sys.stderr)
    writer = EventWriter(log_path)
    # Settings that change decisions go in the log, so replay uses the same ones.
    writer.write("main", "session", {"test_order": args.test_order,
                                     "private": private, "trading_enabled": args.enable_trading})
    clients = build_clients(writer, _split(args.kraken), _split(args.coinbase), keys=keys,
                            private=private, trading_enabled=args.enable_trading)
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
        consume(tailer, sm, out, follow=True, on_action=action_sink(writer, reactor),
                should_stop=lambda: stop_requested.is_set() or not net_thread.is_alive())
    finally:
        net_stop.set()
        net_thread.join(timeout=10)
        writer.close()
        # Drain the last lines. Network is stopped, so any action now is not sent
        # (and not logged); replay will show it as actions.not_in_log too.
        consume(tailer, sm, out, follow=False)
        for line in sm.summary():
            out.emit(line)
        out.close()
        tailer.close()
        print(f"log saved: {log_path}", file=sys.stderr)


def run_replay(args):
    tailer = EventTailer(args.log)
    sm = StateMachine(test_order_symbol=args.test_order,
                      pretend_private_ready=args.pretend_private)
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
    live.add_argument("--private", action="store_true",
                      help="connect the Kraken private feed (your orders), no trading")
    live.add_argument("--enable-trading", action="store_true",
                      help="allow the network layer to send orders (implies --private)")
    live.add_argument("--test-order", metavar="SYMBOL",
                      help="step 5: place one tiny post-only buy 5%% under the bid, then cancel it")
    live.set_defaults(func=run_live)

    rep = sub.add_parser("replay", help="run the state machine over a recorded log")
    rep.add_argument("log")
    rep.add_argument("--out", help="also write output lines to this file")
    rep.add_argument("--test-order", metavar="SYMBOL",
                     help="dry run: turn on the test order for an old log")
    rep.add_argument("--pretend-private", action="store_true",
                     help="dry run: act as if the private feed were ready")
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
