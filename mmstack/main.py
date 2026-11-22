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
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from . import venues
from .eventlog import CompressError, EventTailer, EventWriter, WindowTailer, compress_log
from .kraken_auth import load_keys
from .kraken_private import KrakenPrivateClient
from .net import Reactor, WsClient
from .state import StateMachine

POLL_SECONDS = 0.01  # how often the follower checks the file for new lines


# Lines worth seeing on screen in quiet mode. Everything else (mostly the
# price line printed on every top-of-book change) goes only to the --out file.
IMPORTANT = ("connected", "ACTION", "TEST ORDER", "order ", "MISMATCH", "GAP", "error",
             "rejected", "PAPER FILL", "pulled", "summary", "executions ready",
             "BAD BOOK")
STATUS_SECONDS = 60


def is_important(line):
    return any(tag in line for tag in IMPORTANT)


class Output:
    """Writes every line to the --out file. On screen it shows either every
    line (quiet=False) or only the important ones (quiet=True)."""

    def __init__(self, path=None, stream=None, quiet=False):
        self._stream = stream or sys.stdout
        self._f = open(path, "w", encoding="utf-8", newline="\n") if path else None
        self.quiet = quiet
        self.lines = 0

    def emit(self, line):
        self.lines += 1
        if not self.quiet or is_important(line):
            print(line, file=self._stream)
        if self._f:
            self._f.write(line + "\n")

    def close(self):
        if self._f:
            self._f.close()


def status_line(wall_ns, last_event_ns, events, log_bytes):
    """One line saying the recorder is alive and whether the strategy side is
    keeping up. `behind` is how old the newest processed event is."""
    clock = datetime.fromtimestamp(wall_ns / 1e9).strftime("%H:%M:%S")
    if last_event_ns is None:
        return f"[{clock}] waiting for data"
    behind = max(0.0, (wall_ns - last_event_ns) / 1e9)
    warn = "  <-- falling behind" if behind > 5 else ""
    return (f"[{clock}] {events:,} events, log {log_bytes / 1e6:,.0f} MB, "
            f"{behind:.1f}s behind{warn}")


def consume(tailer, sm, out, follow, should_stop=lambda: False, poll=POLL_SECONDS,
            on_action=None, on_batch=None):
    """Feed every complete event in the log to the state machine.

    follow=False: read to the end of the file (replay, or the final catch-up).
    follow=True:  keep waiting for new lines.
    should_stop() is checked after EVERY batch, including when we are behind
    the recorder, so Ctrl+C always works. It is never checked in the middle of
    an event, so the state machine can't be left half-updated.
    on_action(action) is called for every action the state machine produces
    (live: log it, then send it). In replay it is None: actions are dropped.
    on_batch(last_event, n_events) is called after each batch (status line).
    Returns True if it stopped because should_stop() said so.
    """
    while True:
        events = tailer.read_available()
        for ev in events:
            for line in sm.update(ev):
                out.emit(line)
            for action in sm.take_actions():
                if on_action is not None:
                    on_action(action)
        if on_batch is not None:
            on_batch(events[-1] if events else None, len(events))
        if should_stop():
            return True
        if not events:
            if not follow:
                return False
            time.sleep(poll)


def build_reactor(clients, writer):
    """Network loop for live mode, with the step 7 timer: a logged tick every second."""
    return Reactor(clients, timer_log=writer.write, timer_interval=1.0)


def action_sink(writer, reactor):
    """Live: every action is logged FIRST, then handed to the network thread."""
    def log_then_send(action):
        writer.write("sm", "action", action)  # 1. log it
        reactor.submit(action)  # 2. then send it
    return log_then_send


def build_clients(writer, kraken_symbols, coinbase_products, keys=None,
                  private=False, trading_enabled=False, binanceus_symbols=None,
                  binance_symbols=None):
    clients = []
    specs = []
    if kraken_symbols:
        specs.append(venues.kraken(kraken_symbols))
    if coinbase_products:
        specs.append(venues.coinbase(coinbase_products))
    if binanceus_symbols:
        specs.append(venues.binanceus(binanceus_symbols))
    if binance_symbols:
        specs.append(venues.binance(binance_symbols))
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
                                     "private": private, "trading_enabled": args.enable_trading,
                                     "paper_mm": paper_config(args)})
    clients = build_clients(writer, _split(args.kraken), _split(args.coinbase), keys=keys,
                            private=private, trading_enabled=args.enable_trading,
                            binanceus_symbols=_split(args.binanceus),
                            binance_symbols=_split(args.binance))
    reactor = build_reactor(clients, writer)
    net_stop = threading.Event()

    def net_main():
        try:
            reactor.run(net_stop)
        except Exception as e:  # log it so the file explains what happened
            writer.write("net", "net_error", {"error": repr(e)})
            raise

    net_thread = threading.Thread(target=net_main, name="net", daemon=True)

    presses = []  # one entry per Ctrl+C
    signal.signal(signal.SIGINT, lambda *_: presses.append(1))
    if hasattr(signal, "SIGBREAK"):  # Ctrl+Break on Windows
        signal.signal(signal.SIGBREAK, lambda *_: presses.append(1))

    out = Output(args.out, quiet=not args.verbose)
    print(f"logging to {log_path}  (Ctrl+C to stop)", file=sys.stderr)
    if out.quiet:
        print(f"quiet screen: a status line every {STATUS_SECONDS}s plus important events. "
              "Use --verbose to see every price line.", file=sys.stderr)
    net_thread.start()
    tailer = EventTailer(log_path)
    sm = StateMachine()
    progress = {"events": 0, "last_t": None, "next_status": time.time() + STATUS_SECONDS}

    def on_batch(last_event, n):
        progress["events"] += n
        if last_event is not None:
            progress["last_t"] = last_event["t"]
        if time.time() >= progress["next_status"]:
            progress["next_status"] = time.time() + STATUS_SECONDS
            print(status_line(time.time_ns(), progress["last_t"], progress["events"],
                              os.path.getsize(log_path)), file=sys.stderr)

    caught_up = True
    try:
        consume(tailer, sm, out, follow=True, on_action=action_sink(writer, reactor),
                on_batch=on_batch,
                should_stop=lambda: bool(presses) or not net_thread.is_alive())
    finally:
        net_stop.set()
        net_thread.join(timeout=10)
        writer.close()
        # Catch up on whatever the recorder wrote that we haven't processed yet.
        # Network is stopped, so any action now is not sent (and not logged);
        # replay will show it as actions.not_in_log too.
        first = len(presses)
        print("stopping: recorder stopped, finishing the unread part of the log "
              "(Ctrl+C again to skip)...", file=sys.stderr)
        skipped = consume(tailer, sm, out, follow=False, on_batch=on_batch,
                          should_stop=lambda: len(presses) > first)
        caught_up = not skipped
        for line in sm.summary():
            out.emit(line)
        if not caught_up:
            out.emit("summary INCOMPLETE: stopped before reading the whole log; "
                     "replay the log for the full result")
        out.close()
        tailer.close()
        print(f"log saved: {log_path}", file=sys.stderr)


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_when(text):
    """'2026-11-20T16:13:28Z' or '2026-11-20T11:13:28-05:00' -> ns since 1970.
    A time with no zone is refused: 'midnight' means different things in UTC
    and New York, and guessing wrong would silently shift every test day."""
    s = text.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise ValueError(f"not a date/time: {text!r} (use e.g. 2026-11-20T16:13:28Z)")
    if dt.tzinfo is None:
        raise ValueError(f"{text!r} has no time zone: add Z (UTC) or an offset like -05:00")
    return (dt - EPOCH) // timedelta(microseconds=1) * 1000


def replay_window(args):
    """(from_ns, to_ns) from --from/--to, either may be None. Exits on bad input."""
    try:
        start = parse_when(args.start) if args.start else None
        end = parse_when(args.end) if args.end else None
    except ValueError as e:
        sys.exit(f"replay: {e}")
    if start is not None and end is not None and start >= end:
        sys.exit("replay: --from must be before --to")
    return start, end


def run_replay(args):
    start, end = replay_window(args)
    tailer = WindowTailer(EventTailer(args.log), until_ns=end)
    sm = StateMachine(test_order_symbol=args.test_order,
                      pretend_private_ready=args.pretend_private,
                      paper_mm=paper_config(args),
                      paper_from_ns=start, paper_to_ns=end)
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


def paper_config(args):
    """Step 10 paper market maker settings from the command line (or None)."""
    if not getattr(args, "paper_mm", None):
        return None
    cfg = {"symbols": _split(args.paper_mm),
            "spread": str(Decimal(args.mm_spread) / 100),  # 0.05 (%) -> "0.0005"
            "max_inv": args.mm_max_inv,
            "requote": args.mm_requote,
            "latency_ms": args.mm_latency_ms,
            "fills": args.mm_fills,
            "skew": not args.mm_no_skew,
            "coinbase": args.mm_coinbase,
            "rate_tier": args.mm_rate_tier,
            "alarm_bps": args.mm_alarm_bps,
            "alarm_window_ms": args.mm_alarm_window_ms,
            "alarm_hold_ms": args.mm_alarm_hold_ms,
            "alarm_vol": args.mm_alarm_vol,
            "alarm_vol_tau_s": args.mm_alarm_vol_tau_s}
    if args.mm_venue != "kraken":
        cfg["venue"] = args.mm_venue  # only written when used, so older settings stay as they were
    if args.mm_ref != "coinbase":
        cfg["ref"] = args.mm_ref
    if Decimal(args.mm_imb_alarm):  # only written when used
        cfg.update({"imb_alarm": args.mm_imb_alarm, "imb_hold_ms": args.mm_imb_hold_ms,
                    "imb_reading": args.mm_imb_reading, "imb_levels": args.mm_imb_levels,
                    "imb_bps": args.mm_imb_bps})
    return cfg


def add_paper_args(p):
    p.add_argument("--paper-mm", nargs="?", const="BTC/USD,LINK/USD", metavar="SYMBOLS",
                   help="step 10: paper market maker (default symbols BTC/USD,LINK/USD)")
    p.add_argument("--mm-spread", default="0.05",
                   help="distance of each quote from mid, in %% (default 0.05)")
    p.add_argument("--mm-max-inv", type=int, default=5,
                   help="max inventory, in order sizes (default 5)")
    p.add_argument("--mm-requote", default="0.1",
                   help="re-quote only when the price moves this fraction of the distance (default 0.1)")
    p.add_argument("--mm-latency-ms", type=int, default=150,
                   help="simulated time for a new quote to go live, or an old one to be cancelled (default 150)")
    p.add_argument("--mm-fills", choices=("queue", "through"), default="queue",
                   help="fill rule: 'queue' = queue position at our price (default); "
                        "'through' = only trades strictly through our price (old rule)")
    p.add_argument("--mm-coinbase", default="0", metavar="WEIGHT",
                   help="lean quotes toward Coinbase's price: 0 = off (default), 0.65 = suggested")
    p.add_argument("--mm-alarm-bps", default="0", metavar="BPS",
                   help="Coinbase alarm: take one side off after Coinbase jumps this many bps "
                        "(0 = off, default; 2 = suggested)")
    p.add_argument("--mm-alarm-window-ms", type=int, default=100,
                   help="...within this many ms (default 100)")
    p.add_argument("--mm-alarm-hold-ms", type=int, default=1000,
                   help="...and keep that side off this long (default 1000)")
    p.add_argument("--mm-alarm-vol", default="0", metavar="K",
                   help="volatility alarm: ring when Coinbase moves K times its normal move "
                        "per window (0 = off, default). --mm-alarm-bps then sets a floor")
    p.add_argument("--mm-alarm-vol-tau-s", type=int, default=600,
                   help="...where 'normal' is averaged over about this many seconds (default 600)")
    p.add_argument("--mm-venue", choices=("kraken", "binanceus"), default="kraken",
                   help="whose book and trades to quote on (default kraken). binanceus: "
                        "Binance.US's top-20 book; Kraken's rate limit doesn't apply there")
    p.add_argument("--mm-imb-alarm", default="0", metavar="X",
                   help="imbalance alarm: when the book's lean reaches X (0..1) take the side it "
                        "leans against off (0 = off, default; try 0.6, 0.8, 0.9)")
    p.add_argument("--mm-imb-hold-ms", type=int, default=1000, help="...for this long (default 1000)")
    p.add_argument("--mm-imb-reading", choices=("weighted", "touch", "depth"), default="weighted",
                   help="...measured over the top levels (weighted, default), the best level only "
                        "(touch), or all size within --mm-imb-bps of mid (depth)")
    p.add_argument("--mm-imb-bps", default="5", help="...window for the depth reading (default 5)")
    p.add_argument("--mm-imb-levels", type=int, default=10, help="...levels in the weighted reading (default 10)")
    p.add_argument("--mm-ref", choices=("coinbase", "binance"), default="coinbase",
                   help="whose price the fair-price signal (--mm-coinbase) and the alarms "
                        "watch (default coinbase; binance = Binance's BTCUSDT for BTC/USD)")
    p.add_argument("--mm-rate-tier", choices=("none", "starter", "intermediate", "pro"),
                   default="intermediate",
                   help="obey Kraken's order rate limit for this account level "
                        "(default intermediate; none = only measure it)")
    p.add_argument("--mm-no-skew", action="store_true",
                   help="baseline: no inventory skew (the max-inventory cap stays on)")


def run_imbalance(args):
    from . import imbalance
    keys = [("kraken", s) for s in _split(args.kraken)] + [("binanceus", s) for s in _split(args.binanceus)]
    tailer = EventTailer(args.log)
    data = imbalance.collect(tailer, keys, levels=args.levels)
    tailer.close()
    out = Output(args.out)
    for key in keys:
        for line in imbalance.report(f"{key[0]} {key[1]}", data[key]):
            out.emit(line)
    out.close()


def run_hlwatch(args):
    from . import hlwatch
    address = hlwatch.check_address(args.address)
    os.makedirs(args.data_dir, exist_ok=True)
    path = os.path.join(args.data_dir, f"hl-{address[2:10]}-" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".jsonl")
    writer = EventWriter(path)
    writer.write("hlwatch", "session", {"address": address, "every_s": args.every,
                                         "fills_every_s": args.fills_every})
    print(f"watching {address}, logging to {path} (Ctrl+C to stop)")
    watcher = hlwatch.Watcher(address, writer, fills_every=max(1, round(args.fills_every / args.every)))
    try:
        hlwatch.run(watcher, args.every)
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()
        print(f"stopped after {watcher.polls} polls; log saved: {path}")


def run_hlreport(args):
    from . import hlwatch
    tailer = EventTailer(args.log)
    events = []
    while True:
        batch = tailer.read_available()
        if not batch:
            break
        events.extend(batch)
    tailer.close()
    out = Output(args.out)
    for line in hlwatch.report(events):
        out.emit(line)
    out.close()


def run_days(args):
    from . import days
    found, recording = days.find_days(
        args.folder, index_path=args.index,
        progress=lambda name: print(f"scanning {name} (once; saved to the index)...", flush=True))
    for line in days.report(found, recording):
        print(line)


def run_leadlag(args):
    from . import leadlag
    if args.books:
        books = []
        for item in _split(args.books):
            venue, _, symbol = item.partition(":")
            if not symbol:
                raise SystemExit(f"--books wants venue:symbol, e.g. binance:BTCUSDT (got {item!r})")
            books.append((venue, symbol))
        if len(books) != 2 or books[0] == books[1]:
            raise SystemExit("--books wants exactly two different books, e.g. binance:BTCUSDT,kraken:BTC/USD")
        first, second = books
    else:
        first, second = ("coinbase", args.coinbase), ("kraken", args.kraken)
    pairs = {f"{first[0]} {first[1]}": first, f"{second[0]} {second[1]}": second}
    tailer = EventTailer(args.log)
    series, _ = leadlag.sample_mids(tailer, pairs, args.step_ms * 1_000_000)
    tailer.close()
    out = Output(args.out)
    names = list(pairs)
    for line in leadlag.report(series, names[0], names[1], args.step_ms,
                               threshold_bps=float(args.jump_bps)):
        out.emit(line)
    out.close()


def _split(text):
    return [s.strip() for s in text.split(",") if s.strip()] if text else []


def main(argv=None):
    p = argparse.ArgumentParser(prog="mmstack")
    sub = p.add_subparsers(dest="mode", required=True)

    live = sub.add_parser("live", help="record live market data and run the state machine")
    live.add_argument("--kraken", default="BTC/USD,LINK/USD", help="Kraken symbols, comma separated ('' = off)")
    live.add_argument("--coinbase", default="BTC-USD,LINK-USD", help="Coinbase products, comma separated ('' = off)")
    live.add_argument("--binanceus", default="BTCUSD,LINKUSD",
                      help="Binance.US symbols, comma separated ('' = off)")
    live.add_argument("--binance", default="BTCUSDT,LINKUSDT",
                      help="Binance (global) market-data symbols, comma separated ('' = off)")
    live.add_argument("--data-dir", default="data")
    live.add_argument("--log", help="log file path (must not exist yet)")
    live.add_argument("--out", help="also write output lines to this file")
    live.add_argument("--verbose", action="store_true",
                      help="print every line on screen (default: status line + important events)")
    live.add_argument("--private", action="store_true",
                      help="connect the Kraken private feed (your orders), no trading")
    live.add_argument("--enable-trading", action="store_true",
                      help="allow the network layer to send orders (implies --private)")
    live.add_argument("--test-order", metavar="SYMBOL",
                      help="step 5: place one tiny post-only buy 5%% under the bid, then cancel it")
    add_paper_args(live)
    live.set_defaults(func=run_live)

    rep = sub.add_parser("replay", help="run the state machine over a recorded log")
    rep.add_argument("log")
    rep.add_argument("--out", help="also write output lines to this file")
    rep.add_argument("--from", dest="start", metavar="TIME",
                     help="paper market maker starts at this time; earlier events only build "
                          "the books (e.g. 2026-11-20T16:13:28Z or 2026-11-20T11:13:28-05:00)")
    rep.add_argument("--to", dest="end", metavar="TIME",
                     help="stop reading the log at this time (same format as --from)")
    rep.add_argument("--test-order", metavar="SYMBOL",
                     help="dry run: turn on the test order for an old log")
    rep.add_argument("--pretend-private", action="store_true",
                     help="dry run: act as if the private feed were ready")
    add_paper_args(rep)
    rep.set_defaults(func=run_replay)

    comp = sub.add_parser("compress", help="gzip finished logs (file or folder)")
    comp.add_argument("paths", nargs="+", help="log files and/or folders of logs")
    comp.add_argument("--delete", action="store_true",
                      help="delete each original after its .gz is verified")
    comp.set_defaults(func=run_compress)

    dy = sub.add_parser("days", help="list the test days in a folder of diaries, with gaps and flags")
    dy.add_argument("folder", help="folder of diaries, e.g. E:\\mmdata")
    dy.add_argument("--index", help="index file (default: days_index.json in the folder)")
    dy.set_defaults(func=run_days)

    ll = sub.add_parser("leadlag", help="research: does Coinbase's price move before Kraken's?")
    ll.add_argument("log")
    ll.add_argument("--kraken", default="BTC/USD")
    ll.add_argument("--coinbase", default="BTC-USD")
    ll.add_argument("--books", help="any two books instead, as venue:symbol, the one tested as "
                                    "leader first, e.g. binance:BTCUSDT,kraken:BTC/USD")
    ll.add_argument("--step-ms", type=int, default=50, help="sampling step (default 50)")
    ll.add_argument("--jump-bps", default="2", help="size of a 'jump' in the event study (default 2)")
    ll.add_argument("--out", help="also write the report to this file")
    ll.set_defaults(func=run_leadlag)

    hw = sub.add_parser("hlwatch", help="research: log one Hyperliquid account's public orders, positions and fills")
    hw.add_argument("address", help="the account, 0x + 40 hex digits")
    hw.add_argument("--data-dir", default=os.path.join("data", "hl"), help="where to write the log (default data\\hl)")
    hw.add_argument("--every", type=float, default=5.0, help="seconds between polls (default 5)")
    hw.add_argument("--fills-every", type=float, default=60.0, help="seconds between fill checks (default 60)")
    hw.set_defaults(func=run_hlwatch)

    hr = sub.add_parser("hlreport", help="research: summarize an hlwatch log")
    hr.add_argument("log")
    hr.add_argument("--out", help="also write the report to this file")
    hr.set_defaults(func=run_hlreport)

    im = sub.add_parser("imbalance", help="research: does the book's lean predict the next mid move?")
    im.add_argument("log")
    im.add_argument("--kraken", default="BTC/USD,LINK/USD", help="Kraken books to test ('' = none)")
    im.add_argument("--binanceus", default="BTCUSD,LINKUSD", help="Binance.US books to test ('' = none)")
    im.add_argument("--levels", type=int, default=10, help="levels per side in the weighted reading (default 10)")
    im.add_argument("--out", help="also write the report to this file")
    im.set_defaults(func=run_imbalance)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
