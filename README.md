# mm-stack

A small market-making trading stack, built step by step from the "minimal HFT
stack" guide. **Steps 1–3 are done:**

1. Record everything that happens on the network (Kraken + Coinbase public
   feeds) to an append-only log file, with local timestamps.
2. A state machine that follows the log file, keeps the order books and prints
   the timestamp + best bid/ask + mid whenever the top of a book changes.
3. A replay mode that runs the same state machine on an old log with no
   network. Its output must match the live output exactly.

No API keys and no orders yet. That starts at step 4.

## Setup (Windows)

```
py -3.14 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python -m unittest discover -s tests -t .
```

The TLS network test needs the `openssl` command and is skipped if it's not
on your PATH. That's fine; the live run tests TLS for real.

## Run

Record live (BTC and LINK on both exchanges by default). Stop with Ctrl+C:

```
python -m mmstack.main live --out out_live.txt
```

It prints where the log went, e.g. `data\20261112-170500.jsonl`. Then replay it:

```
python -m mmstack.main replay data\20261112-170500.jsonl --out out_replay.txt
fc out_live.txt out_replay.txt
```

`fc` must say **no differences encountered**. That's the step 3 check.

Logs on another drive: `--data-dir E:\mmdata`.

## Compress finished logs

Raw logs run about 690 MB an hour (BTC books on both exchanges). Gzip makes
them about 13x smaller, and `replay` reads `.jsonl.gz` directly:

```
python -m mmstack.main compress E:\mmdata --delete
python -m mmstack.main replay E:\mmdata\20261113-111358.jsonl.gz
```

- A folder means every `*.jsonl` in it, **skipping any file changed in the
  last 2 minutes** (probably still recording).
- Each `.gz` is decompressed and checked against the original (SHA-256)
  before `--delete` removes the original. Without `--delete` both are kept.
- It never overwrites an existing `.gz`, and refuses a file whose last line
  is unfinished.

Options: `--kraken "BTC/USD,ETH/USD"`, `--coinbase "BTC-USD"`, and `--kraken ""`
to turn a venue off.

## What the output means

```
2026-11-12T22:05:01.123456Z kraken BTC/USD bid=90001.2 ask=90001.3 mid=90001.25
2026-11-12T22:05:09.000001Z kraken BTC/USD CHECKSUM MISMATCH - book invalid until next snapshot
2026-11-12T22:05:12.000002Z coinbase SEQ GAP 812 -> 815 - books invalid until resubscribe
summary kraken.checksum_ok=48211
```

- **Kraken checksum:** Kraken sends a CRC32 of its top 10 levels with every
  update. We compute our own and compare, so `checksum_bad=0` means our book
  matched Kraken's exactly the whole time.
- **Coinbase sequence numbers:** every message on a connection is numbered.
  A gap means a message was lost, so the book can't be trusted.
- At this stage a bad book just stays invalid until the next reconnect.
  Resubscribing needs the state machine to *act*, which is step 4.

## Layout

| File | Job |
|---|---|
| `mmstack/net.py` | All connections on one thread: sockets + `selectors`, TLS via `ssl.MemoryBIO`, WebSocket via `wsproto` (both "sans-io"). Logs before sending. Reconnects; connect and idle timeouts. |
| `mmstack/eventlog.py` | `EventWriter` (append + flush every event), `EventTailer` (follow a growing file or read a `.gz`, in 1 MB batches, never half a line), `compress_log`. |
| `mmstack/books.py` | Order book in exact `Decimal`s; Kraken checksum. |
| `mmstack/state.py` | The state machine. Time comes only from events. |
| `mmstack/venues.py` | Exchange URLs and subscribe messages. |
| `mmstack/main.py` | `live` and `replay`. |

## Log format

One JSON object per line:
`{"seq": 1, "t": <ns>, "conn": "kraken", "type": "ws_in", "data": {"text": "<raw message>"}}`

Types: `conn_open`, `tcp_connected`, `tls_ok`, `http_out`, `http_in`, `ws_open`,
`ws_out`, `ws_in`, `ws_ping_in`, `ws_pong_out`, `ws_close_in`, `ws_close_out`,
`conn_close`. Logs go in `data/` by default (git-ignored).
