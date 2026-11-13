# mm-stack

A small market-making trading stack, built step by step from the "minimal HFT
stack" guide. **Steps 1–7 are done** (8–9 are Linux speed work, saved for later):

1. Record everything that happens on the network (Kraken + Coinbase public
   feeds) to an append-only log file, with local timestamps.
2. A state machine that follows the log file, keeps the order books and prints
   the timestamp + best bid/ask + mid whenever the top of a book changes.
3. A replay mode that runs the same state machine on an old log with no
   network. Its output must match the live output exactly.

4. The state machine can **act**: reconnect a feed whose book went bad, place
   and cancel orders. Every action is written to the log first, then sent.
   A **1-second timer** event is logged too (step 7), so time-based decisions
   (like the test order's 5-second cancel) happen even in a quiet market.
5. One **test order** on Kraken: a tiny post-only buy 5% under the bid,
   cancelled 5 seconds after Kraken confirms it. Replay must make exactly the
   same decisions.

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

## Step 5: the test order

### 1. Kraken API key (new key, just for mm-stack)

In Kraken: Settings → API → create key. Give it only:
query open orders & trades, create & modify orders, cancel & close orders,
and WebSocket API access. **No withdraw, no deposit, no funding.**

Put it in `secrets\kraken.json` (the `secrets` folder is git-ignored):

```json
{"key": "your-api-key", "secret": "your-private-key"}
```

or set `KRAKEN_API_KEY` and `KRAKEN_API_SECRET` instead.

You need a few dollars of free USD on Kraken (the order is Kraken's minimum
size, read from Kraken's instrument data; about $3-4 of BTC right now).

### 2. Dry run first (no keys, nothing sent)

Record a couple of minutes, then replay it pretending the private feed is ready:

```
python -m mmstack.main live --kraken "BTC/USD" --coinbase "" --data-dir E:\mmdata
python -m mmstack.main replay E:\mmdata\<log>.jsonl --test-order BTC/USD --pretend-private
```

You should see one `ACTION place ... price=... qty=...` line. Check the price
is about 5% under the bid.

### 3. Live

```
python -m mmstack.main live --test-order BTC/USD --enable-trading --data-dir E:\mmdata --out out_live.txt
```

Wait for `TEST ORDER COMPLETE`, then Ctrl+C. Then:

```
python -m mmstack.main replay E:\mmdata\<log>.jsonl --out out_replay.txt
fc out_live.txt out_replay.txt
```

`fc` must find no differences, and the summary must show `actions.logged=2`,
no `actions.mismatch`, and `test_order=done`.

### Safety rules (in the network layer, not just the strategy)

- Orders are only sent with `--enable-trading`. Without it the network layer
  refuses (`action ... rejected: trading disabled`).
- At most **1 order per session**.
- Post-only limit orders only.
- Only **our own** orders are ever cancelled (by `cl_ord_id`, which starts
  with `mm`). No `cancel_all` and no dead man's switch: both act on the whole
  Kraken account and would cancel the spread bot's orders too.
- On Ctrl+C every order this session placed gets a cancel.
- API key, signature and WebSocket token never go in the log (`<redacted>`).

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
- A bad book triggers `ACTION reconnect` for that exchange (once, until it
  reconnects), and a fresh snapshot rebuilds it.
- `ACTION MISMATCH` means replay decided something different from what live
  logged. That must never happen; if it does, the state machine depends on
  something that isn't in the log.

## Layout

| File | Job |
|---|---|
| `mmstack/net.py` | All connections on one thread: sockets + `selectors`, TLS via `ssl.MemoryBIO`, WebSocket via `wsproto` (both "sans-io"). Logs before sending. Reconnects; connect and idle timeouts. |
| `mmstack/eventlog.py` | `EventWriter` (append + flush every event), `EventTailer` (follow a growing file or read a `.gz`, in 1 MB batches, never half a line), `compress_log`. |
| `mmstack/books.py` | Order book in exact `Decimal`s; Kraken checksum. |
| `mmstack/state.py` | The state machine: books, actions, the test order. Time comes only from events. |
| `mmstack/kraken_private.py` | Kraken private WebSocket: executions feed, add/cancel orders, safety rules. |
| `mmstack/kraken_auth.py` | API keys, request signing, WebSocket token (all redacted in logs). |
| `mmstack/venues.py` | Exchange URLs and subscribe messages. |
| `mmstack/main.py` | `live` and `replay`. |

## Log format

One JSON object per line:
`{"seq": 1, "t": <ns>, "conn": "kraken", "type": "ws_in", "data": {"text": "<raw message>"}}`

Types: `conn_open`, `tcp_connected`, `tls_ok`, `http_out`, `http_in`, `ws_open`,
`ws_out`, `ws_in`, `ws_ping_in`, `ws_pong_out`, `ws_close_in`, `ws_close_out`,
`conn_close`, plus `timer` (every second: `{"n": 12, "late_ms": 0.4}`), `session` (settings), `action` (logged by the state
machine before sending), `action_rejected` (refused by the network layer).
Logs go in `data/` by default (git-ignored).
