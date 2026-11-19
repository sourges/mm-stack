# mm-stack

A small market-making trading stack, built step by step from the "minimal HFT
stack" guide. **Steps 1–7 and step 10 (paper mode) are done** (8–9 are Linux speed work, saved for later):

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

Record live (BTC and LINK on Kraken, Coinbase and Binance.US by default;
`--binanceus ''` leaves Binance.US out). Stop with Ctrl+C:

```
python -m mmstack.main live --out out_live.txt
```

The screen stays quiet: one status line a minute (events so far, log size,
how far behind the strategy side is) plus the important events (connects,
disconnects, checksum or sequence errors, orders, paper fills). Every line
still goes to the `--out` file, so the `fc` check below is unchanged. Add
`--verbose` to see every price line on screen.

Ctrl+C stops the recorder at once, then the unread part of the log is
finished so the summary is complete. Press Ctrl+C a second time to skip that
(the summary is then marked INCOMPLETE; the log itself is whole, replay it).
Run long recordings in a plain Command Prompt, not the VS Code terminal.

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

## Step 10 (paper mode): the market maker, with simulated orders

Nothing is sent to an exchange. It runs inside the state machine, so it works
live **and on any recorded log** — you can try many settings on the same data:

```
python -m mmstack.main replay E:\mmdata\<log>.jsonl.gz --paper-mm --mm-spread 0.05
python -m mmstack.main replay E:\mmdata\<log>.jsonl.gz --paper-mm --mm-spread 0.01
python -m mmstack.main live --paper-mm --data-dir E:\mmdata          (records the settings in the log)
```

| Option | Default | Meaning |
|---|---|---|
| `--paper-mm [SYMBOLS]` | `BTC/USD,LINK/USD` | turn it on (Kraken symbols) |
| `--mm-spread` | `0.05` | distance of each quote from mid, in % |
| `--mm-max-inv` | `5` | max inventory, in order sizes |
| `--mm-requote` | `0.1` | only move a quote when it's off by this fraction of the distance |
| `--mm-latency-ms` | `150` | time for a new quote to go live, or an old one to be cancelled (we measured ~110 ms) |
| `--mm-fills` | `queue` | fill rule: `queue` (queue position) or `through` (the old, harsher rule) |
| `--mm-no-skew` | off | baseline: no inventory skew (the max-inventory cap stays on) |
| `--mm-coinbase` | `0` | lean the quotes toward Coinbase's price: `0` = off, `0.65` = suggested |
| `--mm-alarm-bps` | `0` | Coinbase alarm: take one side off after Coinbase jumps this many bps (`0` = off, `2` = suggested) |
| `--mm-alarm-window-ms` | `100` | ...within this many ms |
| `--mm-alarm-hold-ms` | `1000` | ...and keep that side off this long |
| `--mm-alarm-vol` | `0` | Volatility alarm: ring when Coinbase moves K times its normal move per window (`0` = off). `--mm-alarm-bps` then sets a floor |
| `--mm-alarm-vol-tau-s` | `600` | ...where "normal" is averaged over about this many seconds |
| `--mm-venue` | `kraken` | Whose book and trades to quote on: `kraken` or `binanceus` |
| `--mm-rate-tier` | `intermediate` | obey Kraken's order rate limit for this account level (`starter`, `intermediate`, `pro`); `none` = only measure it |

How it works:
- **Quotes** at the spread each side of mid, rounded away from mid to Kraken's tick.
  Size = Kraken's minimum (or ~$5 if the log has no instrument data).
- **Skew (the guide's formula)**, x = |inventory| / max: the side that adds
  inventory moves out by `spread * x / (1 - x)`, the side that reduces it moves
  in by `spread * x`. At the max, the adding side isn't quoted.
- **Order timing.** A new quote is live only after the latency. A quote we
  decide to move or pull **stays live for the latency** and can still be
  filled (a "stale" fill). One change in flight per side at a time.
- **Fills, `queue` rule.** When a quote goes live we note the size already
  resting at its price: the queue ahead of us.
  - A trade *at* our price uses up the queue ahead first; the rest fills us.
  - A trade *through* our price fills us completely.
  - Size that leaves our level without a trade is a cancel, assumed spread
    evenly through the queue. Size added joins behind us.
  - We only record the top 10 levels. If our price is outside them, the queue
    is unknown: no at-price fills, and when the level comes into view we
    assume all of it is ahead of us. `queue_visible` in the summary says how
    often the queue could be seen when a quote went live.
- **Coinbase-adjusted fair price** (`--mm-coinbase`). The two exchanges
  follow each other's jumps over about a second, Coinbase slightly ahead. The
  bot tracks the normal gap between Coinbase's and Kraken's mid (a ~60 s
  average) and centres its quotes on
  `kraken_mid + weight * (coinbase_mid - kraken_mid - normal_gap)`.
  It needs the matching Coinbase product in the recording (BTC/USD ↔ BTC-USD).
- **Coinbase alarm** (`--mm-alarm-bps`). Separate from the fair price above
  (measured Nov 16: the fair price did not help at 150 ms). Small Coinbase
  moves are ignored. When Coinbase's mid jumps the set amount within the
  window, measured from its lowest (or highest) price in the window, the side
  about to be run over is cancelled: the ask after a jump up, the bid after a
  jump down. That side stays off for the hold time, then is quoted again. The
  cancel takes the usual latency, so the quote can still be hit first;
  `fills_during_alarm` in the summary counts those. `bid_off` / `ask_off` are
  the share of time each side was held off.
- **Volatility alarm** (`--mm-alarm-vol K`). Same alarm, but the jump size
  adapts: it rings when Coinbase moves K times its normal move per window.
  "Normal" is the root-mean-square Coinbase move per 100 ms window, averaged
  over about 10 minutes (`--mm-alarm-vol-tau-s`). It stays silent for its
  first 60 s of data, a jump is checked before it is added to the average,
  and a price that hasn't moved never rings. Add `--mm-alarm-bps` as a floor
  (useful on pairs where one price tick is already a big move, like LINK).
  The summary shows the average, lowest and highest jump size it used.
- **Quoting on Binance.US** (`--mm-venue binanceus`). Same market maker, fed
  Binance.US's top-20 book and its trades instead of Kraken's (Coinbase stays
  the reference for the alarm). Differences: queues are visible down to
  level 20; the price tick is read off the book's prices (Binance.US's
  exchange info isn't recorded); order size is about $5; Kraken's rate limit
  is not applied and Binance.US's isn't modeled (`rate_limit not modeled` in
  the summary). Fees: Binance.US maker is 0%, so read the `fee0.00%` column.
  Binance.US books come every 100 ms at most, so queue tracking is coarser
  than on Kraken. Needs a log recorded with Binance.US (Nov 18, 2026 on).
- **Kraken's rate limit** (`--mm-rate-tier`). The bot keeps Kraken's per-pair
  rate counter itself (new order +1; cancel +8 if the order is under 5 s old,
  less as it ages; decays every second). It holds back any new order or
  re-quote it couldn't afford, always keeping room to cancel what it has resting.
- **Stale-data guard**: quotes are pulled if the book is invalid or quiet for 10 s.
- Inventory is relative to a starting holding, so it can go negative.

The summary shows, per symbol:
- fills (at our price / through our price / stale), volume, ending inventory
- **PnL** (inventory valued at mid) at 0%, 0.10% and 0.40% fees, and the
  **break-even fee** (negative = loses money even with no fees)
- **pnl_split**: *spread earned* (how far each fill was from mid at that
  moment) and *inventory drift* (everything else). They add up to the 0% PnL.
- **markouts**: after each fill, how far mid moved for (+) or against (−) us
  after 1 s, 5 s, 30 s, in basis points (1 bp = 0.01%). Negative = picked off.
  Also the 1 s figure by fill type.
- requotes (total and per minute), stale pulls, % of time quoted
- **rate_limit_blocked**: the share of our order actions Kraken's per-pair
  rate counter would have refused, for each account level (starter /
  intermediate / pro), had it not held anything back.
- **rate_limit enforced=...**: how many times an order was held back, cancels
  refused (should be 0), and the counter's peak against the limit.
- **coinbase_signal**: the weight, and how often Coinbase's price was usable.

Paper results recorded in a live log before a setting existed replay with the
old behaviour for that setting, so old logs still replay as they ran.

## Research: does Coinbase move before Kraken?

```
python -m mmstack.main leadlag E:\mmdata\<log>.jsonl.gz --out leadlag.txt
```

Samples both BTC mids every 50 ms (by our receive time) and prints:
- the correlation of 50 ms price changes at different delays. A peak at
  `+100 ms` means Coinbase's moves show up on Kraken about 100 ms later.
- an event study: each time one exchange jumps 2 bps within 100 ms, how much
  the other had already moved and how much it moved over the next second.

Options: `--kraken LINK/USD --coinbase LINK-USD`, `--jump-bps 1`, `--step-ms 50`.

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
- **Binance.US** (`binanceus BTCUSD ...`): it sends the whole top 20 levels
  every 100 ms, so each message replaces our copy and can't leave it wrong.
  Checks in the summary:
  - `books_ok` / `books_bad`: a book with an empty side or bid >= ask is not
    used (`BAD BOOK` line, shown once until a good one arrives).
  - `books_out_of_order`: a book older than the one we have, ignored.
  - `trade_gaps` / `trades_missed`: trade ids count up by one per pair, so a
    jump means trades we never received (`TRADE GAP` line). Ids are compared
    within one connection only; trades missed while disconnected don't count.
  - `top_check_ok` / `top_check_differs`: when the real-time best bid/ask
    message and a book message carry the same update id, their tops must
    agree. `differs` should stay at or near 0.
  - Binance.US closes every connection after 24 hours; it reconnects by itself.
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
| `mmstack/paper_mm.py` | Step 10 paper market maker: quotes, skew, simulated fills, PnL, markouts. |
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
