# Experiment queue

The nightly run only picks up files directly in `experiments\`. Files here
wait. Each new experiment has a backlog of ~16 replays (3-4 h) on its first
night, so move **one** file up per evening, e.g.:

    move experiments\queue\1-coinbase-alarm-btc.json experiments\

Order (as of Nov 25, afternoon; move one per evening, top first):

1. `6-binanceus-alarm-btc`: the BTC booth on Binance.US (0% maker, so PnL
   before fees is real PnL), alarm 1 bp vs none, plus "is it above $0 on its
   own?". Binance.US is the only 0%-maker venue open to him.
2. `6b-binanceus-alarm-link`: the same for LINK (alarm 2 bps).
3. `1-coinbase-alarm-btc`: BTC's version of the Kraken LINK alarm (1 bp).
4. `7-requote-1.0-link`: with the alarm, requote less (1.0x instead of 0.1x)
   to free Kraken rate budget (the alarm booth used it all, 7 of 7 days).
5. `8-escalating-hold-link`: with the alarm, hold longer when it keeps
   ringing (1 -> 2 -> 4 -> 8 s; reset after a calm minute).
6. `2-vol-alarm-link`: volatility-scaled trigger vs fixed 2 bp. Shelved Nov 17.
7. `3-max-inv-2-link`: hold at most 2 order sizes instead of 5. Shelved Nov 18.
8. `4-fair-price-btc`: lean BTC quotes toward Coinbase's price. Shelved Nov 16.
9. `5-spread-0.10-link`: quote LINK twice as wide. Shelved on one log.
10. `9-rate-margin-15-link`: what a 15-point rate safety margin costs.

`tuned_on` leaves out Sat 11-14 (20261114-203102) wherever a setting in the
file was picked on that diary.
