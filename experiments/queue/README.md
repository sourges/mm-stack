# Experiment queue

The nightly run only picks up files directly in `experiments\`. Files here
wait. Each new experiment has a backlog of ~16 replays (3-4 h) on its first
night, so move **one** file up per evening, e.g.:

    move experiments\queue\1-coinbase-alarm-btc.json experiments\

Order (most promising first):

1. `1-coinbase-alarm-btc`: BTC's version of the LINK alarm (1 bp), never run
   under the 7-day rules.
2. `2-vol-alarm-link`: an alarm whose trigger follows how jumpy Coinbase is,
   vs the fixed 2 bp trigger. Shelved Nov 17 after losing 4 of 5 pair-days.
3. `3-max-inv-2-link`: hold at most 2 order sizes instead of 5 (with the
   alarm). Shelved Nov 18 after losing 4 of 5.
4. `4-fair-price-btc`: lean BTC quotes toward Coinbase's price. Shelved Nov 16
   on one 18 h log.
5. `5-spread-0.10-link`: quote LINK twice as wide. Shelved on one log.
6. `6-binanceus-venue-btc`: the same BTC booth on Binance.US instead of
   Kraken (needs Binance.US data, recorded since Nov 18).

`tuned_on` leaves out Sat 11-14 (20261114-203102) wherever a setting in the
file was picked on that diary.
