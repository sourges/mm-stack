# Experiment queue

The nightly run only picks up files directly in `experiments\`. Files here
wait. Each new experiment has a backlog of ~16 replays (3-4 h) on its first
night, so move **one** file up per evening, e.g.:

    move experiments\queue\1-coinbase-alarm-btc.json experiments\

Order (as of Nov 25; move one per evening, top first):

1. `1-coinbase-alarm-btc`: BTC's version of the LINK alarm (1 bp), never run
   under the 7-day rules.
2. `7-requote-1.0-link`: with the alarm, requote less (1.0x instead of 0.1x)
   to free Kraken rate budget. The alarm booth used the whole budget
   (125/125) on 7 of 7 days.
3. `8-escalating-hold-link`: with the alarm, hold longer when it keeps
   ringing (1 -> 2 -> 4 -> 8 s; reset after a calm minute), so a fast market
   can't turn it into a message storm.
4. `2-vol-alarm-link`: an alarm whose trigger follows how jumpy Coinbase is,
   vs the fixed 2 bp trigger. Shelved Nov 17 after losing 4 of 5 pair-days.
5. `3-max-inv-2-link`: hold at most 2 order sizes instead of 5 (with the
   alarm). Shelved Nov 18 after losing 4 of 5.
6. `4-fair-price-btc`: lean BTC quotes toward Coinbase's price. Shelved Nov 16
   on one 18 h log.
7. `5-spread-0.10-link`: quote LINK twice as wide. Shelved on one log.
8. `6-binanceus-venue-btc`: the same BTC booth on Binance.US instead of
   Kraken (needs Binance.US data, recorded since Nov 18).

`tuned_on` leaves out Sat 11-14 (20261114-203102) wherever a setting in the
file was picked on that diary.
