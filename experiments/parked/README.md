# Parked experiments

Not in the queue and not run at night. Each one waits for something first.

- `7-requote-0.3-link.json` (from the "Trading spread analysis" chat, Nov 24):
  requote 0.3 vs 0.1 on the booth WITHOUT the alarm. Waits for: the
  December sweep. Requote 0.1 / 0.3 / 1.0 on the alarm booth
  (`--mm-alarm-bps 2`, tuned_on 20261114-203102) answers it properly; this
  file is kept only as a reminder.
- `8-no-skew-link.json` (same chat): skew off vs on. Skew limits inventory
  risk, which average PnL can't see. Waits for: risk numbers in the paper
  summary (worst inventory held, average inventory, worst intraday PnL drop),
  added together with another replay-code change; then rerun with the alarm
  in the base and a risk-based judge.
