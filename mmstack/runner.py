"""Experiment runner: one idea, every usable day, a fixed verdict.

An experiment file says what to compare and how to judge it, BEFORE any
result is seen:

    {
      "question": "Does the Coinbase alarm at 3 bps improve the 1 s markout?",
      "base":   ["--paper-mm"],
      "change": ["--mm-alarm-bps", "3"],
      "judge":  {"number": "markout_1s", "better": "higher"},
      "needs":  ["kraken", "coinbase"],
      "tuned_on": ["20261114-203102"]
    }

"tuned_on" (optional): diaries whose days were used to pick the setting
being tested. They're left out of the verdict (rule: tune on some days,
judge on others).

For each usable test day (from days.py) it replays base and base+change with
--from/--to, then judges each symbol, weekdays and weekends separately:

  WORKS          at least MIN_DAYS days, better on at least 6 of every 7, and
                 the average gain is more than twice its wobble (standard error)
  DOESN'T WORK   the same, the other way
  NOT ENOUGH EVIDENCE   everything else (the default)

Booth hiding: a day where the change keeps less than a quarter of the
baseline's fills counts as worse, however good its markout looks, and its
gain isn't averaged in. Judging on total $ (pnl_fee0) rather than per $ or
per fill also keeps hiding from looking good.

Each replay's output is saved under a key made of the diary, the day, the
settings and a fingerprint of the bot's code, so a rerun only replays what's
new, and changing the code redoes everything.
"""

import csv
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from . import days as days_mod

MIN_DAYS = 7
WIN_SHARE = 6 / 7  # better on at least 6 of every 7 days
WOBBLES = 2  # the average gain must be more than this many standard errors
HIDING_SHARE = 0.25  # change keeps fewer fills than this share of base: hiding
# (an alarm is MEANT to drop fills: the Coinbase alarm keeps ~40%. Hiding is
# the imbalance alarm keeping 2.4%. Judge on total $ (pnl_fee0), not per $,
# and hiding can't win anyway.)
PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(PACKAGE_DIR)

# number name -> (summary section, key)   section None = the first "fills=" line
NUMBERS = {
    "fills": (None, "fills"),
    "markout_1s": ("markout", "1s"),
    "markout_5s": ("markout", "5s"),
    "markout_30s": ("markout", "30s"),
    "pnl_fee0": ("pnl", "fee0.00%"),
    "pnl_fee0.10": ("pnl", "fee0.10%"),
    "pnl_fee0.40": ("pnl", "fee0.40%"),
    "breakeven_fee": ("pnl", "breakeven_fee"),
    "spread_earned": ("pnl_split", "spread_earned"),
    "inventory_drift": ("pnl_split", "inventory_drift"),
}
# what each report/CSV row shows, besides the judged number
COLUMNS = ["fills", "through", "stale", "markout_1s", "markout_5s", "markout_30s",
           "pnl_fee0", "pnl_fee0.10", "pnl_fee0.40", "breakeven_fee", "spread_earned",
           "inventory_drift", "quoted_time", "requotes", "held_back", "peak"]


class ExperimentError(ValueError):
    pass


# ---------- the experiment file ----------
def load_experiment(path):
    try:
        with open(path, encoding="utf-8") as f:
            exp = json.load(f)
    except OSError as e:
        raise ExperimentError(f"can't read {path}: {e}")
    except ValueError as e:
        raise ExperimentError(f"{path} is not valid JSON: {e}")
    if not isinstance(exp, dict):
        raise ExperimentError("the experiment file must be a JSON object { ... }")
    for key in ("question", "base", "change", "judge"):
        if key not in exp:
            raise ExperimentError(f"missing \"{key}\" (write the prediction before running)")
    if not isinstance(exp["base"], list) or not all(isinstance(a, str) for a in exp["base"]):
        raise ExperimentError('"base" must be a list of strings, e.g. ["--paper-mm"]')
    if "--paper-mm" not in exp["base"]:
        raise ExperimentError('"base" must include "--paper-mm"')
    if (not isinstance(exp["change"], list) or not exp["change"]
            or not all(isinstance(a, str) for a in exp["change"])):
        raise ExperimentError('"change" must be a non-empty list of strings: the ONE thing that differs')
    judge = exp["judge"]
    if not isinstance(judge, dict) or judge.get("number") not in NUMBERS:
        raise ExperimentError(f'"judge" needs "number", one of: {", ".join(NUMBERS)}')
    if judge.get("better") not in ("higher", "lower"):
        raise ExperimentError('"judge" needs "better": "higher" or "lower"')
    needs = exp.setdefault("needs", ["kraken", "coinbase"])
    bad = [v for v in needs if v not in days_mod.VENUES]
    if bad or not needs:
        raise ExperimentError(f'"needs" must list venues from: {", ".join(days_mod.VENUES)}')
    tuned = exp.setdefault("tuned_on", [])
    if not isinstance(tuned, list) or not all(isinstance(x, str) for x in tuned):
        raise ExperimentError('"tuned_on" must be a list of diary names, e.g. ["20261114-203102"]')
    exp.setdefault("name", os.path.splitext(os.path.basename(path))[0])
    return exp


# ---------- code fingerprint ----------
# Files that can't change a replay's output: editing them keeps saved replays.
NOT_REPLAY_CODE = ("runner.py", "days.py")


def code_fingerprint(package_dir=PACKAGE_DIR):
    """Hash of the package's .py files that replay uses (all but NOT_REPLAY_CODE).
    Line endings are normalised, so the same code checked out on Windows and
    Linux gives the same fingerprint."""
    h = hashlib.sha256()
    for name in sorted(os.listdir(package_dir)):
        if name.endswith(".py") and name not in NOT_REPLAY_CODE:
            with open(os.path.join(package_dir, name), "rb") as f:
                h.update(name.encode() + b"\0" + f.read().replace(b"\r\n", b"\n") + b"\0")
    return h.hexdigest()[:16]


# ---------- reading a replay's summary ----------
PAIR = re.compile(r"([A-Za-z0-9_.%]+)=([^\s()]+)")


def _number(text):
    """'-1.02bps' -> -1.02, '100.0%' -> 100.0, '120/125' -> 120.0, 'n/a' -> None."""
    m = re.match(r"^-?\d+(\.\d+)?", text)
    return float(m.group(0)) if m else None


def parse_summary(text):
    """Replay output -> {"hours": float|None, "problems": [lines],
    "symbols": {symbol: {column: float|None}}}."""
    hours, problems, raw = None, [], {}
    for line in text.splitlines():
        if line.startswith("summary window "):
            hours = _number(dict(PAIR.findall(line)).get("hours", ""))
            continue
        if not line.startswith("summary "):
            if line.strip():
                problems.append(line)
            continue
        if not line.startswith("summary paper ") or line.startswith("summary paper settings"):
            continue
        rest = line[len("summary paper "):].split(" ", 2)
        if len(rest) < 2:
            continue
        sym = rest[0]
        section = None if "=" in rest[1] else rest[1]
        for k, v in PAIR.findall(line):
            raw.setdefault(sym, {})[(section, k)] = v
    symbols = {}
    for sym, kv in raw.items():
        row = {name: _number(kv[key]) if key in kv else None for name, key in NUMBERS.items()}
        row["through"] = _number(kv.get((None, "through"), ""))
        row["stale"] = _number(kv.get((None, "stale"), ""))
        row["quoted_time"] = _number(kv.get((None, "quoted_time"), ""))
        row["requotes"] = _number(kv.get((None, "requotes"), ""))
        row["held_back"] = _number(kv.get(("rate_limit", "held_back"), ""))
        row["peak"] = _number(kv.get(("rate_limit", "peak"), ""))
        symbols[sym] = row
    return {"hours": hours, "problems": problems, "symbols": symbols}


# ---------- planning ----------
def iso(ns):
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _diary_id(name):
    """'20261114-203102.jsonl.gz' -> '20261114-203102'."""
    return days_mod.diary_stem(name)[:-len(".jsonl")]


def choose_days(found, needs, tuned_on=()):
    """(use, left_out): usable days that have every needed venue and no gap in
    one, and weren't used to tune the setting."""
    tuned = {_diary_id(t) if t.endswith((".jsonl", ".gz")) else t for t in tuned_on}
    use, left_out = [], []
    for d in found:
        if not d["usable"]:
            continue
        if _diary_id(d["diary"]) in tuned:
            left_out.append((d, "used to tune this setting"))
            continue
        missing = [v for v in needs if v not in d["venues"]]
        gaps = [f"{v} {d['gaps'][v]}m" for v in needs if v in d["gaps"]]
        if missing:
            left_out.append((d, "no " + ", ".join(missing)))
        elif gaps:
            left_out.append((d, "gap " + ", ".join(gaps)))
        else:
            use.append(d)
    return use, left_out


def replay_args(day, flags, skip, data_dir):
    args = ["replay", os.path.join(data_dir, day["diary"]), *flags,
            "--from", iso(day["from_ns"]), "--to", iso(day["to_ns"]), "--summary-only"]
    if skip:
        args += ["--skip-conns", ",".join(skip)]
    return args


def job_key(day, flags, skip, code):
    blob = json.dumps({"diary": days_mod.diary_stem(day["diary"]), "from": day["from_ns"],
                       "to": day["to_ns"], "flags": flags, "skip": skip, "code": code},
                      sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def plan(exp, use, data_dir, code):
    """One job per day per side."""
    skip = [v for v in days_mod.VENUES if v not in exp["needs"]]
    sides = {"base": list(exp["base"]), "change": list(exp["base"]) + list(exp["change"])}
    jobs = []
    for d in use:
        for side, flags in sides.items():
            jobs.append({"day": d, "side": side, "flags": flags,
                         "args": replay_args(d, flags, skip, data_dir),
                         "key": job_key(d, flags, skip, code)})
    return jobs


# ---------- running ----------
def low_priority():
    """Keyword arguments that start a child process at low priority."""
    if sys.platform == "win32":
        return {"creationflags": subprocess.BELOW_NORMAL_PRIORITY_CLASS}
    return {"preexec_fn": lambda: os.nice(10)}


def run_replay_process(args):
    """Run one replay as its own process. Returns (ok, output)."""
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    p = subprocess.run([sys.executable, "-m", "mmstack.main", *args], cwd=REPO_DIR, env=env,
                       capture_output=True, text=True, encoding="utf-8", errors="replace",
                       **low_priority())
    if p.returncode != 0:
        return False, (p.stdout + p.stderr)[-4000:]
    return True, p.stdout


def run_jobs(jobs, cache_dir, workers=2, runner=run_replay_process, say=print):
    """Fill job["output"] (from the cache or by replaying). Failed jobs get
    job["error"] and are not cached, so the next run tries again."""
    os.makedirs(cache_dir, exist_ok=True)
    todo = []
    for j in jobs:
        path = os.path.join(cache_dir, j["key"] + ".txt")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                j["output"] = f.read()
            j["cached"] = True
        else:
            todo.append(j)
    say(f"{len(jobs)} replays: {len(jobs) - len(todo)} saved from earlier runs, {len(todo)} to run "
        f"({workers} at a time)")
    if not todo:
        return
    started = time.time()

    def one(j):
        t0 = time.time()
        ok, out = runner(j["args"])
        return j, ok, out, time.time() - t0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(one, j) for j in todo]
        for n, fut in enumerate(as_completed(futures), 1):
            j, ok, out, secs = fut.result()
            label = f"{j['day']['diary']} #{j['day']['piece']} {j['side']}"
            if ok:
                j["output"] = out
                tmp = os.path.join(cache_dir, j["key"] + ".tmp")
                with open(tmp, "w", encoding="utf-8", newline="\n") as f:
                    f.write(out)
                os.replace(tmp, os.path.join(cache_dir, j["key"] + ".txt"))
                say(f"[{n}/{len(todo)}] {label} done in {secs / 60:.1f} min "
                    f"({(time.time() - started) / 60:.0f} min so far)")
            else:
                j["error"] = out
                say(f"[{n}/{len(todo)}] {label} FAILED after {secs / 60:.1f} min")


# ---------- judging ----------
def compare(exp, jobs):
    """Per day and symbol: base and change rows, and the difference.
    Returns (rows, left_out). A row's diff is positive when the change is better."""
    number, better = exp["judge"]["number"], exp["judge"]["better"]
    by_day = {}
    for j in jobs:
        by_day.setdefault((j["day"]["diary"], j["day"]["piece"]), {"day": j["day"]})[j["side"]] = j
    rows, left_out = [], []
    for pair in by_day.values():
        d = pair["day"]
        if any("error" in pair[s] for s in ("base", "change")):
            left_out.append((d, None, "replay failed"))
            continue
        res = {s: parse_summary(pair[s]["output"]) for s in ("base", "change")}
        problems = res["base"]["problems"] + res["change"]["problems"]
        if any("ACTION MISMATCH" in p for p in problems):
            left_out.append((d, None, "ACTION MISMATCH (replay doesn't match the log)"))
            continue
        symbols = sorted(set(res["base"]["symbols"]) & set(res["change"]["symbols"]))
        wanted = exp.get("symbols")
        for sym in symbols:
            if wanted and sym not in wanted:
                continue
            b, c = res["base"]["symbols"][sym], res["change"]["symbols"][sym]
            if b[number] is None or c[number] is None:
                left_out.append((d, sym, f"no {number} (no fills?)"))
                continue
            diff = c[number] - b[number] if better == "higher" else b[number] - c[number]
            hiding = bool(b["fills"]) and (c["fills"] or 0) < HIDING_SHARE * b["fills"]
            rows.append({"day": d, "symbol": sym, "base": b, "change": c, "diff": diff,
                         "hiding": hiding, "problems": len(problems),
                         "hours": res["base"]["hours"]})
    return rows, left_out


def verdict(rows):
    """(word, reason) for one symbol and one kind of day."""
    n = len(rows)
    if n == 0:
        return "NOT ENOUGH EVIDENCE", "no days"
    # a hiding day can't add to the average gain (its markout looks good only
    # because it hardly traded); it can still pull the average down
    diffs = [min(r["diff"], 0.0) if r["hiding"] else r["diff"] for r in rows]
    wins = sum(1 for r in rows if r["diff"] > 0 and not r["hiding"])
    losses = sum(1 for r in rows if r["diff"] < 0 or r["hiding"])
    mean = sum(diffs) / n
    wobble = (math.sqrt(sum((x - mean) ** 2 for x in diffs) / (n - 1)) / math.sqrt(n)) if n > 1 else float("inf")
    hidden = sum(1 for r in rows if r["hiding"])
    wob = "n/a" if math.isinf(wobble) else f"{wobble:.3f}"
    stats = (f"better on {wins} of {n}, worse on {losses}; avg {mean:+.3f} "
             f"(wobble {wob})" + (f"; {hidden} day(s) hiding" if hidden else ""))
    if n < MIN_DAYS:
        return "NOT ENOUGH EVIDENCE", f"{n} days (need {MIN_DAYS}); {stats}"
    if wins >= WIN_SHARE * n and mean > WOBBLES * wobble:
        return "WORKS", stats
    if losses >= WIN_SHARE * n and mean < -WOBBLES * wobble:
        return "DOESN'T WORK", stats
    if wins >= WIN_SHARE * n or losses >= WIN_SHARE * n:
        return "NOT ENOUGH EVIDENCE", f"too noisy: {stats}"
    return "NOT ENOUGH EVIDENCE", stats


# ---------- output ----------
def _fmt(x):
    return "n/a" if x is None else f"{x:g}"


def report(exp, code, rows, left_out, tz):
    number = exp["judge"]["number"]
    lines = [f"experiment: {exp['name']}",
             f"question:   {exp['question']}",
             f"base:       {' '.join(exp['base'])}",
             f"change:     {' '.join(exp['change'])}",
             f"judged by:  {number}, {exp['judge']['better']} is better",
             f"needs:      {', '.join(exp['needs'])}    code: {code}",
             f"rule:       {MIN_DAYS}+ days, better on {round(WIN_SHARE * 7)} of 7, avg gain > "
             f"{WOBBLES}x wobble; a day keeping < {HIDING_SHARE:.0%} of base fills counts as worse",
             ""]
    for sym in sorted({r["symbol"] for r in rows}):
        for kind in ("weekday", "weekend"):
            group = [r for r in rows if r["symbol"] == sym and r["day"]["kind"] == kind]
            word, why = verdict(group)
            lines.append(f"{sym:<9} {kind:<8} {word}: {why}")
    lines += ["", f"{'day':<34} {'kind':<8} {'symbol':<9} {'hours':>5} {'base':>9} {'change':>9} "
                  f"{'diff':>8} {'fills b/c':>11} {'held_back b/c':>15}  notes"]
    for r in sorted(rows, key=lambda r: (r["symbol"], r["day"]["from_ns"])):
        d = r["day"]
        when = days_mod._local(d["from_ns"], tz)
        notes = []
        if r["hiding"]:
            notes.append("hiding")
        if r["problems"]:
            notes.append(f"{r['problems']} problem lines")
        notes += [f for f in d["flags"] if f == "short"]
        lines.append(f"{when + ' (' + d['diary'][:15] + ')':<34} {d['kind']:<8} {r['symbol']:<9} "
                     f"{_fmt(r['hours']):>5} {_fmt(r['base'][number]):>9} {_fmt(r['change'][number]):>9} "
                     f"{r['diff']:>+8.3f} {_fmt(r['base']['fills']) + '/' + _fmt(r['change']['fills']):>11} "
                     f"{_fmt(r['base']['held_back']) + '/' + _fmt(r['change']['held_back']):>15}  "
                     f"{', '.join(notes)}")
    if left_out:
        lines += ["", "left out:"]
        for d, sym, why in left_out:
            lines.append(f"  {days_mod._local(d['from_ns'], tz)} ({d['diary']} #{d['piece']})"
                         f"{' ' + sym if sym else ''}: {why}")
    return lines


CSV_FIELDS = (["run_at", "experiment", "code", "diary", "piece", "from", "to", "hours", "kind",
               "side", "symbol"] + COLUMNS + ["problem_lines", "hiding"])


def append_csv(path, exp, code, rows, run_at):
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(CSV_FIELDS)
        for r in rows:
            d = r["day"]
            for side in ("base", "change"):
                vals = r[side]
                w.writerow([run_at, exp["name"], code, d["diary"], d["piece"], iso(d["from_ns"]),
                            iso(d["to_ns"]), r["hours"], d["kind"], side, r["symbol"]]
                           + ["" if vals.get(c) is None else vals[c] for c in COLUMNS]
                           + [r["problems"], int(r["hiding"])])


# ---------- the whole run ----------
def run(exp_path, data_dir, results_dir, workers=2, newest=None, dry_run=False,
        runner=run_replay_process, say=print):
    exp = load_experiment(exp_path)
    code = code_fingerprint()
    tz = days_mod.new_york()
    found, recording = days_mod.find_days(
        data_dir, progress=lambda name: say(f"scanning {name} (once; saved to the index)..."))
    use, skipped = choose_days(found, exp["needs"], exp["tuned_on"])
    if newest:
        use = use[-newest:]
    say(f"{exp['name']}: {len(use)} days to use "
        f"({sum(d['kind'] == 'weekday' for d in use)} weekday, "
        f"{sum(d['kind'] == 'weekend' for d in use)} weekend), {len(skipped)} left out")
    jobs = plan(exp, use, data_dir, code)
    out_dir = os.path.join(results_dir, exp["name"])
    cache_dir = os.path.join(results_dir, "cache")
    if dry_run:
        for j in jobs:
            saved = os.path.exists(os.path.join(cache_dir, j["key"] + ".txt"))
            say(f"{'saved' if saved else 'to run'}: python -m mmstack.main {' '.join(j['args'])}")
        return None
    run_jobs(jobs, cache_dir, workers=workers, runner=runner, say=say)
    rows, left_out = compare(exp, jobs)
    left_out = [(d, None, why) for d, why in skipped] + left_out
    lines = report(exp, code, rows, left_out, tz)
    os.makedirs(out_dir, exist_ok=True)
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(os.path.join(out_dir, "report.txt"), "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join([f"run at {run_at}"] + lines) + "\n")
    append_csv(os.path.join(results_dir, "results.csv"), exp, code, rows, run_at)
    say("")
    for line in lines:
        say(line)
    say("")
    say(f"saved: {os.path.join(out_dir, 'report.txt')}")
    return rows
