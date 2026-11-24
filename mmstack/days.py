"""Test days: which recorded days do we have, and can we trust them?

Each diary (log file) is cut into 24-hour pieces from its first event. A
diary that starts at midnight (daily rotation) gives calendar days; an old
diary that started at 7:31 pm gives 7:31 pm -> 7:31 pm days. One rule for both.

For every piece we report:
  - hours actually covered (a piece under MIN_HOURS isn't used, under
    SHORT_HOURS is flagged "short")
  - weekday or weekend: where most of its hours fall, Saturday 00:00 to
    Monday 00:00 New York time
  - which venues sent data in it (a Binance test can't use a day without Binance)
  - gaps: a venue that sent nothing for GAP_NS or longer
  - drops: how often each venue's connection closed

Scanning a big .gz diary means reading all of it (gzip can't jump to the
end), so each diary is scanned once and the result kept in an index file.
Only the start of each line is read (time, connection, event type), never
the message itself.
"""

import json
import os
import re
import time
from datetime import datetime, timezone

from .eventlog import open_log

NS = 1_000_000_000
HOUR_NS = 3600 * NS
SLICE_NS = 24 * HOUR_NS
MIN_HOURS = 16  # shorter pieces are listed but not used
SHORT_HOURS = 20  # shorter pieces are flagged "short"
GAP_NS = 5 * 60 * NS  # a venue silent this long is a gap, not a quiet market
RECENT_SECONDS = 120  # a diary changed this recently is probably still recording
VENUES = ("kraken", "coinbase", "binanceus", "binance")
INDEX_NAME = "days_index.json"
INDEX_VERSION = 1  # bump when the scan result changes shape: old entries get rescanned

# {"seq":12,"t":1763000000123456789,"conn":"kraken","type":"ws_in",...
LINE_START = re.compile(rb'^\{"seq":\d+,"t":(\d+),"conn":"([^"]*)","type":"([^"]*)"')


def new_york():
    """The New York time zone. Windows Python needs the tzdata package for it."""
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        return ZoneInfo("America/New_York")
    except ZoneInfoNotFoundError:
        raise SystemExit("New York time zone not found: run  pip install -r requirements.txt  "
                         "(Windows needs the tzdata package)")


def diary_stem(name):
    """'20261116-193111.jsonl.gz' -> '20261116-193111.jsonl' (same diary, compressed or not)."""
    return name[:-3] if name.endswith(".gz") else name


# ---------- scanning one diary ----------
def scan(path, gap_ns=GAP_NS):
    """Read a diary once. Returns a plain dict (it goes into the index file):
        first_t, last_t, events
        slices: one per 24 h from first_t: {"data": {venue: n}, "drops": {venue: n}}
        gaps: {venue: [[silence_start, silence_end], ...]} for silences >= gap_ns,
              including before a venue's first message and after its last one
              (if that venue sent anything at all in this diary)
    Lines that don't start like an event (a half-written last line) are ignored.
    """
    first_t = last_t = None
    events = 0
    slices = []
    last_data = {}  # venue -> t of its latest ws_in
    gaps = {}

    def slice_at(t):
        i = (t - first_t) // SLICE_NS
        while len(slices) <= i:
            slices.append({"data": {}, "drops": {}})
        return slices[i]

    with open_log(path) as f:
        for line in f:
            m = LINE_START.match(line)
            if not m or not line.endswith(b"\n"):
                continue
            t = int(m.group(1))
            conn = m.group(2).decode()
            type_ = m.group(3).decode()
            if first_t is None:
                first_t = t
            last_t = t
            events += 1
            s = slice_at(t)  # every event, so a piece exists even if all venues went quiet
            if conn not in VENUES:
                continue
            if type_ == "ws_in":
                s["data"][conn] = s["data"].get(conn, 0) + 1
                before = last_data.get(conn, first_t)
                if t - before >= gap_ns:
                    gaps.setdefault(conn, []).append([before, t])
                last_data[conn] = t
            elif type_ == "conn_close":
                s["drops"][conn] = s["drops"].get(conn, 0) + 1

    for conn, t in last_data.items():  # silent until the end of the diary
        if last_t - t >= gap_ns:
            gaps.setdefault(conn, []).append([t, last_t])
    return {"first_t": first_t, "last_t": last_t, "events": events,
            "slices": slices, "gaps": gaps}


# ---------- the index (each diary scanned once) ----------
def load_index(path):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save_index(path, index):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=1, sort_keys=True)
    os.replace(tmp, path)  # never leave a half-written index behind


def cached_scan(index, name, size):
    """The saved scan for this diary, or None if it must be (re)scanned.

    Keyed by the name without .gz: a diary scanned as .jsonl is the same
    diary after compress (compress checks the .gz matches it exactly), so its
    scan is reused instead of reading the whole .gz again."""
    entry = index.get(diary_stem(name))
    if not entry or entry.get("version") != INDEX_VERSION:
        return None
    if entry["file"] == name and entry["size"] != size:
        return None  # same file, different size: it changed
    if entry["file"] != name and not entry["file"].endswith(".jsonl"):
        return None  # only a plain .jsonl scan carries over to its .gz
    return entry["scan"]


def diaries(folder, now=None):
    """(finished, recording): diary file names in the folder, sorted.
    A .jsonl that also has a .gz is skipped (compress still deciding)."""
    now = time.time() if now is None else now
    names = sorted(os.listdir(folder))
    finished, recording = [], []
    for name in names:
        full = os.path.join(folder, name)
        if not os.path.isfile(full) or not (name.endswith(".jsonl") or name.endswith(".jsonl.gz")):
            continue
        if name.endswith(".jsonl") and name + ".gz" in names:
            continue
        if name.endswith(".jsonl") and now - os.path.getmtime(full) < RECENT_SECONDS:
            recording.append(name)  # only a plain diary can still be recording; a fresh .gz is done
        else:
            finished.append(name)
    return finished, recording


def scan_folder(folder, index_path=None, now=None, progress=None):
    """Scan every finished diary not already in the index; save the index.
    Returns (scans {name: scan}, recording [names]). progress(name) is called
    before each slow scan."""
    index_path = index_path or os.path.join(folder, INDEX_NAME)
    index = load_index(index_path)
    finished, recording = diaries(folder, now)
    scans = {}
    for name in finished:
        full = os.path.join(folder, name)
        size = os.path.getsize(full)
        result = cached_scan(index, name, size)
        if result is None:
            if progress:
                progress(name)
            result = scan(full)
            index[diary_stem(name)] = {"version": INDEX_VERSION, "file": name,
                                       "size": size, "scan": result}
            save_index(index_path, index)  # after each file, so a stop loses nothing
        elif index[diary_stem(name)]["file"] != name:  # .jsonl -> .gz: note the new file
            index[diary_stem(name)].update(file=name, size=size)
            save_index(index_path, index)
        scans[name] = result
    return scans, recording


# ---------- cutting diaries into test days ----------
def weekend_hours(start_ns, end_ns, tz):
    """Hours of [start, end) that fall on Saturday or Sunday in tz (by the minute)."""
    minute = 60 * NS
    n = 0
    t = start_ns
    while t < end_ns:
        local = datetime.fromtimestamp(t / NS, tz=timezone.utc).astimezone(tz)
        if local.weekday() >= 5:
            n += 1
        t += minute
    return n / 60


def _overlap(a0, a1, b0, b1):
    return max(0, min(a1, b1) - max(a0, b0))


def days_from_scan(name, s, tz):
    """One dict per 24 h piece of this diary."""
    out = []
    if s["first_t"] is None:
        return out
    diary_venues = sorted(set(v for sl in s["slices"] for v in sl["data"]))
    for i, sl in enumerate(s["slices"]):
        start = s["first_t"] + i * SLICE_NS
        end = start + SLICE_NS  # the --to for replay
        covered_end = min(end, s["last_t"])
        hours = (covered_end - start) / HOUR_NS
        weekend = weekend_hours(start, covered_end, tz)
        flags = []
        if hours < MIN_HOURS:
            flags.append("too short")
        elif hours < SHORT_HOURS:
            flags.append("short")
        gap_min = {}
        for venue in diary_venues:
            worst = max((_overlap(g0, g1, start, covered_end)
                         for g0, g1 in s["gaps"].get(venue, [])), default=0)
            if worst >= GAP_NS:
                gap_min[venue] = round(worst / (60 * NS))
                flags.append(f"gap {venue} {gap_min[venue]}m")
        out.append({
            "diary": name, "piece": i + 1, "from_ns": start, "to_ns": end,
            "last_ns": covered_end, "hours": round(hours, 2),
            "kind": "weekend" if weekend > hours / 2 else "weekday",
            "venues": sorted(sl["data"]), "drops": dict(sorted(sl["drops"].items())),
            "gaps": gap_min, "flags": flags, "usable": hours >= MIN_HOURS,
        })
    return out


def find_days(folder, index_path=None, now=None, progress=None):
    """(days, recording): every test day in the folder's finished diaries,
    oldest first, and the diaries skipped because they're still recording."""
    tz = new_york()
    scans, recording = scan_folder(folder, index_path, now, progress)
    days = []
    for name in sorted(scans):
        days.extend(days_from_scan(name, scans[name], tz))
    return days, recording


# ---------- printing ----------
def _local(t_ns, tz):
    return datetime.fromtimestamp(t_ns / NS, tz=timezone.utc).astimezone(tz).strftime("%a %m-%d %H:%M")


def report(days, recording, tz=None):
    tz = tz or new_york()
    lines = ["test days (24 h pieces of each diary, New York time)", ""]
    lines.append(f"{'diary':<26} {'#':>2}  {'from':<15}  {'to':<15} {'hours':>6}  {'kind':<8} "
                 f"{'venues':<34} flags")
    for d in days:
        flags = ", ".join(d["flags"]) or "ok"
        drops = " ".join(f"{v}:{n}" for v, n in d["drops"].items())
        lines.append(f"{d['diary']:<26} {d['piece']:>2}  {_local(d['from_ns'], tz):<15}  "
                     f"{_local(d['last_ns'], tz):<15} {d['hours']:>6.1f}  {d['kind']:<8} "
                     f"{','.join(d['venues']):<34} {flags}" + (f"  (drops {drops})" if drops else ""))
    for name in recording:
        lines.append(f"{name:<26}  still recording, not scanned")
    usable = [d for d in days if d["usable"]]
    clean = [d for d in usable if not d["gaps"]]
    lines.append("")
    lines.append(f"usable days: {sum(d['kind'] == 'weekday' for d in usable)} weekday, "
                 f"{sum(d['kind'] == 'weekend' for d in usable)} weekend "
                 f"({len(usable) - len(clean)} of them with a gap flag)")
    return lines
