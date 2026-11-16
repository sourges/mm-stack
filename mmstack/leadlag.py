"""Does one exchange's price move before the other's?  (Research tool.)

Replays a log through the normal state machine, samples both mid prices on a
fixed time grid (default every 50 ms, by OUR receive time, which is what a
bot at this location would see), and reports:

1. Correlation of short-term price changes at different delays. If the best
   match is at "+150 ms", the first venue's moves show up on the second venue
   about 150 ms later.
2. An event study: every time the leader's mid jumps (default 2 bps within
   100 ms), how much of that move has the follower already made, and how much
   does it make over the next 50 ms ... 1 s?

This is analysis, not part of the deterministic trading core, so it uses
plain floats.
"""

from .state import StateMachine

HORIZONS_MS = (50, 100, 200, 500, 1000)


def sample_mids(tailer, pairs, step_ns):
    """pairs: {"name": (venue, symbol)}. Returns ({name: [mid or None, ...]}, start_t).
    Sample i is the mid in effect at start_t + i * step_ns."""
    sm = StateMachine()
    series = {name: [] for name in pairs}
    next_t = None
    while True:
        events = tailer.read_available()
        if not events:
            break
        for ev in events:
            t = ev["t"]
            if next_t is None:
                next_t = start_t = t
            while t > next_t:  # record the state BEFORE this event, for every grid point passed
                for name, key in pairs.items():
                    book = sm.books.get(key)
                    mid = book.mid() if book is not None and sm.valid.get(key) else None
                    series[name].append(float(mid) if mid is not None else None)
                next_t += step_ns
            sm.update(ev)
    return series, (start_t if next_t is not None else None)


def changes_bps(series):
    """Step-to-step change in basis points; None where either side is missing."""
    out = []
    for a, b in zip(series, series[1:]):
        out.append((b - a) / a * 10000 if a is not None and b is not None else None)
    return out


def lag_correlation(lead, follow, max_lag):
    """corr(lead change at t, follow change at t + lag) for lag in -max_lag..max_lag steps.
    A peak at a POSITIVE lag means `lead` moves first."""
    ra, rb = changes_bps(lead), changes_bps(follow)
    n = min(len(ra), len(rb))
    result = {}
    for lag in range(-max_lag, max_lag + 1):
        sxy = sxx = syy = 0.0
        lo, hi = max(0, -lag), min(n, n - lag)
        for t in range(lo, hi):
            x, y = ra[t], rb[t + lag]
            if x is None or y is None:
                continue
            sxy += x * y
            sxx += x * x
            syy += y * y
        result[lag] = sxy / (sxx * syy) ** 0.5 if sxx > 0 and syy > 0 else 0.0
    return result


def event_study(lead, follow, step_ms, window_ms=100, threshold_bps=2.0, quiet_ms=1000):
    """Each time `lead` moves >= threshold within the window: average move of
    the follower, in the jump's direction, measured from the start of the window."""
    w = max(1, window_ms // step_ms)
    hs = [h // step_ms for h in HORIZONS_MS]
    quiet = quiet_ms // step_ms
    n = min(len(lead), len(follow))
    last_event = -quiet
    count = 0
    lead_jump = follow_now = 0.0
    lead_later = [0.0] * len(hs)
    follow_later = [0.0] * len(hs)
    for t in range(w, n - max(hs)):
        if t - last_event < quiet:
            continue
        l0, l1, f0, f1 = lead[t - w], lead[t], follow[t - w], follow[t]
        if None in (l0, l1, f0, f1):
            continue
        jump = (l1 - l0) / l0 * 10000
        if abs(jump) < threshold_bps:
            continue
        ends = [(lead[t + h], follow[t + h]) for h in hs]
        if any(a is None or b is None for a, b in ends):
            continue
        sign = 1.0 if jump > 0 else -1.0
        last_event = t
        count += 1
        lead_jump += abs(jump)
        follow_now += sign * (f1 - f0) / f0 * 10000
        for i, (a, b) in enumerate(ends):
            lead_later[i] += sign * (a - l0) / l0 * 10000
            follow_later[i] += sign * (b - f0) / f0 * 10000
    if count == 0:
        return {"events": 0}
    return {"events": count,
            "lead_jump": lead_jump / count,
            "follow_at_jump": follow_now / count,
            "lead_later": [x / count for x in lead_later],
            "follow_later": [x / count for x in follow_later]}


def report(series, name_a, name_b, step_ms, max_lag_ms=1000, threshold_bps=2.0):
    """Text report, as a list of lines."""
    a, b = series[name_a], series[name_b]
    both = sum(1 for x, y in zip(a, b) if x is not None and y is not None)
    lines = [f"leadlag {name_a} vs {name_b}: {both} samples every {step_ms} ms "
             f"({both * step_ms / 3600000:.2f} hours with both books valid)"]
    if both < 100:
        return lines + ["leadlag not enough data"]
    corr = lag_correlation(a, b, max_lag_ms // step_ms)
    best = max(corr, key=lambda k: corr[k])
    lines.append(f"leadlag correlation of {step_ms} ms price changes, by delay "
                 f"(+ = {name_a} first, - = {name_b} first):")
    shown = [k for k in sorted(corr) if abs(k * step_ms) <= 300 or (k * step_ms) % 250 == 0]
    for k in shown:
        mark = "  <-- strongest" if k == best else ""
        lines.append(f"leadlag   {k * step_ms:+6d} ms  {corr[k]:+.3f}{mark}")
    if best > 0:
        lines.append(f"leadlag strongest at {best * step_ms:+d} ms: {name_a} tends to move first")
    elif best < 0:
        lines.append(f"leadlag strongest at {best * step_ms:+d} ms: {name_b} tends to move first")
    else:
        lines.append("leadlag strongest at 0 ms: they move together (within one step)")
    for lead, follow in ((name_a, name_b), (name_b, name_a)):
        ev = event_study(series[lead], series[follow], step_ms, threshold_bps=threshold_bps)
        lines.append(f"leadlag when {lead} jumps >= {threshold_bps:g} bps in 100 ms: "
                     f"{ev['events']} events")
        if not ev["events"]:
            continue
        lines.append(f"leadlag   {lead} jump {ev['lead_jump']:.2f} bps; "
                     f"{follow} had already moved {ev['follow_at_jump']:+.2f} bps")
        later = "  ".join(f"+{h}ms {x:+.2f}" for h, x in zip(HORIZONS_MS, ev["follow_later"]))
        lines.append(f"leadlag   {follow} total move after: {later}")
        keep = "  ".join(f"+{h}ms {x:+.2f}" for h, x in zip(HORIZONS_MS, ev["lead_later"]))
        lines.append(f"leadlag   {lead} total move after:  {keep}")
        still = ev["follow_later"][-1] - ev["follow_at_jump"]
        lines.append(f"leadlag   => {follow} still had {still:+.2f} bps to go in the next second")
    return lines
