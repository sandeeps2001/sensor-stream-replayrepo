"""Loss, duplicates, reordering, late data, per-device clock models, and latency."""
from collections import defaultdict


def _percentile(values, p):
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def samples(rows):
    return [r for r in rows if r.get("kind", "sample") == "sample"]


def by_device(rows):
    groups = defaultdict(list)
    for r in rows:
        groups[r["device_id"]].append(r)
    return dict(groups)


def loss_and_reorder(rows):
    """Per-device loss, duplicates, and reordering. Rows must be in arrival order.

    Expected count comes from each device's end-of-session marker (final seq), so
    events lost at the tail of a stream are counted. Without a marker we fall back
    to the highest seq seen, which cannot detect tail loss.
    """
    report = {}
    for dev, drows in by_device(rows).items():
        ends = [r["seq"] for r in drows if r.get("kind") == "end"]
        seqs = [r["seq"] for r in samples(drows)]
        last = max(ends) if ends else max(seqs)
        expected = last + 1
        unique = set(seqs)
        reordered, highest = 0, -1
        seen = set()
        for s in seqs:
            if s in seen:
                continue           # a duplicate is not a reorder
            seen.add(s)
            if s < highest:
                reordered += 1
            highest = max(highest, s)
        report[dev] = {"expected": expected, "received": len(unique),
                       "lost": expected - len(unique),
                       "loss_pct": round(100 * (expected - len(unique)) / expected, 3),
                       "reordered": reordered, "duplicates": len(seqs) - len(unique),
                       "end_marker": bool(ends)}
    return report


def estimate_clock(rows):
    """Fit ingest_ts ~ a + b * device_ts for ONE device, using the earliest arrivals.

    Network delay is always >= 0, so the lower envelope of (ingest - device) tracks
    the clock relationship. We fit on the minimum-delay point in each 1 s window.
    """
    windows = {}
    for r in samples(rows):
        w = int(r["device_ts"])
        d = r["ingest_ts"] - r["device_ts"]
        if w not in windows or d < windows[w][1]:
            windows[w] = (r["device_ts"], d)
    pts = sorted(windows.values())
    n = len(pts)
    mx = sum(p[0] for p in pts) / n
    my = sum(p[1] for p in pts) / n
    sxx = sum((p[0] - mx) ** 2 for p in pts)
    slope = sum((p[0] - mx) * (p[1] - my) for p in pts) / sxx if sxx else 0.0
    intercept = my - slope * mx
    return {"offset_s": round(-intercept, 6), "drift_ppm": round(-slope * 1e6, 1), "fit_points": n}


def estimate_clocks(rows):
    """One clock model per device: devices never share a clock."""
    return {dev: estimate_clock(drows) for dev, drows in by_device(rows).items()}


def corrected_time(row, clocks):
    """Map a device timestamp onto the ingestion timeline using that device's clock model."""
    c = clocks[row["device_id"]] if "offset_s" not in clocks else clocks
    return row["device_ts"] - c["offset_s"] - row["device_ts"] * c["drift_ppm"] * 1e-6


def late_events(rows, clocks, allowed_lateness_s=0.02):
    """Watermark check per device: an event is late if, when it arrives, its corrected
    event time is more than allowed_lateness_s behind the newest event already seen."""
    late = defaultdict(list)
    newest = {}
    for r in samples(rows):  # arrival order
        dev = r["device_id"]
        t = corrected_time(r, clocks)
        if dev in newest and t < newest[dev] - allowed_lateness_s:
            late[dev].append(r["seq"])
        newest[dev] = max(newest.get(dev, t), t)
    return {dev: late.get(dev, []) for dev in by_device(rows)}


def latency(rows, clocks):
    """Latency = ingest time - corrected device time. Uses only data a real system has."""
    lat = [r["ingest_ts"] - corrected_time(r, clocks) for r in samples(rows)]
    return {f"p{p}_ms": round(1000 * _percentile(lat, p), 2) for p in (50, 95, 99)}


def true_latency(rows):
    """Ground-truth latency (simulation only) to validate the estimate above."""
    lat = [r["ingest_ts"] - r["true_ts"] for r in samples(rows)]
    return {f"p{p}_ms": round(1000 * _percentile(lat, p), 2) for p in (50, 95, 99)}
