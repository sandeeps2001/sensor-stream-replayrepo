"""Loss, reordering, clock drift, and latency measurement from a recording."""


def _percentile(values, p):
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def loss_and_reorder(rows):
    """Loss from gaps in sequence numbers; reorder = arrivals with a lower seq than one already seen."""
    seqs = [r["seq"] for r in rows]  # rows are in arrival order
    expected = max(seqs) - min(seqs) + 1
    received = len(set(seqs))
    reordered, highest = 0, -1
    for s in seqs:
        if s < highest:
            reordered += 1
        highest = max(highest, s)
    return {"expected": expected, "received": received, "lost": expected - received,
            "loss_pct": round(100 * (expected - received) / expected, 3),
            "reordered": reordered, "duplicates": len(seqs) - received}


def estimate_clock(rows, rate_hz=None):
    """Fit ingest_ts ~ a + b * device_ts using the earliest-arriving points.

    Network delay is always >= 0, so the lower envelope of (ingest - device) tracks
    the clock relationship. We fit on the minimum-delay point in each 1 s window.
    Returns offset (s) and drift (ppm) of the device clock relative to ingest time.
    """
    windows = {}
    for r in rows:
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
    # ingest - device = intercept + slope*device  ->  drift_ppm = -slope*1e6
    return {"offset_s": round(-intercept, 6), "drift_ppm": round(-slope * 1e6, 1),
            "fit_points": n}


def corrected_time(row, clock):
    """Map a device timestamp onto the ingestion timeline using the fitted clock model."""
    return row["device_ts"] - clock["offset_s"] - row["device_ts"] * clock["drift_ppm"] * 1e-6


def latency(rows, clock):
    """Latency = ingest time - corrected device time. Uses only data a real system has."""
    lat = [r["ingest_ts"] - corrected_time(r, clock) for r in rows]
    return {f"p{p}_ms": round(1000 * _percentile(lat, p), 2) for p in (50, 95, 99)}


def true_latency(rows):
    """Ground-truth latency (simulation only) to validate the estimate above."""
    lat = [r["ingest_ts"] - r["true_ts"] for r in rows]
    return {f"p{p}_ms": round(1000 * _percentile(lat, p), 2) for p in (50, 95, 99)}
