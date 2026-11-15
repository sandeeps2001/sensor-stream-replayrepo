"""Deterministic replay of a recorded session in event-time order.

Ordering key: (corrected device time, device_id, seq), using each device's own
clock model. Arrival order is ignored, so late or reordered deliveries are
replayed in the order they actually happened. Duplicate deliveries are dropped.
The same recording always yields the same output; replay_digest() proves it.
"""
import hashlib
import json
import os

from .metrics import corrected_time, estimate_clocks, samples


def load(path):
    with open(os.path.join(path, "events.jsonl")) as f:
        rows = [json.loads(line) for line in f if line.strip()]
    with open(os.path.join(path, "meta.json")) as f:
        meta = json.load(f)
    return rows, meta


def replay(rows, clocks=None):
    clocks = clocks or estimate_clocks(rows)
    seen, out = set(), []
    keyed = sorted(samples(rows), key=lambda r: (round(corrected_time(r, clocks), 6), r["device_id"], r["seq"]))
    for r in keyed:
        key = (r["device_id"], r["seq"])
        if key in seen:
            continue
        seen.add(key)
        out.append({"t": round(corrected_time(r, clocks), 6), "device_id": r["device_id"],
                    "seq": r["seq"], "ax": r["ax"], "ay": r["ay"], "az": r["az"]})
    return out


def replay_digest(replayed):
    return hashlib.sha256(json.dumps(replayed, sort_keys=True).encode()).hexdigest()[:16]
