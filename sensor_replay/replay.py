"""Deterministic replay of a recorded session in event-time order.

Ordering key: (corrected device time, device_id, seq). Arrival order is ignored,
so late or reordered deliveries are replayed in the order they actually happened.
The same recording always yields the same output; replay_digest() proves it.
"""
import hashlib
import json
import os

from .metrics import corrected_time, estimate_clock


def load(path):
    with open(os.path.join(path, "events.jsonl")) as f:
        rows = [json.loads(line) for line in f if line.strip()]
    with open(os.path.join(path, "meta.json")) as f:
        meta = json.load(f)
    return rows, meta


def replay(rows, clock=None):
    clock = clock or estimate_clock(rows)
    seen, out = set(), []
    for r in sorted(rows, key=lambda r: (round(corrected_time(r, clock), 6), r["device_id"], r["seq"])):
        key = (r["device_id"], r["seq"])
        if key in seen:      # drop duplicate deliveries
            continue
        seen.add(key)
        out.append({"t": round(corrected_time(r, clock), 6), "device_id": r["device_id"],
                    "seq": r["seq"], "ax": r["ax"], "ay": r["ay"], "az": r["az"]})
    return out


def replay_digest(replayed):
    return hashlib.sha256(json.dumps(replayed, sort_keys=True).encode()).hexdigest()[:16]
