"""Ingest events, stamp ingestion time, and write a versioned session recording.

Output per session:
  data/<session_id>/<version>/events.jsonl   one event per line, in arrival order
  data/<session_id>/<version>/meta.json      session metadata + content hash
The version is a short hash of the event content, so identical data always maps
to the same version and any change produces a new one.
"""
import hashlib
import json
import os


def ingest(transport, out_dir="data", session_meta=None):
    rows = []
    for ev, ingest_ts in transport.consume():
        row = dict(ev)
        row["ingest_ts"] = round(ingest_ts, 6)
        rows.append(row)
    if not rows:
        raise ValueError("no events received")
    payload = "\n".join(json.dumps(r, sort_keys=True) for r in rows)
    version = hashlib.sha256(payload.encode()).hexdigest()[:12]
    session_id = rows[0]["session_id"]
    path = os.path.join(out_dir, session_id, version)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "events.jsonl"), "w") as f:
        f.write(payload + "\n")
    meta = {"session_id": session_id, "version": version, "event_count": len(rows),
            "device_ids": sorted({r["device_id"] for r in rows})}
    meta.update(session_meta or {})
    with open(os.path.join(path, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    return path
