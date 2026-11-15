"""Export a replayed session for the 3D viewer.

    py export_viewer.py        # writes viewer/session.json
Then open the viewer:
    py -m http.server 8000     # from the repo root
    http://localhost:8000/viewer/
"""
import json
import os

from sensor_replay.simulator import LinkConfig, simulate
from sensor_replay.transport import InMemoryTransport
from sensor_replay.ingest import ingest
from sensor_replay.replay import load, replay
from sensor_replay import metrics


def main():
    t = InMemoryTransport()
    for arrival, ev in simulate(cfg=LinkConfig()):
        t.send(ev.to_dict(), arrival)
    rows, meta = load(ingest(t))
    clock = metrics.estimate_clock(rows)
    events = replay(rows, clock)
    seqs = {e["seq"] for e in events}
    lost = [s for s in range(min(seqs), max(seqs) + 1) if s not in seqs]
    out = {
        "session_id": meta["session_id"], "version": meta["version"],
        "events": events, "lost_seqs": lost,
        "loss": metrics.loss_and_reorder(rows), "clock": clock,
        "latency": metrics.latency(rows, clock),
    }
    os.makedirs("viewer", exist_ok=True)
    with open(os.path.join("viewer", "session.json"), "w") as f:
        json.dump(out, f)
    print(f"wrote viewer/session.json ({len(events)} events, {len(lost)} lost)")


if __name__ == "__main__":
    main()
