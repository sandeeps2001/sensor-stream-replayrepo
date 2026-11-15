"""Export a replayed session for the 3D viewer.

    python export_viewer.py        # writes viewer/session.json
Then open the viewer:
    python -m http.server 8000     # from the repo root
    http://localhost:8000/viewer/
"""
import json
import os

from sensor_replay.transport import InMemoryTransport
from sensor_replay.ingest import ingest
from sensor_replay.replay import load, replay
from sensor_replay import metrics
from run_demo import FLEET
from sensor_replay.simulator import simulate_fleet

DEVICE = "racket-01"  # the viewer animates one racket


def main():
    t = InMemoryTransport()
    for arrival, ev in simulate_fleet(FLEET):
        t.send(ev.to_dict(), arrival)
    rows, meta = load(ingest(t))
    clocks = metrics.estimate_clocks(rows)
    events = [e for e in replay(rows, clocks) if e["device_id"] == DEVICE]
    dev_rows = [r for r in rows if r["device_id"] == DEVICE]
    seqs = {e["seq"] for e in events}
    expected = metrics.loss_and_reorder(dev_rows)[DEVICE]["expected"]
    lost = [s for s in range(expected) if s not in seqs]
    out = {
        "session_id": meta["session_id"], "version": meta["version"], "device_id": DEVICE,
        "events": events, "lost_seqs": lost,
        "loss": metrics.loss_and_reorder(dev_rows)[DEVICE], "clock": clocks[DEVICE],
        "latency": metrics.latency(dev_rows, clocks),
    }
    os.makedirs("viewer", exist_ok=True)
    with open(os.path.join("viewer", "session.json"), "w") as f:
        json.dump(out, f)
    print(f"wrote viewer/session.json ({DEVICE}: {len(events)} events, {len(lost)} lost)")


if __name__ == "__main__":
    main()
