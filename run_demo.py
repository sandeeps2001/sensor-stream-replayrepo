"""End-to-end demo: simulate a fleet -> ingest -> record -> measure -> replay.

    python run_demo.py            # in-memory transport, no dependencies
    python run_demo.py --kafka    # real Kafka (start it with: docker compose up -d)
"""
import argparse
import json

from sensor_replay.simulator import LinkConfig, simulate_fleet
from sensor_replay.transport import InMemoryTransport, KafkaTransport
from sensor_replay.ingest import ingest
from sensor_replay.replay import load, replay, replay_digest
from sensor_replay import metrics

# two rackets whose clocks disagree in different ways; racket-02 also loses its last 20 samples
FLEET = {
    "racket-01": LinkConfig(clock_offset_s=0.250, clock_drift_ppm=200.0),
    "racket-02": LinkConfig(clock_offset_s=-0.120, clock_drift_ppm=-150.0, tail_drop=20),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", action="store_true")
    args = ap.parse_args()

    transport = KafkaTransport() if args.kafka else InMemoryTransport()
    for arrival_ts, ev in simulate_fleet(FLEET):
        transport.send(ev.to_dict(), arrival_ts)

    path = ingest(transport, session_meta={"rate_hz": 100.0, "calibration": {"accel_scale": 1.0}})
    rows, _ = load(path)

    clocks = metrics.estimate_clocks(rows)
    report = {
        "recording": path,
        "loss": metrics.loss_and_reorder(rows),
        "late_events": {d: len(v) for d, v in metrics.late_events(rows, clocks).items()},
        "clock_estimate": clocks,
        "clock_truth": {d: {"offset_s": c.clock_offset_s, "drift_ppm": c.clock_drift_ppm} for d, c in FLEET.items()},
        "latency_estimated": metrics.latency(rows, clocks),
    }
    if not args.kafka:
        report["latency_true"] = metrics.true_latency(rows)
    out1, out2 = replay(rows), replay(rows)
    report["replay"] = {"events": len(out1), "digest_run1": replay_digest(out1),
                        "digest_run2": replay_digest(out2),
                        "deterministic": replay_digest(out1) == replay_digest(out2)}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
