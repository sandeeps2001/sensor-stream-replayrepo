"""End-to-end demo: simulate -> ingest -> record -> measure -> replay.

    python run_demo.py            # in-memory transport, no dependencies
    python run_demo.py --kafka    # real Kafka (start it with: docker compose up -d)
"""
import argparse
import json

from sensor_replay.simulator import LinkConfig, simulate
from sensor_replay.transport import InMemoryTransport, KafkaTransport
from sensor_replay.ingest import ingest
from sensor_replay.replay import load, replay, replay_digest
from sensor_replay import metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", action="store_true")
    args = ap.parse_args()

    cfg = LinkConfig()
    arrivals = simulate(cfg=cfg)
    transport = KafkaTransport() if args.kafka else InMemoryTransport()
    for arrival_ts, ev in arrivals:
        transport.send(ev.to_dict(), arrival_ts)

    path = ingest(transport, session_meta={"rate_hz": cfg.rate_hz, "calibration": {"accel_scale": 1.0}})
    rows, meta = load(path)

    clock = metrics.estimate_clock(rows)
    report = {
        "recording": path,
        "loss": metrics.loss_and_reorder(rows),
        "clock_estimate": clock,
        "clock_truth": {"offset_s": cfg.clock_offset_s, "drift_ppm": cfg.clock_drift_ppm},
        "latency_estimated": metrics.latency(rows, clock),
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
