# sensor-stream-replay

A small, testable pipeline for **timestamped sensor streams**: simulate a motion sensor over an unreliable link, ingest it (in memory or through Kafka), record versioned sessions, measure **loss, reordering, clock drift and latency**, and **replay sessions deterministically in event-time order**.

Built by Sandeep Saravanan as a self-contained demo of the problems behind sensor-driven feedback systems.

## Quick start

```bash
python run_demo.py              # no dependencies, fully reproducible
python -m pytest -q             # tests (pip install pytest)

# optional: run through real Kafka
docker compose up -d
pip install kafka-python
python run_demo.py --kafka
```

## How it works

```
simulator ──► transport (in-memory | Kafka) ──► ingest ──► data/<session>/<version>/
 device clock      network delay, drops,         stamps         events.jsonl + meta.json
 offset + drift    reordering                     ingest_ts
                                                      │
                               metrics ◄──────────────┤
                               loss, reorder,         │
                               clock fit, latency     ▼
                                                    replay (event-time order, deterministic)
```

| Module | What it does |
|---|---|
| `simulator.py` | 100 Hz accelerometer events. Each carries a `seq` number and a `device_ts` from a clock that starts 250 ms off and drifts 200 ppm. The link adds delay and jitter, drops ~1% of events, and reorders ~2%. |
| `transport.py` | In-memory transport (uses simulated arrival times as the ingestion clock) or Kafka (keyed by device so each device's events stay ordered within a partition). |
| `ingest.py` | Stamps `ingest_ts`, writes `events.jsonl` + `meta.json`. The version ID is a hash of the content, so identical data always gets the same version. |
| `metrics.py` | Loss from sequence gaps, reorder count, a clock model (offset + drift) fitted on the lower envelope of `ingest_ts - device_ts`, and latency percentiles. |
| `replay.py` | Sorts by corrected device time, then device and seq, drops duplicates, and produces a digest to prove replays are identical. |

## Sample results (`python run_demo.py`)

| Measure | Value |
|---|---|
| Loss | 12 of 1,000 events (1.2%), detected from sequence gaps |
| Reordered arrivals | 10 |
| Clock drift | estimated 180.6 ppm (true 200 ppm) |
| Clock offset | estimated 0.235 s (true 0.250 s) |
| Latency, estimated p50 / p95 / p99 | 5.1 / 9.6 / 12.8 ms |
| Latency, ground truth p50 / p95 / p99 | 20.3 / 24.6 / 27.8 ms |
| Replay | 988 events, identical digest across runs |

## Design decisions and limits

- **Event time, not arrival time.** Replay orders by corrected device time, so late or reordered events land where they actually happened.
- **Sequence numbers** are the most reliable loss signal. Timestamps alone can't tell a dropped event from a slow one.
- **Clock model from the lower envelope.** Network delay is never negative, so the fastest arrivals in each window best reflect the true clock relationship.
- **Known limit:** from one-way timestamps alone, a constant network delay is indistinguishable from clock offset. That's why estimated latency is ~15 ms below the true value: the minimum delay gets absorbed into the offset. Fixing it needs a two-way sync (NTP/PTP-style round trips) or a shared reference event, such as a known impact visible to both the sensor and a reference camera.
- **Determinism** comes from a total ordering key (time, device, seq), fixed rounding, and duplicate removal. Tests check identical digests.

## Next steps

Two-way clock sync to separate offset from delay; per-device watermarks for late data; Parquet storage; a replay view plotting measured vs. reference motion with timing error.
