# sensor-stream-replay

![tests](https://github.com/sandeeps2001/sensor-stream-replayrepo/actions/workflows/tests.yml/badge.svg)

A small, tested pipeline for **timestamped sensor streams from multiple devices**: simulate motion sensors over an unreliable link, ingest them (in memory or through Kafka), record versioned sessions, measure **loss, duplicates, reordering, late data, per-device clock drift and latency**, and **replay sessions deterministically in event-time order**.

## Quick start

```bash
python run_demo.py              # no dependencies, fully reproducible
python -m pytest -q             # 8 tests (pip install pytest)

# optional: run through real Kafka
docker compose up -d
pip install kafka-python
python run_demo.py --kafka
```

## 3D replay viewer (Three.js)

```bash
python export_viewer.py         # writes viewer/session.json
python -m http.server 8000      # from the repo root
# open http://localhost:8000/viewer/
```

Replays one racket's session in 3D, tilting with the accelerometer data, with play/pause, speed control, a scrubbable timeline, and red marks wherever packets were lost.

## How it works

```
simulate_fleet ──► transport (in-memory | Kafka) ──► ingest ──► data/<session>/<version>/
 one clock per       delay, drops, reordering,        stamps         events.jsonl + meta.json
 device + drift      duplicates, end markers          ingest_ts
                                                          │
                          metrics ◄───────────────────────┤
                          per-device loss, duplicates,    │
                          reorder, late data, clocks      ▼
                                                        replay (event-time order, deterministic)
```

| Module | What it does |
|---|---|
| `simulator.py` | 100 Hz accelerometer events per device. Each device has its own clock offset and drift (ppm). The link adds delay and jitter, drops ~1%, reorders ~2%, duplicates ~0.5%, and can cut the tail of a stream. Each device ends with an end-of-session marker carrying its final sequence number. |
| `transport.py` | In-memory transport, or Kafka keyed by device so each device's events stay ordered within a partition. |
| `ingest.py` | Stamps `ingest_ts` and writes `events.jsonl` + `meta.json`. The version ID is a hash of the content. |
| `metrics.py` | Per-device loss (from sequence numbers and the end marker), duplicates, reordering, watermark-based late-event detection, a per-device clock model, and latency percentiles. |
| `replay.py` | Orders by corrected event time using each device's own clock, then device and seq, drops duplicates, and produces a digest proving replays are identical. |

## Sample results (`python run_demo.py`, two devices)

| Measure | racket-01 | racket-02 |
|---|---|---|
| Lost (incl. tail) | 11 of 1,000 (1.1%) | 26 of 1,000 (2.6%) — includes 20 lost at the tail |
| Duplicates removed | 2 | 2 |
| Late events (20 ms watermark) | 7 | 12 |
| Clock drift: estimated / true | 215.2 / 200 ppm | −135.3 / −150 ppm |
| Clock offset: estimated / true | 0.235 / 0.250 s | −0.135 / −0.120 s |

Latency p50 / p95 / p99: estimated 5.0 / 9.5 / 33.0 ms, ground truth 20.2 / 24.7 / 48.1 ms. Replay: 1,963 events, identical digest on every run.

## Design decisions

- **Sequence numbers, not timestamps, measure loss.** A timestamp can't tell a dropped event from a slow one; a gap in sequence numbers can.
- **An end-of-session marker makes tail loss visible.** Gaps between the first and last *received* event miss anything lost at the end. The device's final sequence number, retried until delivered, closes that hole. A test shows exactly 20 lost events go unseen without it.
- **One clock model per device.** Devices drift differently, so a single fit across devices is wrong; a test shows a global fit misses one device's drift by more than 100 ppm while per-device fits stay within 50.
- **Clock model from the lower envelope.** Network delay is never negative, so the fastest arrivals in each window best reflect the true clock relationship.
- **Event time, not arrival time, for replay.** Late and reordered events land where they actually happened. Ordering by (corrected time, device, seq) with fixed rounding and duplicate removal makes replay deterministic.
- **Watermark for late data.** An event is late if it arrives more than an allowed lateness behind the newest event already seen from that device; late events are flagged, not dropped.

## Known limits

- **Offset vs. delay:** with one-way timestamps alone, a constant network delay is indistinguishable from clock offset, so estimated latency is ~15 ms below the truth. Fixing it needs two-way sync (NTP/PTP-style round trips) or a shared reference event, such as an impact seen by both the sensor and a camera.
- The clock model is linear; real clocks can wander with temperature, which would need a windowed or piecewise fit.
- If the end marker itself never arrives (device dies), tail loss falls back to the last received sequence number and can be undercounted.

## How this was built (AI-assisted)

I built this with AI coding tools. I specified the problem and the requirements (simulated clock drift, loss, reordering, duplicates, deterministic replay, measurable results), reviewed and ran every part on my own machine, and verified behavior with tests. After the first version, I reviewed my own design for weaknesses and added the fixes above: per-device clocks, the end-of-session marker for tail loss, duplicate handling, watermark-based late detection, and CI. The tests that demonstrate each failure mode (for example, the global clock fit being wrong) are there so the claims can be checked, not just read.
