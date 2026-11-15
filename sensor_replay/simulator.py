"""Simulates racket-mounted motion sensors streaming over an unreliable link.

Each device has its own clock: it starts at an offset from true time and drifts
at its own rate (ppm). The network adds random delay, drops events, reorders
some, and occasionally delivers duplicates. When a session ends, each device
sends an end marker carrying its final sequence number; the marker is retried
until delivered, so the receiver can count losses at the tail of the stream.
True send time is kept only as ground truth for evaluation.
"""
from dataclasses import dataclass, asdict
import math
import random


@dataclass
class SensorEvent:
    session_id: str
    device_id: str
    seq: int              # monotonically increasing per device; used to detect loss/reorder
    device_ts: float      # seconds, from the device's (imperfect) clock
    ax: float
    ay: float
    az: float
    true_ts: float        # ground truth only, for evaluation
    kind: str = "sample"  # "sample" or "end" (end-of-session marker)

    def to_dict(self):
        return asdict(self)


@dataclass
class LinkConfig:
    rate_hz: float = 100.0
    duration_s: float = 10.0
    clock_offset_s: float = 0.250     # device clock starts 250 ms ahead of true time
    clock_drift_ppm: float = 200.0    # device clock runs 200 ppm fast
    base_delay_s: float = 0.015
    jitter_s: float = 0.010
    drop_prob: float = 0.01
    reorder_prob: float = 0.02
    reorder_extra_delay_s: float = 0.030
    dup_prob: float = 0.005
    tail_drop: int = 0                # drop the last N samples (models a link cut at the end)


def _device_clock(true_ts, cfg):
    return cfg.clock_offset_s + true_ts * (1 + cfg.clock_drift_ppm * 1e-6)


def simulate(session_id="s1", device_id="racket-01", cfg=LinkConfig(), seed=42):
    """Return a list of (arrival_true_ts, SensorEvent) for one device, sorted by arrival."""
    rng = random.Random(f"{seed}-{device_id}")
    n = int(cfg.rate_hz * cfg.duration_s)
    arrivals = []
    for seq in range(n):
        true_ts = seq / cfg.rate_hz
        swing = math.sin(2 * math.pi * 1.5 * true_ts)  # a 1.5 Hz swing pattern
        ev = SensorEvent(session_id, device_id, seq, round(_device_clock(true_ts, cfg), 6),
                         round(9.8 * swing, 4), round(2.0 * swing, 4), round(9.81, 4),
                         round(true_ts, 6))
        if seq >= n - cfg.tail_drop or rng.random() < cfg.drop_prob:
            continue  # lost in transit
        delay = cfg.base_delay_s + rng.random() * cfg.jitter_s
        if rng.random() < cfg.reorder_prob:
            delay += cfg.reorder_extra_delay_s
        arrivals.append((true_ts + delay, ev))
        if rng.random() < cfg.dup_prob:  # retransmission: same event arrives twice
            arrivals.append((true_ts + delay + cfg.base_delay_s, ev))
    # end-of-session marker, retried until delivered
    end_true = n / cfg.rate_hz
    end = SensorEvent(session_id, device_id, n - 1, round(_device_clock(end_true, cfg), 6),
                      0.0, 0.0, 0.0, round(end_true, 6), kind="end")
    arrivals.append((end_true + cfg.base_delay_s * 3, end))
    arrivals.sort(key=lambda x: x[0])
    return arrivals


def simulate_fleet(devices, session_id="s1", seed=42):
    """Simulate several devices, each with its own clock and link, merged by arrival time.

    devices: dict of device_id -> LinkConfig
    """
    merged = []
    for device_id, cfg in devices.items():
        merged.extend(simulate(session_id, device_id, cfg, seed))
    merged.sort(key=lambda x: x[0])
    return merged
