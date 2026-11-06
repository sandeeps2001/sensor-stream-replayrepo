"""Simulates a racket-mounted motion sensor streaming over an unreliable link.

Each event carries the device's own clock reading (device_ts), which starts at an
offset from true time and drifts at a fixed rate (ppm). The network adds random
delay, occasionally drops events, and can deliver them out of order. True send
time is kept only as ground truth for evaluation; a real pipeline never sees it.
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


def simulate(session_id="s1", device_id="racket-01", cfg=LinkConfig(), seed=42):
    """Return a list of (arrival_true_ts, SensorEvent) sorted by arrival time."""
    rng = random.Random(seed)
    n = int(cfg.rate_hz * cfg.duration_s)
    arrivals = []
    for seq in range(n):
        true_ts = seq / cfg.rate_hz
        device_ts = cfg.clock_offset_s + true_ts * (1 + cfg.clock_drift_ppm * 1e-6)
        swing = math.sin(2 * math.pi * 1.5 * true_ts)  # a 1.5 Hz swing pattern
        ev = SensorEvent(session_id, device_id, seq, round(device_ts, 6),
                         round(9.8 * swing, 4), round(2.0 * swing, 4), round(9.81, 4),
                         round(true_ts, 6))
        if rng.random() < cfg.drop_prob:
            continue  # lost in transit
        delay = cfg.base_delay_s + rng.random() * cfg.jitter_s
        if rng.random() < cfg.reorder_prob:
            delay += cfg.reorder_extra_delay_s
        arrivals.append((true_ts + delay, ev))
    arrivals.sort(key=lambda x: x[0])
    return arrivals
