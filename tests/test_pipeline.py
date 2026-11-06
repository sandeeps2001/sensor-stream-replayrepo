import os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sensor_replay.simulator import LinkConfig, simulate
from sensor_replay.transport import InMemoryTransport
from sensor_replay.ingest import ingest
from sensor_replay.replay import load, replay, replay_digest
from sensor_replay import metrics


def _record(cfg=LinkConfig(), seed=42):
    t = InMemoryTransport()
    for arrival, ev in simulate(cfg=cfg, seed=seed):
        t.send(ev.to_dict(), arrival)
    path = ingest(t, out_dir=tempfile.mkdtemp())
    return load(path)[0], path


def test_loss_is_detected_from_sequence_gaps():
    cfg = LinkConfig()
    rows, _ = _record(cfg)
    loss = metrics.loss_and_reorder(rows)
    true_sent = int(cfg.rate_hz * cfg.duration_s)
    assert loss["received"] == len(rows)
    assert loss["expected"] <= true_sent and loss["lost"] > 0


def test_reordering_is_detected():
    rows, _ = _record()
    assert metrics.loss_and_reorder(rows)["reordered"] > 0


def test_clock_estimate_close_to_truth():
    cfg = LinkConfig()
    rows, _ = _record(cfg)
    clock = metrics.estimate_clock(rows)
    # offset is recovered up to the minimum network delay; drift within 50 ppm
    assert abs(clock["drift_ppm"] - cfg.clock_drift_ppm) < 50
    assert abs(clock["offset_s"] - (cfg.clock_offset_s - cfg.base_delay_s)) < 0.01


def test_replay_is_deterministic_and_in_event_order():
    rows, _ = _record()
    a, b = replay(rows), replay(rows)
    assert replay_digest(a) == replay_digest(b)
    seqs = [e["seq"] for e in a]
    assert seqs == sorted(seqs)


def test_same_data_same_version():
    _, p1 = _record(seed=7)
    _, p2 = _record(seed=7)
    assert os.path.basename(p1) == os.path.basename(p2)
