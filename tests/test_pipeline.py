import os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sensor_replay.simulator import LinkConfig, simulate, simulate_fleet
from sensor_replay.transport import InMemoryTransport
from sensor_replay.ingest import ingest
from sensor_replay.replay import load, replay, replay_digest
from sensor_replay import metrics

FLEET = {
    "racket-01": LinkConfig(clock_offset_s=0.250, clock_drift_ppm=200.0),
    "racket-02": LinkConfig(clock_offset_s=-0.120, clock_drift_ppm=-150.0, tail_drop=20),
}


def _record(arrivals):
    t = InMemoryTransport()
    for arrival, ev in arrivals:
        t.send(ev.to_dict(), arrival)
    path = ingest(t, out_dir=tempfile.mkdtemp())
    return load(path)[0], path


def test_per_device_clock_estimates_close_to_truth():
    rows, _ = _record(simulate_fleet(FLEET))
    clocks = metrics.estimate_clocks(rows)
    for dev, cfg in FLEET.items():
        assert abs(clocks[dev]["drift_ppm"] - cfg.clock_drift_ppm) < 50
        # offset is recovered up to the minimum network delay
        assert abs(clocks[dev]["offset_s"] - (cfg.clock_offset_s - cfg.base_delay_s)) < 0.01


def test_one_global_clock_fit_is_wrong_for_mixed_devices():
    rows, _ = _record(simulate_fleet(FLEET))
    global_fit = metrics.estimate_clock(rows)
    per_device = metrics.estimate_clocks(rows)
    # a single model cannot match both devices' drift (+200 and -150 ppm)
    errors = [abs(global_fit["drift_ppm"] - FLEET[d].clock_drift_ppm) for d in FLEET]
    assert max(errors) > 100
    assert all(abs(per_device[d]["drift_ppm"] - FLEET[d].clock_drift_ppm) < 50 for d in FLEET)


def test_tail_loss_detected_with_end_marker():
    rows, _ = _record(simulate_fleet(FLEET))
    loss = metrics.loss_and_reorder(rows)
    assert loss["racket-02"]["end_marker"]
    assert loss["racket-02"]["lost"] >= 20          # includes the 20 dropped at the tail


def test_tail_loss_missed_without_end_marker():
    rows, _ = _record(simulate_fleet(FLEET))
    no_marker = [r for r in rows if r.get("kind") != "end"]
    with_marker = metrics.loss_and_reorder(rows)["racket-02"]["lost"]
    without = metrics.loss_and_reorder(no_marker)["racket-02"]["lost"]
    assert with_marker - without == 20               # exactly the tail gap goes unseen


def test_duplicates_counted_and_removed_in_replay():
    cfg = LinkConfig(dup_prob=0.05)
    rows, _ = _record(simulate(cfg=cfg))
    loss = metrics.loss_and_reorder(rows)["racket-01"]
    assert loss["duplicates"] > 0
    out = replay(rows)
    keys = [(e["device_id"], e["seq"]) for e in out]
    assert len(keys) == len(set(keys)) == loss["received"]


def test_late_events_flagged_by_watermark():
    cfg = LinkConfig(reorder_prob=0.05, reorder_extra_delay_s=0.200)
    rows, _ = _record(simulate(cfg=cfg))
    clocks = metrics.estimate_clocks(rows)
    assert len(metrics.late_events(rows, clocks, allowed_lateness_s=0.05)["racket-01"]) > 0


def test_replay_is_deterministic_and_in_event_order():
    rows, _ = _record(simulate_fleet(FLEET))
    a, b = replay(rows), replay(rows)
    assert replay_digest(a) == replay_digest(b)
    times = [e["t"] for e in a]
    assert times == sorted(times)


def test_same_data_same_version():
    _, p1 = _record(simulate_fleet(FLEET, seed=7))
    _, p2 = _record(simulate_fleet(FLEET, seed=7))
    assert os.path.basename(p1) == os.path.basename(p2)
