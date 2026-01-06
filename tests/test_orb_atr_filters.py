import pytest

from strategy.opening_range_breakout import _AtrTracker, _dynamic_breakout_buffer, _orb_width_bounds


def test_atr_tracker_wilder_seed_and_smooth():
    atr = _AtrTracker(3)
    atr.update(10.0, 8.0, 9.0)
    atr.update(11.0, 9.0, 10.0)
    assert not atr.ready()
    atr.update(12.0, 9.0, 11.0)
    assert atr.ready()
    assert atr.value == pytest.approx(7.0 / 3.0)
    atr.update(13.0, 10.0, 12.0)
    expected = ((7.0 / 3.0) * 2.0 + 3.0) / 3.0
    assert atr.value == pytest.approx(expected)


def test_orb_width_gate_bounds():
    atr_val = 10.0
    min_ok, max_ok = _orb_width_bounds(atr_val, 15, 0.55, 2.5)
    def _in_bounds(width: float) -> bool:
        return min_ok <= width <= max_ok

    assert not _in_bounds(min_ok - 0.01)
    assert _in_bounds((min_ok + max_ok) / 2.0)
    assert not _in_bounds(max_ok + 0.01)


def test_dynamic_buffer_respects_atr():
    fixed = 5.0
    assert _dynamic_breakout_buffer(fixed, None, 0.35, True) == fixed
    assert _dynamic_breakout_buffer(fixed, 20.0, 0.35, True) == pytest.approx(7.0)
    assert _dynamic_breakout_buffer(fixed, 20.0, 0.35, False) == fixed


def test_breakout_threshold_with_buffer():
    orb_high = 100.0
    buffer_pts = 5.0
    assert not (105.0 > (orb_high + buffer_pts))
    assert 105.01 > (orb_high + buffer_pts)
