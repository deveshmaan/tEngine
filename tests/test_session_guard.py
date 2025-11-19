import datetime as dt

from engine.config import IST, RiskLimits
from engine.risk import SessionGuard


def _time(value: str) -> dt.datetime:
    hour, minute, second = [int(part) for part in value.split(":")]
    return dt.datetime(2024, 7, 1, hour, minute, second, tzinfo=IST)


def _limits(post_close_behavior: str = "halt_if_flat") -> RiskLimits:
    return RiskLimits(
        daily_pnl_stop=1000,
        per_symbol_loss_stop=600,
        max_open_lots=5,
        notional_premium_cap=50000,
        max_order_rate=10,
        no_new_entries_after=dt.time(15, 10),
        square_off_by=dt.time(15, 22),
        post_close_behavior=post_close_behavior,
    )


def test_session_guard_preopen_continue():
    guard = SessionGuard(_limits())
    result = guard.evaluate_boot_state(_time("09:00:00"), has_positions=False)
    assert result == {"action": "continue", "reason": "PREOPEN"}


def test_session_guard_live_continue():
    guard = SessionGuard(_limits())
    result = guard.evaluate_boot_state(_time("15:05:00"), has_positions=False)
    assert result == {"action": "continue", "reason": "LIVE"}


def test_session_guard_after_cutoff_halt():
    guard = SessionGuard(_limits())
    result = guard.evaluate_boot_state(_time("15:12:00"), has_positions=False)
    assert result == {"action": "halt", "reason": "AFTER_CUTOFF"}


def test_session_guard_post_close_with_positions():
    guard = SessionGuard(_limits())
    result = guard.evaluate_boot_state(_time("15:25:00"), has_positions=True)
    assert result == {"action": "squareoff_then_halt", "reason": "POST_CLOSE"}


def test_session_guard_post_close_flat_default_halt():
    guard = SessionGuard(_limits(post_close_behavior="halt_if_flat"))
    result = guard.evaluate_boot_state(_time("15:25:00"), has_positions=False)
    assert result == {"action": "halt", "reason": "POST_CLOSE"}


def test_session_guard_post_close_shutdown_behavior():
    guard = SessionGuard(_limits(post_close_behavior="shutdown"))
    result = guard.evaluate_boot_state(_time("15:25:00"), has_positions=False)
    assert result == {"action": "shutdown", "reason": "POST_CLOSE"}
