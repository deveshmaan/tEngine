from engine.sizing import SizingInputs, compute_qty, risk_fraction


def test_risk_fraction_normalizes_percent_and_fraction() -> None:
    assert risk_fraction(0.12) == 0.12
    assert abs(risk_fraction(12.0) - 0.12) < 1e-9


def test_compute_qty_respects_lot_and_caps() -> None:
    qty = compute_qty(SizingInputs(capital_base=100_000, risk_fraction=0.1, premium=100.0, lot_size=50))
    assert qty == 100  # 10k budget / 5k per-lot = 2 lots

    qty2 = compute_qty(SizingInputs(capital_base=100_000, risk_fraction=0.1, premium=100.0, lot_size=50, max_premium_per_trade=6_000))
    assert qty2 == 50

    qty3 = compute_qty(
        SizingInputs(
            capital_base=100_000,
            risk_fraction=0.2,
            premium=100.0,
            lot_size=50,
            portfolio_cap=6_000,
            portfolio_open_premium=0,
        )
    )
    assert qty3 == 50

