from strategy.liquidity_pool_breaker import (
    classify_liquidity_event,
    detect_breach,
    select_liquid_contract,
)


def test_detect_breach_up_down():
    assert detect_breach(19990.0, 20010.0, 20000) == "up"
    assert detect_breach(20010.0, 19990.0, 20000) == "down"
    assert detect_breach(20000.0, 20000.0, 20000) is None


def test_classify_liquidity_event_paths():
    assert (
        classify_liquidity_event(
            oi_change_pct=-0.2,
            vol_delta=150,
            oi_drop_pct_trigger=0.15,
            absorption_oi_rise_pct=0.1,
            min_vol_delta=100,
        )
        == "stop_grab"
    )
    assert (
        classify_liquidity_event(
            oi_change_pct=0.12,
            vol_delta=150,
            oi_drop_pct_trigger=0.15,
            absorption_oi_rise_pct=0.1,
            min_vol_delta=100,
        )
        == "absorption"
    )
    assert (
        classify_liquidity_event(
            oi_change_pct=-0.05,
            vol_delta=150,
            oi_drop_pct_trigger=0.15,
            absorption_oi_rise_pct=0.1,
            min_vol_delta=100,
        )
        == "absorption"
    )
    assert (
        classify_liquidity_event(
            oi_change_pct=-0.2,
            vol_delta=50,
            oi_drop_pct_trigger=0.15,
            absorption_oi_rise_pct=0.1,
            min_vol_delta=100,
        )
        == "none"
    )


def test_select_liquid_contract_prefers_highest_score():
    chain = {
        "CE": {
            19900: {"oi": 10, "volume": 100},
            20000: {"oi": 200, "volume": 10},
        },
        "PE": {},
    }
    selected = select_liquid_contract(chain, [19900, 20000], "CE", min_oi=0, min_vol=0)
    assert selected is not None
    strike, entry = selected
    assert strike == 20000
    assert entry["oi"] == 200

    filtered = select_liquid_contract(chain, [19900, 20000], "CE", min_oi=250, min_vol=0)
    assert filtered is None
