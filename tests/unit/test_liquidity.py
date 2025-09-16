from indicators_engine.pipelines.liquidity import LiquidityState

def test_liquidity_snapshot_and_imbalance():
    st = LiquidityState("ESZ5", depth_levels=2)
    st.apply_snapshot({
        "eventSymbol": "ESZ5",
        "time": 1726500000000000000,
        "bids": [[4999.50, 5], [4999.25, 2], [4999.00, 1]],
        "asks": [[5000.00, 3], [5000.25, 4], [5000.50, 2]],
    })
    out = st.snapshot()
    # depth_levels=2 ⇒ bids=5+2=7, asks=3+4=7 ⇒ imbalance=0
    assert out["ts"] == 1726500000000
    assert out["bids_depth"] == 7.0
    assert out["asks_depth"] == 7.0
    assert abs(out["depth_imbalance"]) < 1e-12
    # top-of-book
    assert out["best_bid"] == 4999.5 and out["bid1_size"] == 5.0
    assert out["best_ask"] == 5000.0 and out["ask1_size"] == 3.0

def test_liquidity_updates_change_depth_and_top_imb():
    st = LiquidityState("ESZ5", depth_levels=1)
    st.apply_snapshot({
        "eventSymbol": "ESZ5",
        "time": 1726500000000000000,
        "bids": [[4999.50, 5]],
        "asks": [[5000.00, 3]],
    })
    # Update: sube ask1 a 10 → depth_imbalance (con depth_levels=1) pasa a negativo
    st.apply_update({"eventSymbol":"ESZ5","side":"ask","price":5000.00,"size":10,"time":1726500000500000000})
    out = st.snapshot()
    assert out["ts"] == 1726500000500
    assert out["bids_depth"] == 5.0
    assert out["asks_depth"] == 10.0
    denom = 5.0 + 10.0
    exp_imb = (5.0 - 10.0)/denom
    assert abs(out["depth_imbalance"] - exp_imb) < 1e-12
    # top-of-book imbalance = (5 - 10)/15
    assert abs(out["top_imbalance"] - (-5.0/15.0)) < 1e-12
