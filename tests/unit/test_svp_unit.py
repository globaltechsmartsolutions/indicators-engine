from indicators_engine.indicators.volume.svp import SVP, SVPConfig
from indicators_engine.core.types import Trade, Bar
from datetime import datetime, timezone

def session_key_utc_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")

def _print_snapshot(tag, snap):
    bins_str = ", ".join([f"{b[0]}: {b[1]}" for b in snap["bins"]])
    print(f"[{tag}] POC={snap['poc']}  VTOTAL={snap['total_v']}  BINS={{ {bins_str} }}")

def test_svp_basic_accumulation():
    """Test SVP basic accumulation with bars"""
    print("\n[TEST] SVP basic accumulation")
    svp = SVP(SVPConfig(
        session_key_fn=session_key_utc_day,
        tick_size=0.25
    ))
    
    sym = "ESZ5"
    ts0 = 1_700_000_000_000
    
    # Bar 1: 100.00 with volume 2
    bar1 = Bar(
        ts=ts0,
        symbol=sym,
        tf="1m",
        open=100.0,
        high=100.1,
        low=99.9,
        close=100.0,
        volume=2.0
    )
    result1 = svp.on_bar(bar1)
    snap1 = svp.snapshot(symbol=sym)
    _print_snapshot("AFTER#1", snap1)
    assert snap1["total_v"] == 2.0, "total_v after bar#1 should be 2.0"
    assert snap1["poc"] is not None
    poc_price1, poc_vol1 = snap1["poc"]
    assert poc_price1 == 100.0
    
    # Bar 2: 100.25 with volume 1
    bar2 = Bar(
        ts=ts0 + 60_000,
        symbol=sym,
        tf="1m",
        open=100.25,
        high=100.3,
        low=100.2,
        close=100.25,
        volume=1.0
    )
    result2 = svp.on_bar(bar2)
    snap2 = svp.snapshot(symbol=sym)
    _print_snapshot("AFTER#2", snap2)
    assert snap2["total_v"] == 3.0, "total_v after bar#2 should be 3.0"
    poc_price2, poc_vol2 = snap2["poc"]
    assert poc_price2 == 100.0 or poc_price2 == 100.25  # Either could be POC
    
    # Bar 3: Another 100.25
    bar3 = Bar(
        ts=ts0 + 120_000,
        symbol=sym,
        tf="1m",
        open=100.25,
        high=100.3,
        low=100.2,
        close=100.25,
        volume=1.0
    )
    result3 = svp.on_bar(bar3)
    snap3 = svp.snapshot(symbol=sym)
    _print_snapshot("AFTER#3", snap3)
    assert snap3["total_v"] == 4.0, "total_v after bar#3 should be 4.0"
    poc_price3, poc_vol3 = snap3["poc"]
    assert poc_price3 in [100.0, 100.25]

def test_svp_top_n():
    """Test SVP snapshot_top method"""
    svp = SVP(SVPConfig(
        session_key_fn=session_key_utc_day,
        tick_size=0.25,
        top_n=2
    ))
    
    sym = "TEST"
    ts0 = 1_700_000_000_000
    
    # Add multiple bars
    for i, price in enumerate([100.0, 100.25, 100.5, 100.75, 100.0]):
        bar = Bar(
            ts=ts0 + i * 60_000,
            symbol=sym,
            tf="1m",
            open=price,
            high=price + 0.1,
            low=price - 0.1,
            close=price,
            volume=1.0 + i
        )
        svp.on_bar(bar)
    
    top = svp.snapshot_top(n=2, symbol=sym)
    assert len(top) <= 2, "Should return at most 2 top levels"
    assert len(top) > 0, "Should have at least one top level"

def test_svp_empty():
    """Test SVP with no data"""
    svp = SVP(SVPConfig(session_key_fn=session_key_utc_day, tick_size=0.25))
    snap = svp.snapshot(symbol="EMPTY")
    assert snap["symbol"] == "EMPTY"
    assert snap["bins"] == []
    assert snap["total_v"] == 0.0
    assert snap["poc"] is None
