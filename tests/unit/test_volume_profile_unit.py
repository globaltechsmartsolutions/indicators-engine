from indicators_engine.indicators.volume.volume_profile import VolumeProfile, VolumeProfileConfig
from indicators_engine.core.types import Bar

def _print_snapshot(tag, snap):
    bins_str = ", ".join([f"{b[0]}: {b[1]}" for b in snap["bins"]])
    print(f"[{tag}] VTOTAL={snap['total_v']}  BINS={{ {bins_str} }}")

def test_vp_basic_accumulation():
    """Test Volume Profile basic accumulation"""
    print("\n[TEST] Volume Profile basic accumulation")
    vp = VolumeProfile(VolumeProfileConfig(tick_size=0.25))
    
    sym = "ESZ5"
    ts0 = 1_700_000_000_000
    
    # Bar 1: price 100.00, volume 2
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
    vp.on_bar(bar1)
    snap1 = vp.snapshot()
    _print_snapshot("AFTER#1", snap1)
    assert snap1["total_v"] == 2.0
    
    # Bar 2: price 100.25, volume 1
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
    vp.on_bar(bar2)
    snap2 = vp.snapshot()
    _print_snapshot("AFTER#2", snap2)
    assert snap2["total_v"] == 3.0
    
    # Bar 3: another 100.25, volume 2
    bar3 = Bar(
        ts=ts0 + 120_000,
        symbol=sym,
        tf="1m",
        open=100.25,
        high=100.3,
        low=100.2,
        close=100.25,
        volume=2.0
    )
    vp.on_bar(bar3)
    snap3 = vp.snapshot()
    _print_snapshot("AFTER#3", snap3)
    assert snap3["total_v"] == 5.0
    
    # Check bins
    bins_dict = dict(snap3["bins"])
    assert bins_dict.get(100.0, 0) == 2.0  # volume at 100.0
    assert bins_dict.get(100.25, 0) == 3.0  # volume at 100.25

def test_vp_top_n():
    """Test Volume Profile snapshot_top method"""
    vp = VolumeProfile(VolumeProfileConfig(tick_size=0.25, top_n=2))
    
    sym = "TEST"
    ts0 = 1_700_000_000_000
    
    # Add multiple bars with different prices
    for i, price in enumerate([100.0, 100.25, 100.5, 100.75]):
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
        vp.on_bar(bar)
    
    top = vp.snapshot_top(n=2)
    assert len(top) <= 2
    assert len(top) > 0
    
    # Top should be sorted by volume descending
    if len(top) > 1:
        assert top[0][1] >= top[1][1], "Top levels should be sorted by volume"

def test_vp_empty():
    """Test Volume Profile with no data"""
    vp = VolumeProfile(VolumeProfileConfig())
    snap = vp.snapshot()
    assert snap["bins"] == []
    assert snap["total_v"] == 0

def test_vp_bar_mode_close():
    """Test Volume Profile with close mode"""
    vp = VolumeProfile(VolumeProfileConfig(bar_mode="close"))
    
    bar = Bar(
        ts=1_700_000_000_000,
        symbol="TEST",
        tf="1m",
        open=99.0,
        high=101.0,
        low=98.0,
        close=100.0,
        volume=10.0
    )
    vp.on_bar(bar)
    snap = vp.snapshot()
    # In close mode, all volume goes to close price bin
    bins_dict = dict(snap["bins"])
    assert bins_dict[100.0] == 10.0
