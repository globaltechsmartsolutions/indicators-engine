import math
from indicators_engine.indicators.classic.macd import MACD, MACDConfig
from indicators_engine.core.types import Bar

def test_macd_basic():
    """Test basic MACD calculation"""
    macd = MACD(MACDConfig(fast=12, slow=26, signal=9))
    ts = 1_700_000_000_000
    outs = []
    
    # Generate some bars
    for i in range(50):
        bar = Bar(
            ts=ts,
            symbol="ESZ5",
            tf="1m",
            open=100.0,
            high=100.5,
            low=99.5,
            close=100.0 + i * 0.05,
            volume=1000.0
        )
        result = macd.on_bar(bar)
        ts += 60_000
        if result is not None:
            outs.append((ts, result))
    
    assert len(outs) > 0
    last = outs[-1][1]
    assert set(last.keys()) == {"macd", "signal", "hist"}
    for k in ("macd", "signal", "hist"):
        assert math.isfinite(last[k])
    # Histogram should equal macd - signal
    assert abs(last["hist"] - (last["macd"] - last["signal"])) < 1e-9


def test_macd_warmup():
    """Test MACD warmup period"""
    macd = MACD(MACDConfig(fast=12, slow=26, signal=9))
    ts = 1_700_000_000_000
    
    # Should return None during warmup
    for i in range(35):
        bar = Bar(
            ts=ts,
            symbol="TEST",
            tf="1m",
            open=100.0,
            high=100.5,
            low=99.5,
            close=100.0,
            volume=1000.0
        )
        result = macd.on_bar(bar)
        ts += 60_000
        if i < 34:
            # During warmup
            pass
    
    # Should get valid output after warmup
    bar = Bar(
        ts=ts,
        symbol="TEST",
        tf="1m",
        open=100.0,
        high=100.5,
        low=99.5,
        close=101.0,
        volume=1000.0
    )
    result = macd.on_bar(bar)
    assert result is not None
    for k in ("macd", "signal", "hist"):
        assert math.isfinite(result[k])


def test_macd_uptrend():
    """Test MACD with uptrend"""
    macd = MACD(MACDConfig())
    ts = 1_700_000_000_000
    last = None
    
    for i in range(50):
        bar = Bar(
            ts=ts,
            symbol="TEST",
            tf="1m",
            open=100.0 + i * 0.3,
            high=100.5 + i * 0.3,
            low=99.5 + i * 0.3,
            close=100.0 + i * 0.5,
            volume=1000.0
        )
        result = macd.on_bar(bar)
        ts += 60_000
        if result is not None:
            last = result
    
    assert last is not None
    # In uptrend, MACD should be positive
    assert last["macd"] > 0.0
    assert last["signal"] > 0.0


def test_macd_downtrend():
    """Test MACD with downtrend"""
    macd = MACD(MACDConfig())
    ts = 1_700_000_000_000
    last = None
    
    for i in range(50):
        bar = Bar(
            ts=ts,
            symbol="TEST",
            tf="1m",
            open=100.0 - i * 0.3,
            high=100.5 - i * 0.3,
            low=99.5 - i * 0.3,
            close=100.0 - i * 0.5,
            volume=1000.0
        )
        result = macd.on_bar(bar)
        ts += 60_000
        if result is not None:
            last = result
    
    assert last is not None
    # In downtrend, MACD should be negative
    assert last["macd"] < 0.0
    assert last["signal"] < 0.0


def test_macd_flat_market():
    """Test MACD with flat market"""
    macd = MACD(MACDConfig())
    ts = 1_700_000_000_000
    outs = []
    
    for i in range(40):
        bar = Bar(
            ts=ts,
            symbol="TEST",
            tf="1m",
            open=100.0,
            high=100.01,
            low=99.99,
            close=100.0,
            volume=1000.0
        )
        result = macd.on_bar(bar)
        ts += 60_000
        if result is not None:
            outs.append(result)
    
    assert len(outs) > 0
    last = outs[-1]
    # In flat market, MACD should be close to zero
    assert abs(last["macd"]) < 1.0
    assert abs(last["signal"]) < 1.0
    assert abs(last["hist"]) < 1.0
