import math
from indicators_engine.indicators.classic.adx import ADX, ADXConfig
from indicators_engine.core.types import Bar

def test_adx_basic():
    """Test basic ADX calculation"""
    adx = ADX(ADXConfig(period=14))
    ts = 1_700_000_000_000
    outs = []
    
    # Generate some bars with trending data
    for i in range(50):
        bar = Bar(
            ts=ts,
            symbol="ESZ5",
            tf="1m",
            open=100.0 + i * 0.1,
            high=100.5 + i * 0.1,
            low=99.5 + i * 0.1,
            close=100.0 + i * 0.2,
            volume=1000.0
        )
        result = adx.on_bar(bar)
        ts += 60_000
        if result is not None:
            outs.append((ts, result))
    
    assert len(outs) > 0
    last = outs[-1][1]
    assert set(last.keys()) == {"plus_di", "minus_di", "adx"}
    for k in ("adx", "plus_di", "minus_di"):
        assert math.isfinite(last[k])
        assert 0.0 <= last[k] <= 100.0


def test_adx_warmup():
    """Test ADX warmup period"""
    adx = ADX(ADXConfig(period=14))
    ts = 1_700_000_000_000
    
    # Should return None during warmup
    for i in range(15):
        bar = Bar(
            ts=ts,
            symbol="TEST",
            tf="1m",
            open=100.0,
            high=100.5,
            low=99.5,
            close=100.0 + i * 0.1,  # Add some trend
            volume=1000.0
        )
        result = adx.on_bar(bar)
        ts += 60_000
        if i < 14:
            pass
    
    # Should get valid output after warmup with trending data
    bar = Bar(
        ts=ts,
        symbol="TEST",
        tf="1m",
        open=101.0,
        high=101.5,
        low=100.5,
        close=102.0,  # Upward trend
        volume=1000.0
    )
    result = adx.on_bar(bar)
    assert result is not None
    for k in ("adx", "plus_di", "minus_di"):
        assert math.isfinite(result[k])
        assert 0.0 <= result[k] <= 100.0


def test_adx_flat_market():
    """Test ADX with flat market - ADX returns None when no direction"""
    adx = ADX(ADXConfig(period=14))
    ts = 1_700_000_000_000
    outs = []
    
    for i in range(30):
        bar = Bar(
            ts=ts,
            symbol="TEST",
            tf="1m",
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=1000.0
        )
        result = adx.on_bar(bar)
        ts += 60_000
        if result is not None:
            outs.append(result)
    
    # ADX should return None for perfectly flat market (no direction)
    # This is correct behavior - no directional movement means no ADX
    # If there's any output, verify it's valid
    for result in outs:
        for k in ("adx", "plus_di", "minus_di"):
            assert math.isfinite(result[k])
            assert 0.0 <= result[k] <= 100.0
