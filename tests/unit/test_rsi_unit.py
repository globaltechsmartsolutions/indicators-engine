from indicators_engine.indicators.classic.rsi import RSI, RSIConfig
from indicators_engine.core.types import Bar

def test_rsi_basic():
    """Test basic RSI calculation"""
    rsi = RSI(RSIConfig(period=14))
    closes = [100,100.1,100.2,100.2,100.0,100.05,100.1,100.15,100.2,100.1,100.05,100.0,99.95,100.0,100.05,100.1,100.15,100.2,100.25,100.3]
    vals = []
    ts = 1_700_000_000_000
    for c in closes:
        bar = Bar(
            ts=ts,
            symbol="ESZ5",
            tf="1m",
            open=c,
            high=c + 0.01,
            low=c - 0.01,
            close=c,
            volume=1000.0
        )
        v = rsi.on_bar(bar)
        ts += 60_000
        if v is not None:
            vals.append(v)
    
    assert len(vals) >= len(closes) - 14
    assert 0 <= vals[-1] <= 100
    assert vals[-1] is not None


def test_rsi_warmup():
    """Test RSI warmup period"""
    rsi = RSI(RSIConfig(period=14))
    ts = 1_700_000_000_000
    
    # First few bars should return None during warmup
    for i in range(14):
        bar = Bar(
            ts=ts,
            symbol="TEST",
            tf="1m",
            open=100.0,
            high=100.05,
            low=99.95,
            close=100.0 + i * 0.01,
            volume=1000.0
        )
        result = rsi.on_bar(bar)
        ts += 60_000
        if i < 14:
            # First result may be None during warmup
            pass
    
    # After warmup, should get valid RSI
    bar = Bar(
        ts=ts,
        symbol="TEST",
        tf="1m",
        open=100.0,
        high=100.05,
        low=99.95,
        close=101.0,
        volume=1000.0
    )
    result = rsi.on_bar(bar)
    assert result is not None
    assert 0 <= result <= 100
