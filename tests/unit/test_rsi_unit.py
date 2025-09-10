from indicators_engine.pipelines.rsi import RsiCalc

def test_rsi_calc_basic():
    rsi = RsiCalc(period=14)
    closes = [100,100.1,100.2,100.2,100.0,100.05,100.1,100.15,100.2,100.1,100.05,100.0,99.95,100.0,100.05,100.1,100.15,100.2,100.25,100.3]
    vals = []
    ts = 1_700_000_000_000
    for c in closes:
        v = rsi.on_bar("ESZ5","1m",ts,c); ts += 60_000
        if v is not None: vals.append(v)
    assert len(vals) >= len(closes) - 14
    assert 0 <= vals[-1] <= 100
