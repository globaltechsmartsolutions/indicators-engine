# tests/test_macd_calc.py
import math
from indicators_engine.pipelines.macd import MacdCalc

def test_macd_calc_basic():
    macd = MacdCalc(fast=12, slow=26, signal=9)
    closes = [
        100, 100.1, 100.2, 100.2, 100.0, 100.05, 100.1, 100.15, 100.2, 100.1,
        100.05, 100.0, 99.95, 100.0, 100.05, 100.1, 100.15, 100.2, 100.25, 100.3
    ]
    out = []
    ts = 1_700_000_000_000
    for c in closes:
        v = macd.on_bar("ESZ5", "1m", ts, c)
        ts += 60_000
        if v is not None:
            out.append(v)

    assert len(out) >= len(closes) - 3

    last = out[-1]
    assert set(last.keys()) == {"macd", "signal", "hist"}
    assert all(isinstance(last[k], float) and math.isfinite(last[k]) for k in last)
    # coherencia interna
    assert abs(last["hist"] - (last["macd"] - last["signal"])) < 1e-9


def test_macd_trend_sign_positive_on_uptrend():
    macd = MacdCalc()
    closes = [100 + i * 0.5 for i in range(60)]
    ts = 1_700_000_000_000
    last = None
    for c in closes:
        last = macd.on_bar("ESZ5", "1m", ts, c) or last
        ts += 60_000
    assert last is not None
    assert last["macd"] > 0.0
    assert last["signal"] > 0.0


def test_macd_trend_sign_negative_on_downtrend():
    macd = MacdCalc()
    closes = [100 - i * 0.5 for i in range(60)]
    ts = 1_700_000_000_000
    last = None
    for c in closes:
        last = macd.on_bar("ESZ5", "1m", ts, c) or last
        ts += 60_000
    assert last is not None
    assert last["macd"] < 0.0
    assert last["signal"] < 0.0


def test_macd_ignores_duplicate_or_out_of_order_ts():
    macd = MacdCalc()
    ts = 1_700_000_000_000

    for c in (100.0, 100.2, 100.4):
        macd.on_bar("ESZ5", "1m", ts, c)
        ts += 60_000

    v1 = macd.on_bar("ESZ5", "1m", ts, 100.6)
    assert v1 is not None
    v_dup = macd.on_bar("ESZ5", "1m", ts, 100.8)
    assert v_dup is None
    v_old = macd.on_bar("ESZ5", "1m", ts - 60_000, 100.9)
    assert v_old is None
