import os
import statistics
import math
import pytest
from indicators_engine.pipelines.adx import AdxCalc

# Debug controlado por env: ADX_TEST_DEBUG=1 para ver trazas
DEBUG = os.getenv("ADX_TEST_DEBUG") == "1"

def _log(msg: str):
    if DEBUG:
        print(msg)

def _throttled_bar_log(i: int, n: int, candle, out):
    """Imprime poco: primeras 3, cada 50 y últimas 2; solo cuando hay salida."""
    if not DEBUG or out is None:
        return
    if i <= 3 or i % 50 == 0 or i > n - 2:
        _log(f"[{i}/{n}] ADX={out['value']:.2f} +DI={out['plus_di']:.2f} -DI={out['minus_di']:.2f} "
             f"DX={out['dx']:.2f} ts={out.get('ts','-')} close={candle['close']}")

# ──────────────────────────────────────────────────────────────────────
# 1) Random walk (Wilder, period=14): alternancia +DI/–DI y ADX sano
# ──────────────────────────────────────────────────────────────────────

def test_adx_wilder_randomwalk_basics(make_candles_fn):
    symbol, tf = "ESZ5", "1m"
    n = 220
    period = 14
    adx = AdxCalc(period=period, method="wilder")

    candles = list(make_candles_fn(
        n=n, tf=tf, price0=648.0, amplitude=1.2,
        pattern="randomwalk", seed=42, symbol=symbol
    ))

    outs = []
    for i, c in enumerate(candles, 1):
        v = adx.on_bar(symbol, tf, int(c["ts"]),
                       float(c["high"]), float(c["low"]), float(c["close"]))
        if v is not None:
            outs.append(v)
        _throttled_bar_log(i, n, c, v)

    expected_min = n - (2 * period - 1) - 2
    assert len(outs) >= expected_min, f"Pocos ADX emitidos: {len(outs)} < {expected_min}"

    has_plus = any(o["plus_di"] > 1.0 for o in outs)
    has_minus = any(o["minus_di"] > 1.0 for o in outs)
    assert has_plus and has_minus, f"No alterna DI: +DI? {has_plus}  -DI? {has_minus}"

    adx_vals = [float(o["value"]) for o in outs]
    med = statistics.median(adx_vals)
    assert 5.0 <= med <= 70.0, f"Mediana ADX {med:.2f} fuera de rango razonable"

# ──────────────────────────────────────────────────────────────────────
# 2) Propiedades: cota [0,100] y timestamps no decrecientes si existen
# ──────────────────────────────────────────────────────────────────────

def test_adx_bounds_and_monotonic_ts(make_candles_fn):
    symbol, tf = "ESZ5", "1m"
    adx = AdxCalc(period=14, method="wilder")
    candles = list(make_candles_fn(
        n=120, tf=tf, price0=500.0, amplitude=1.0,
        pattern="randomwalk", seed=7, symbol=symbol
    ))

    outs = []
    for i, c in enumerate(candles, 1):
        v = adx.on_bar(symbol, tf, int(c["ts"]),
                       float(c["high"]), float(c["low"]), float(c["close"]))
        if v:
            outs.append(v)
        _throttled_bar_log(i, len(candles), c, v)

    assert outs, "No se emitió ningún ADX"

    prev_ts = -math.inf
    for o in outs:
        assert 0.0 <= o["plus_di"]  <= 100.0
        assert 0.0 <= o["minus_di"] <= 100.0
        assert 0.0 <= o["dx"]       <= 100.0
        assert 0.0 <= o["adx"]      <= 100.0
        if "ts" in o:
            assert int(o["ts"]) >= prev_ts
            prev_ts = int(o["ts"])

# ──────────────────────────────────────────────────────────────────────
# 3) Cambio de régimen: tendencia ➜ rango ➜ tendencia contraria
# ──────────────────────────────────────────────────────────────────────

def test_adx_regime_change(make_candles_fn):
    symbol, tf = "ESZ5", "1m"
