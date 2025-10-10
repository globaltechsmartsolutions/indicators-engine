import math
from indicators_engine.indicators.classic.adx import AdxCalc

# ---------------- Helpers ----------------
def _get(v, k, default=None):
    if isinstance(v, dict):
        return v.get(k, default)
    return getattr(v, k, default)

def _bar_args(v):
    return (
        _get(v, "ts"),
        float(_get(v, "high")),
        float(_get(v, "low")),
        float(_get(v, "close")),
    )

def _feed(adx: AdxCalc, symbol: str, tf: str, candles):
    outs = []
    for v in candles:
        ts, hi, lo, cl = _bar_args(v)
        out = adx.on_bar(symbol, tf, ts, hi, lo, cl)
        if out is not None:
            outs.append((ts, out))
    return outs

def _mk_params(pytestconfig, *, seed, n, symbol, tf):
    """Lee parámetros de pytest.ini con valores por defecto si no existen."""
    def ini(name, default=None):
        try:
            val = pytestconfig.getini(name)
            return default if (val is None or val == "") else val
        except (ValueError, KeyError):
            return default

    price0 = float(ini("candle_price", 100.0))
    amplitude = float(ini("candle_amplitude", 1.0))
    pattern = ini("candle_pattern", "randomwalk")
    return dict(symbol=symbol, tf=tf, n=n, seed=seed,
                price0=price0, amplitude=amplitude, pattern=pattern)

# ---------------- Tests ----------------
def test_adx_wilder_randomwalk_basics(pytestconfig, make_candles_fn):
    symbol, tf = "ESZ5", "1m"
    period = 14
    n = 220

    adx = AdxCalc(period=period)

    params = _mk_params(pytestconfig, seed=42, n=n, symbol=symbol, tf=tf)
    candles = list(make_candles_fn(**params))
    outs = _feed(adx, symbol, tf, candles)

    # Warm-up: tu implementación empieza a emitir tras period+1
    assert len(outs) >= max(0, n - (period + 1))

    last_ts, last = outs[-1]
    assert set(last.keys()) == {"adx", "pdi", "mdi"}
    for k in ("adx", "pdi", "mdi"):
        v = last[k]
        assert math.isfinite(v)
        assert 0.0 <= v <= 100.0


def test_adx_bounds_and_monotonic_ts(pytestconfig, make_candles_fn):
    symbol, tf = "ESZ5", "1m"
    adx = AdxCalc(period=14)

    params = _mk_params(pytestconfig, seed=123, n=80, symbol=symbol, tf=tf)
    candles = list(make_candles_fn(**params))

    # Alimentamos 40 barras para garantizar outputs
    outs = _feed(adx, symbol, tf, candles[:40])
    assert len(outs) > 0
    last_ts_ok = outs[-1][0]

    # Enviar una barra con ts anterior o igual debe devolver None
    v_old = candles[5]  # claramente anterior a last_ts_ok
    ts, hi, lo, cl = _bar_args(v_old)
    assert ts < last_ts_ok
    assert adx.on_bar(symbol, tf, ts, hi, lo, cl) is None

    # Con un ts mayor vuelve a producir salida
    v_new = candles[41]
    ts2, hi2, lo2, cl2 = _bar_args(v_new)
    assert ts2 > last_ts_ok
    out2 = adx.on_bar(symbol, tf, ts2, hi2, lo2, cl2)
    assert out2 is not None
    for k in ("adx", "pdi", "mdi"):
        assert 0.0 <= out2[k] <= 100.0
        assert math.isfinite(out2[k])


def test_adx_buffer_trim(pytestconfig, make_candles_fn):
    symbol, tf = "ESZ5", "1m"
    period = 14
    max_buffer = 40
    adx = AdxCalc(period=period, max_buffer=max_buffer)

    params = _mk_params(pytestconfig, seed=7, n=200, symbol=symbol, tf=tf)
    candles = list(make_candles_fn(**params))
    _feed(adx, symbol, tf, candles)

    s = adx.state[f"{symbol}|{tf}"]
    # El DF interno no debe crecer más allá de max_buffer
    assert len(s["df"]) <= max_buffer


def test_adx_flat_market_zeroes():
    """
    Mercado plano -> ATR≈0, DI≈0, DX≈0, ADX≈0
    Verificamos que no haya divisiones por cero ni NaNs.
    """
    symbol, tf = "ESZ5", "1m"
    period = 14
    adx = AdxCalc(period=period)

    base_ts = 1_700_000_000_000
    n = 2 * period + 20
    candles = [
        {"ts": base_ts + i * 60_000, "high": 100.0, "low": 100.0, "close": 100.0}
        for i in range(n)
    ]

    outs = _feed(adx, symbol, tf, candles)
    assert len(outs) > 0
    last = outs[-1][1]
    for k in ("adx", "pdi", "mdi"):
        v = last[k]
        assert math.isfinite(v)
        # Tolerancia numérica por ewm; debe estar muy cerca de 0
        assert 0.0 <= v <= 1.0
