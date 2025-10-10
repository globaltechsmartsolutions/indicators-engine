# tests/test_macd_unit.py
import math
from indicators_engine.indicators.classic.macd import MacdCalc

# ---------------- Helpers ----------------
def _get(v, k, default=None):
    if isinstance(v, dict):
        return v.get(k, default)
    return getattr(v, k, default)

def _bar_args_close(v):
    return (
        _get(v, "ts"),
        float(_get(v, "close")),
    )

def _feed(macd: MacdCalc, symbol: str, tf: str, candles):
    outs = []
    for v in candles:
        ts, cl = _bar_args_close(v)
        out = macd.on_bar(symbol, tf, ts, cl)
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
def test_macd_randomwalk_basics(pytestconfig, make_candles_fn):
    """
    Similar al test de ADX:
    - Alimentamos n velas generadas
    - Verificamos warm-up (MACD comienza tras slow+signal)
    - Último valor tiene claves esperadas y es finito
    - Coherencia: hist = macd - signal
    """
    symbol, tf = "ESZ5", "1m"
    fast, slow, signal = 12, 26, 9
    n = 220

    macd = MacdCalc(fast=fast, slow=slow, signal=signal)

    params = _mk_params(pytestconfig, seed=42, n=n, symbol=symbol, tf=tf)
    candles = list(make_candles_fn(**params))
    outs = _feed(macd, symbol, tf, candles)

    assert len(outs) >= max(0, n - (slow + signal))

    last_ts, last = outs[-1]
    assert set(last.keys()) == {"macd", "signal", "hist"}
    for k in ("macd", "signal", "hist"):
        assert math.isfinite(last[k])
    assert abs(last["hist"] - (last["macd"] - last["signal"])) < 1e-9


def test_macd_bounds_and_monotonic_ts(pytestconfig, make_candles_fn):
    """
    - Generamos velas y aseguramos que con suficientes barras produce salida.
    - Enviar un ts repetido o anterior -> None.
    - Con ts posterior -> produce salida finita.
    """
    symbol, tf = "ESZ5", "1m"
    macd = MacdCalc()

    params = _mk_params(pytestconfig, seed=123, n=80, symbol=symbol, tf=tf)
    candles = list(make_candles_fn(**params))

    outs = _feed(macd, symbol, tf, candles[:40])
    assert len(outs) > 0
    last_ts_ok = outs[-1][0]

    # Barra antigua
    v_old = candles[5]
    ts_old, cl_old = _bar_args_close(v_old)
    assert ts_old < last_ts_ok
    assert macd.on_bar(symbol, tf, ts_old, cl_old) is None

    # Barra duplicada (mismo ts que last_ts_ok) -> None
    # Tomamos el close de la última vela que produjo salida
    last_idx = next(i for i, v in enumerate(candles[:40]) if v["ts"] == last_ts_ok)
    ts_dup, cl_dup = _bar_args_close(candles[last_idx])
    assert macd.on_bar(symbol, tf, ts_dup, cl_dup) is None

    # Barra nueva
    v_new = candles[41]
    ts2, cl2 = _bar_args_close(v_new)
    assert ts2 > last_ts_ok
    out2 = macd.on_bar(symbol, tf, ts2, cl2)
    assert out2 is not None
    for k in ("macd", "signal", "hist"):
        assert math.isfinite(out2[k])


def test_macd_buffer_trim(pytestconfig, make_candles_fn):
    """
    El buffer interno no debe crecer más allá de max_buffer.
    """
    symbol, tf = "ESZ5", "1m"
    max_buffer = 40
    macd = MacdCalc(max_buffer=max_buffer)

    params = _mk_params(pytestconfig, seed=7, n=200, symbol=symbol, tf=tf)
    candles = list(make_candles_fn(**params))
    _feed(macd, symbol, tf, candles)

    s = macd.state[f"{symbol}|{tf}"]
    assert len(s["df"]) <= max_buffer


def test_macd_flat_market_zeroes():
    """
    Mercado plano -> EMA_fast = EMA_slow -> MACD≈0, signal≈0, hist≈0
    Verificamos que no haya NaNs y que esté muy cercano a 0.
    """
    symbol, tf = "ESZ5", "1m"
    fast, slow, signal = 12, 26, 9
    macd = MacdCalc(fast=fast, slow=slow, signal=signal)

    base_ts = 1_700_000_000_000
    n = slow + signal + 50  # suficiente para calentar y estabilizar
    candles = [
        {"ts": base_ts + i * 60_000, "close": 100.0}
        for i in range(n)
    ]

    outs = _feed(macd, symbol, tf, candles)
    assert len(outs) > 0
    last = outs[-1][1]
    for k in ("macd", "signal", "hist"):
        v = last[k]
        assert math.isfinite(v)
        assert abs(v) <= 1e-6  # tolerancia numérica por ewm


def test_macd_trend_sign():
    """
    Tendencia alcista -> macd y signal positivos; bajista -> negativos
    (coincide con el test original pero consolidado)
    """
    # Uptrend
    macd = MacdCalc()
    ts = 1_700_000_000_000
    last_up = None
    for i in range(60):
        last_up = macd.on_bar("ESZ5", "1m", ts, 100.0 + i * 0.5) or last_up
        ts += 60_000
    assert last_up is not None
    assert last_up["macd"] > 0.0
    assert last_up["signal"] > 0.0

    # Downtrend
    macd2 = MacdCalc()
    ts = 1_700_000_000_000
    last_dn = None
    for i in range(60):
        last_dn = macd2.on_bar("ESZ5", "1m", ts, 100.0 - i * 0.5) or last_dn
        ts += 60_000
    assert last_dn is not None
    assert last_dn["macd"] < 0.0
    assert last_dn["signal"] < 0.0
