# tests/test_vwap_unit.py
import math
import pytest
import inspect
from indicators_engine.indicators.classic.vwap_bar import VwapCalc

# ------------- Helpers -------------
def _mk_trades(*, n, ts0, price0, dprice, size0, dsize, tf="1m"):
    """
    Genera una secuencia determinista de 'n' trades.
    price = price0 + i*dprice
    size  = max(1, size0 + (i % 3) - 1) + dsize*(i % 2)  (siempre > 0)
    """
    trades = []
    ts = ts0
    for i in range(n):
        price = float(price0 + i * dprice)
        base = max(1, size0 + (i % 3) - 1)  # evita 0/negativos
        size = base + (dsize if (i % 2) == 0 else 0)
        trades.append({"ts": ts, "price": price, "size": float(size), "tf": tf})
        ts += 1000  # 1s
    return trades

def _feed(vwap: VwapCalc, symbol: str, trades):
    outs = []
    for t in trades:
        out = vwap.on_trade(symbol=symbol, ts=t["ts"], price=t["price"], size=t["size"], tf=t.get("tf"))
        if out is not None:
            outs.append((t["ts"], out))
    return outs


# ------------- Tests -------------
def test_vwap_basic_accumulation_deterministic():
    """
    Comprobamos que el VWAP acumulado del objeto coincide con el VWAP teórico
    sum(p_i * v_i) / sum(v_i) para una secuencia determinista.
    """
    vwap = VwapCalc()
    sym = "ESZ5"
    trades = _mk_trades(n=50, ts0=1_700_000_000_000, price0=100.0, dprice=0.25, size0=2, dsize=1, tf="1m")

    # feed y acumulado teórico en paralelo
    num = 0.0  # suma p*v
    den = 0.0  # suma v
    last = None
    for t in trades:
        num += t["price"] * t["size"]
        den += t["size"]
        expected = num / den
        out = vwap.on_trade(symbol=sym, ts=t["ts"], price=t["price"], size=t["size"], tf=t["tf"])
        if out is not None:
            last = out
            assert math.isfinite(out)
            assert abs(out - expected) <= 1e-9

    assert last is not None


def test_vwap_ignores_non_positive_size():
    """
    size <= 0 debe ser ignorado -> on_trade devuelve None y no altera el acumulado.
    """
    vwap = VwapCalc()
    sym = "NQZ5"
    ts0 = 1_700_000_000_000

    # size 0
    out = vwap.on_trade(symbol=sym, ts=ts0, price=200.0, size=0, tf="1m")
    assert out is None

    # size negativo
    out = vwap.on_trade(symbol=sym, ts=ts0 + 1, price=200.0, size=-5, tf="1m")
    assert out is None

    # primer trade válido
    out = vwap.on_trade(symbol=sym, ts=ts0 + 2, price=200.0, size=5, tf="1m")
    assert out == 200.0


def test_vwap_daily_reset_boundary():
    """
    Si VwapCalc soporta reset diario (parámetro 'reset_daily' en __init__ o atributo),
    en el cambio exacto de día deben resetearse los acumulados.
    Si no, marcamos la prueba como XFAIL (comportamiento no soportado por la implementación).
    """
    sig = inspect.signature(VwapCalc.__init__)
    supports_kw = "reset_daily" in sig.parameters
    vwap = VwapCalc(**({"reset_daily": True} if supports_kw else {}))

    # Si no hay soporte explícito, ver si existe un atributo que indique reset diario.
    supports_attr = getattr(vwap, "reset_daily", None) is True
    if not (supports_kw or supports_attr):
        pytest.xfail("VwapCalc no soporta 'reset diario' explícito; se marca XFAIL.")

    sym = "ESZ5"
    day_ms = 86_400_000

    # Día 1
    t1 = vwap.on_trade(symbol=sym, ts=1_700_000_000_000, price=100.0, size=10, tf=None)
    t2 = vwap.on_trade(symbol=sym, ts=1_700_000_100_000, price=110.0, size=10, tf=None)
    assert round(t2, 6) == 105.0  # media ponderada del día 1

    # Día 2 (frontera exacta +day_ms) -> reset esperado
    t3 = vwap.on_trade(symbol=sym, ts=1_700_000_000_000 + day_ms, price=90.0, size=5, tf=None)
    assert t3 == 90.0
