
from indicators_engine.pipelines.orderflow import OrderFlowCalc

def _print_snapshot(tag, snap):
    print(f"[{tag}] bid={snap['bid']} ask={snap['ask']}  BUY={snap['buy']} SELL={snap['sell']} DELTA={snap['delta']}")

def test_orderflow_basic_with_bbo():
    print("\n[TEST] OrderFlow básico con BBO (agresor por ask/bid)")
    of = OrderFlowCalc(reset_daily=False)
    sym, tf = "ESZ5", "1m"
    ts0 = 1_700_000_000_000

    # BBO inicial
    s0 = of.on_bbo(symbol=sym, ts=ts0, bid=100.00, ask=100.25, tf=tf)
    _print_snapshot("BBO#0", s0)

    # Trade en ask -> BUY
    s1 = of.on_trade(symbol=sym, ts=ts0+1, price=100.25, size=2, tf=tf)
    _print_snapshot("AFTER#1 (trade@ask BUY x2)", s1)
    assert s1["buy"] == 2.0 and s1["sell"] == 0.0 and s1["delta"] == 2.0

    # Trade en bid -> SELL
    s2 = of.on_trade(symbol=sym, ts=ts0+2, price=100.00, size=1, tf=tf)
    _print_snapshot("AFTER#2 (trade@bid SELL x1)", s2)
    assert s2["buy"] == 2.0 and s2["sell"] == 1.0 and s2["delta"] == 1.0

    # Trade entre bid/ask, más cercano al ask -> BUY
    s3 = of.on_trade(symbol=sym, ts=ts0+3, price=100.24, size=3, tf=tf)
    _print_snapshot("AFTER#3 (trade near ask BUY x3)", s3)
    assert s3["buy"] == 5.0 and s3["sell"] == 1.0 and s3["delta"] == 4.0

def test_orderflow_daily_reset():
    print("\n[TEST] OrderFlow reset diario")
    of = OrderFlowCalc(reset_daily=True)
    sym = "ESZ5"

    # Día 1
    s0 = of.on_bbo(symbol=sym, ts=1_700_000_000_000, bid=100.00, ask=100.25, tf=None)
    _print_snapshot("DAY1_BBO", s0)
    s1 = of.on_trade(symbol=sym, ts=1_700_000_000_001, price=100.25, size=2, tf=None)
    _print_snapshot("DAY1_AFTER#1", s1)
    assert s1["delta"] == 2.0 and s1["buy"] == 2.0 and s1["sell"] == 0.0

    # Día siguiente -> reset
    s2 = of.on_bbo(symbol=sym, ts=1_700_086_400_000, bid=99.75, ask=100.00, tf=None)
    _print_snapshot("DAY2_BBO", s2)
    s3 = of.on_trade(symbol=sym, ts=1_700_086_400_001, price=99.75, size=1, tf=None)
    _print_snapshot("DAY2_AFTER#1", s3)
    # 99.75==bid -> SELL, delta -1.0
    assert s3["delta"] == -1.0 and s3["buy"] == 0.0 and s3["sell"] == 1.0

    # Trade en ask para BUY
    s4 = of.on_trade(symbol=sym, ts=1_700_086_400_002, price=100.00, size=1, tf=None)
    _print_snapshot("DAY2_AFTER#2", s4)
    assert s4["delta"] == 0.0 and s4["buy"] == 1.0 and s4["sell"] == 1.0
