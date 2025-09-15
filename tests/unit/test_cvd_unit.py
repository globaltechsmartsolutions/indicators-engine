# tests/unit/test_cvd_unit.py
from indicators_engine.pipelines.cvd import CvdCalc

def test_cvd_basic_buy_sell_with_bid_ask():
    cvd = CvdCalc(reset_daily=False)
    sym, tf = "ESZ5", "1m"
    ts0 = 1_700_000_000_000

    print("\n[TEST] basic_buy_sell_with_bid_ask")

    # Trade 1: cruza ask -> compra
    v1 = cvd.on_trade(sym, ts0,   price=100.0, size=2, tf=tf, bid=99.99, ask=100.0)
    print(f"T1: price=100.0 ask=100.0 -> BUY size=2 => CVD={v1}")

    # Trade 2: cruza bid -> venta
    v2 = cvd.on_trade(sym, ts0+1, price=99.99, size=1, tf=tf, bid=99.99, ask=100.01)
    print(f"T2: price=99.99 bid=99.99 -> SELL size=1 => CVD={v2}")

    # Trade 3: en el spread, sube vs last -> compra
    v3 = cvd.on_trade(sym, ts0+2, price=100.005, size=3, tf=tf, bid=100.0, ask=100.01)
    print(f"T3: price=100.005 in spread ↑last -> BUY size=3 => CVD={v3}")

    # Trade 4: side explícito SELL
    v4 = cvd.on_trade(sym, ts0+3, price=100.005, size=5, tf=tf, side="SELL")
    print(f"T4: side=SELL -> SELL size=5 => CVD={v4}")

    assert v1 == 2.0
    assert v2 == 1.0
    assert v3 == 4.0
    assert v4 == -1.0


def test_cvd_same_price_tick_rule_memory():
    cvd = CvdCalc(reset_daily=False)
    sym, tf = "NQZ5", "1m"
    ts0 = 1_700_000_000_000

    print("\n[TEST] same_price_tick_rule_memory")

    # Marca dirección alcista con subida de precio
    v0 = cvd.on_trade(sym, ts0,   price=200.0, size=1, tf=tf)
    print(f"T0: first trade price=200.0 no bid/ask no side -> dir={cvd.state[sym+'|'+tf]['last_dir']} CVD={v0}")

    v1 = cvd.on_trade(sym, ts0+1, price=200.25, size=2, tf=tf)
    print(f"T1: price=200.25 > last=200.0 -> BUY size=2 => CVD={v1}")

    # Mismo precio (200.25), sin bid/ask ni side -> reutiliza último dir (buy)
    v2 = cvd.on_trade(sym, ts0+2, price=200.25, size=4, tf=tf)
    print(f"T2: price=200.25 == last=200.25 -> reuse last_dir={cvd.state[sym+'|'+tf]['last_dir']} size=4 => CVD={v2}")

    assert v2 == 7.0


def test_cvd_daily_reset():
    cvd = CvdCalc(reset_daily=True)
    sym = "ESZ5"
    tf = None

    print("\n[TEST] daily_reset")

    # Dos trades el mismo día
    v1 = cvd.on_trade(sym, 1_700_000_000_000, price=100, size=10, tf=tf, side="B")
    print(f"T1: side=B size=10 => CVD={v1}")
    v2 = cvd.on_trade(sym, 1_700_000_100_000, price=101, size=5,  tf=tf, side="B")
    print(f"T2: side=B size=5 same day => CVD={v2}")

    assert v2 == 15.0

    # Siguiente día -> reset a 0 antes de acumular
    v3 = cvd.on_trade(sym, 1_700_086_400_000, price=99,  size=3,  tf=tf, side="S")
    print(f"T3: next day reset -> side=S size=3 => CVD={v3}")

    assert v3 == -3.0
