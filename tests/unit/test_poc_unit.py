from indicators_engine.pipelines.poc import PocCalc

def test_poc_basic_accumulation():
    poc = PocCalc(tick_size=0.25, reset_daily=False)
    sym, tf = "ESZ5", "1m"
    ts0 = 1_700_000_000_000

    print("\n[TEST] basic_accumulation")

    # Trade 1
    v1 = poc.on_trade(symbol=sym, ts=ts0, price=100.00, size=2, tf=tf)
    print(f"T1: price=100.00 size=2 -> POC={v1}, vol@100.00=2")

    # Trade 2
    v2 = poc.on_trade(symbol=sym, ts=ts0+1, price=100.25, size=1, tf=tf)
    print(f"T2: price=100.25 size=1 -> POC={v2}, vol@100.00=2, vol@100.25=1")
    assert v1 == 100.00
    assert v2 == 100.00  # sigue ganando 100.00 con vol=2

    # Trade 3: empata volúmenes
    v3 = poc.on_trade(symbol=sym, ts=ts0+2, price=100.25, size=1, tf=tf)
    print(f"T3: price=100.25 size=1 -> POC={v3}, vol@100.00=2, vol@100.25=2 (tie-break cercano último precio)")
    assert v3 == 100.25


def test_poc_tie_break_highest_if_equal_distance():
    poc = PocCalc(tick_size=0.25, reset_daily=False)
    sym, tf = "ESZ5", "1m"
    ts = 1_700_000_000_000

    print("\n[TEST] tie_break_highest_if_equal_distance")

    poc.on_trade(symbol=sym, ts=ts,   price=100.00, size=2, tf=tf)
    poc.on_trade(symbol=sym, ts=ts+1, price=100.50, size=2, tf=tf)
    v = poc.on_trade(symbol=sym, ts=ts+2, price=100.25, size=1, tf=tf)
    print(f"Volúmenes: 100.00->2, 100.50->2 ; último precio=100.25 => POC={v} (elige el más alto)")
    assert v == 100.50


def test_poc_daily_reset():
    poc = PocCalc(tick_size=0.25, reset_daily=True)
    sym = "ESZ5"

    print("\n[TEST] daily_reset")

    v1 = poc.on_trade(symbol=sym, ts=1_700_000_000_000, price=100.00, size=2, tf=None)
    print(f"Día 1 T1: price=100.00 size=2 -> POC={v1}")

    v2 = poc.on_trade(symbol=sym, ts=1_700_000_100_000, price=100.25, size=1, tf=None)
    print(f"Día 1 T2: price=100.25 size=1 -> POC={v2}, vol@100.00=2, vol@100.25=1")
    assert v2 == 100.00

    v3 = poc.on_trade(symbol=sym, ts=1_700_086_400_000, price=99.75, size=1, tf=None)
    print(f"Día 2 T1: reset de sesión -> vol@99.75=1 -> POC={v3}")
    assert v3 == 99.75
