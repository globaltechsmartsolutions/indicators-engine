from indicators_engine.pipelines.vwap import VwapCalc

def test_vwap_basic_accumulation():
    vwap = VwapCalc(reset_daily=False)
    sym, tf = "ESZ5", "1m"
    ts0 = 1_700_000_000_000

    print("\n[TEST] basic_accumulation")

    # Trade 1
    v1 = vwap.on_trade(symbol=sym, ts=ts0, price=100.0, size=2, tf=tf)
    print(f"T1: price=100 size=2 -> cum_pv=200, cum_vol=2, VWAP={v1}")
    assert v1 == 100.0

    # Trade 2
    v2 = vwap.on_trade(symbol=sym, ts=ts0+1, price=101.0, size=1, tf=tf)
    cum_pv = 200 + 101
    cum_vol = 2 + 1
    print(f"T2: price=101 size=1 -> cum_pv={cum_pv}, cum_vol={cum_vol}, VWAP={v2}")
    assert round(v2, 6) == round(301/3, 6)


def test_vwap_ignores_non_positive_size():
    vwap = VwapCalc(reset_daily=False)
    sym, tf = "NQZ5", "1m"
    ts0 = 1_700_000_000_000

    print("\n[TEST] ignores_non_positive_size")

    v1 = vwap.on_trade(symbol=sym, ts=ts0, price=200.0, size=0, tf=tf)
    print(f"T1: size=0 -> ignorado, VWAP={v1}")
    assert v1 is None

    v2 = vwap.on_trade(symbol=sym, ts=ts0+1, price=200.0, size=-5, tf=tf)
    print(f"T2: size=-5 -> ignorado, VWAP={v2}")
    assert v2 is None

    v3 = vwap.on_trade(symbol=sym, ts=ts0+2, price=200.0, size=5, tf=tf)
    print(f"T3: price=200 size=5 -> cum_pv=1000, cum_vol=5, VWAP={v3}")
    assert v3 == 200.0


def test_vwap_daily_reset():
    vwap = VwapCalc(reset_daily=True)
    sym = "ESZ5"

    print("\n[TEST] daily_reset")

    v1 = vwap.on_trade(symbol=sym, ts=1_700_000_000_000, price=100, size=10, tf=None)
    print(f"T1: price=100 size=10 (día1) -> cum_pv=1000, cum_vol=10, VWAP={v1}")

    v2 = vwap.on_trade(symbol=sym, ts=1_700_000_100_000, price=110, size=10, tf=None)
    print(f"T2: price=110 size=10 (día1) -> cum_pv=1000+1100=2100, cum_vol=20, VWAP={v2}")
    assert round(v2, 6) == 105.0

    v3 = vwap.on_trade(symbol=sym, ts=1_700_086_400_000, price=90, size=5, tf=None)
    print(f"T3: cambio de día -> reset -> price=90 size=5 -> cum_pv=450, cum_vol=5, VWAP={v3}")
    assert v3 == 90.0
