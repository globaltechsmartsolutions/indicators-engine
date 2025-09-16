
from indicators_engine.pipelines.svp import SvpCalc

def _print_snapshot(tag, snap):
    bins_str = ", ".join([f"{b['price']}: {b['volume']}" for b in snap["bins"]])
    print(f"[{tag}] POC={snap['poc']}  VTOTAL={snap['vtotal']}  BINS={{ {bins_str} }}")

def test_svp_basic_accumulation():
    print("\n[TEST] SVP basic accumulation & tie-break (Bookmap-style)")
    svp = SvpCalc(tick_size=0.25, reset_daily=False)
    sym, tf = "ESZ5", "1m"
    ts0 = 1_700_000_000_000

    print(f"[SETUP] tick_size=0.25  symbol={sym}  tf={tf}  ts0={ts0}")

    # Trade 1: 100.00 x 2
    print("[STEP] Trade#1 -> price=100.00 size=2")
    s1 = svp.on_trade(symbol=sym, ts=ts0, price=100.00, size=2, tf=tf)
    _print_snapshot("AFTER#1", s1)
    assert s1["vtotal"] == 2.0, "vtotal tras trade#1 debe ser 2.0"
    assert svp.get_volume_at_price(sym, 100.00, tf=tf) == 2.0, "volumen en 100.00 debe ser 2.0"
    assert s1["poc"] == 100.00, "POC inicial debe ser 100.00"

    # Trade 2: 100.25 x 1 (POC sigue 100.00)
    print("[STEP] Trade#2 -> price=100.25 size=1")
    s2 = svp.on_trade(symbol=sym, ts=ts0+1, price=100.25, size=1, tf=tf)
    _print_snapshot("AFTER#2", s2)
    assert s2["vtotal"] == 3.0, "vtotal tras trade#2 debe ser 3.0"
    assert s2["poc"] == 100.00, "POC debe mantenerse en 100.00 (2 vs 1)"

    # Trade 3: 100.25 x 1 (empata -> developing POC al nivel más recientemente actualizado: 100.25)
    print("[STEP] Trade#3 -> price=100.25 size=1 (empata y POC debería saltar a 100.25)")
    s3 = svp.on_trade(symbol=sym, ts=ts0+2, price=100.25, size=1, tf=tf)
    _print_snapshot("AFTER#3", s3)
    assert s3["vtotal"] == 4.0, "vtotal tras trade#3 debe ser 4.0"
    assert s3["poc"] == 100.25, "POC debe moverse a 100.25 por tie-break de 'última actualización'"

def test_svp_daily_reset():
    print("\n[TEST] SVP daily reset (UTC-day boundary)")
    svp = SvpCalc(tick_size=0.25, reset_daily=True)
    sym = "ESZ5"

    print("[STEP] Day#1 Trade -> price=100.00 size=2")
    s1 = svp.on_trade(symbol=sym, ts=1_700_000_000_000, price=100.00, size=2, tf=None)
    _print_snapshot("DAY1_AFTER#1", s1)
    assert s1["poc"] == 100.00 and s1["vtotal"] == 2.0

    print("[STEP] Day#1 Trade -> price=100.25 size=1")
    s2 = svp.on_trade(symbol=sym, ts=1_700_000_100_000, price=100.25, size=1, tf=None)
    _print_snapshot("DAY1_AFTER#2", s2)
    assert s2["poc"] == 100.00 and s2["vtotal"] == 3.0

    # Día siguiente -> reset
    print("[STEP] Day#2 Trade (next day, should reset) -> price=99.75 size=1")
    s3 = svp.on_trade(symbol=sym, ts=1_700_086_400_000, price=99.75, size=1, tf=None)
    _print_snapshot("DAY2_AFTER#1", s3)
    assert s3["poc"] == 99.75 and s3["vtotal"] == 1.0
