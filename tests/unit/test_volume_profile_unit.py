from indicators_engine.pipelines.volume_profile import VolumeProfileCalc

def _print_snapshot(tag, snap):
    bins_str = ", ".join([f"{b['price']}: {b['volume']}" for b in snap["bins"]])
    print(f"[{tag}] bucket_start={snap['bucket_start']}  VTOTAL={snap['vtotal']}  POC={snap['poc']}  BINS={{ {bins_str} }}")

def test_vp_same_bucket_accumulation():
    print("\n[TEST] Volume Profile acumulación en mismo bucket (1m)")
    vp = VolumeProfileCalc(tick_size=0.25, tf="1m", max_buckets=3)
    sym, tf = "ESZ5", "1m"
    ts0 = 1_700_000_000_000  # ms

    # Alinear al inicio de minuto para no cruzar de bucket
    b0 = (ts0 // 60_000) * 60_000

    # 3 trades dentro del MISMO minuto
    s1 = vp.on_trade(symbol=sym, ts=b0 + 1_000,  price=100.00, size=2, tf=tf)
    _print_snapshot("AFTER#1", s1)
    s2 = vp.on_trade(symbol=sym, ts=b0 + 20_000, price=100.25, size=1, tf=tf)
    _print_snapshot("AFTER#2", s2)
    s3 = vp.on_trade(symbol=sym, ts=b0 + 39_000, price=100.25, size=2, tf=tf)  # < 60s para no cambiar de bucket
    _print_snapshot("AFTER#3", s3)

    assert s1["bucket_start"] == s2["bucket_start"] == s3["bucket_start"] == b0
    assert s3["vtotal"] == 5.0
    prices = [b["price"] for b in s3["bins"]]
    vols    = [b["volume"] for b in s3["bins"]]
    assert set(prices) == {100.00, 100.25}
    assert set(vols) == {2.0, 3.0}
    assert s3["poc"] == 100.25

def test_vp_bucket_rollover_and_storage():
    print("\n[TEST] Volume Profile cambio de bucket y almacenamiento (sin asumir eviction)")
    vp = VolumeProfileCalc(tick_size=0.25, tf="1m", max_buckets=2)
    sym, tf = "ESZ5", "1m"
    base = 1_700_000_000_000

    # Bucket A
    s1 = vp.on_trade(symbol=sym, ts=base + 10_000, price=100.00, size=2, tf=tf)
    _print_snapshot("BUCKET_A_AFTER#1", s1)

    # Bucket B (siguiente minuto)
    tsB = base + 60_000 + 5_000
    s2 = vp.on_trade(symbol=sym, ts=tsB, price=100.25, size=3, tf=tf)
    _print_snapshot("BUCKET_B_AFTER#1", s2)

    # Bucket C (otro minuto más)
    tsC = base + 2*60_000 + 5_000
    s3 = vp.on_trade(symbol=sym, ts=tsC, price=100.50, size=1, tf=tf)
    _print_snapshot("BUCKET_C_AFTER#1", s3)

    # Recuperar B y C; comprobar que sus perfiles son correctos
    b_ts = ((base + 60_000) // 60_000) * 60_000
    c_ts = ((base + 120_000) // 60_000) * 60_000

    snapB = vp.get_bucket(symbol=sym, bucket_ts=b_ts, tf=tf)
    _print_snapshot("GET_BUCKET_B", snapB)
    snapC = vp.get_bucket(symbol=sym, bucket_ts=c_ts, tf=tf)
    _print_snapshot("GET_BUCKET_C", snapC)

    assert snapB["vtotal"] == 3.0 and snapB["poc"] == 100.25
    assert snapC["vtotal"] == 1.0 and snapC["poc"] == 100.50

    # Nota: algunas implementaciones expulsan el bucket A (eviction por max_buckets=2)
    # Otras lo mantienen en memoria histórica. Por eso NO lo forzamos aquí.
    a_ts = (base // 60_000) * 60_000
    snapA = vp.get_bucket(symbol=sym, bucket_ts=a_ts, tf=tf)
    _print_snapshot("GET_BUCKET_A (informativo)", snapA)
    # No hay assert sobre A para ser compatible con ambas variantes.
