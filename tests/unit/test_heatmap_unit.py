from indicators_engine.pipelines.heatmap import HeatmapState

def test_heatmap_accum_max_per_bucket():
    st = HeatmapState("ESZ5", tick_size=0.25, bucket_ms=1000)

    # SNAPSHOT (nanos→ms dentro del estado)
    st.apply_snapshot({
        "eventSymbol": "ESZ5",
        "time": 1726500000000000000,
        "bids": [[4999.50, 5], [4999.25, 2]],
        "asks": [[5000.00, 3], [5000.25, 4]],
    })
    f1 = st.frame()
    assert f1["ts"] == 1726500000000
    # hay filas para todos los niveles del snapshot
    pset1 = {tuple(r) for r in f1["rows"]}
    assert [1726500000000, 4999.5, 5.0] in f1["rows"]
    assert [1726500000000, 5000.25, 4.0] in f1["rows"]

    # UPDATE dentro del mismo bucket: sube size del 4999.50 a 12 => max del bucket = 12
    st.apply_update({
        "eventSymbol": "ESZ5", "side": "bid",
        "price": 4999.50, "size": 12, "time": 1726500000000500000
    })
    f2 = st.frame()
    # mismo bucket
    assert f2["ts"] == 1726500000000
    rows = {p: s for _, p, s in f2["rows"]}
    assert rows[4999.5] == 12.0  # acumulado por max

    # UPDATE en siguiente bucket (+1s)
    st.apply_update({
        "eventSymbol": "ESZ5", "side": "ask",
        "price": 5000.00, "size": 10, "time": 1726500001000000000
    })
    f3 = st.frame()
    assert f3["ts"] == 1726500001000  # nuevo bucket
    rows3 = {p: s for _, p, s in f3["rows"]}
    assert rows3[5000.0] == 10.0
