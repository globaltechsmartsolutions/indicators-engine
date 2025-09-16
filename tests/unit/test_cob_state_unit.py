from indicators_engine.pipelines.cob_state import BookState

def test_apply_snapshot_and_trim_depth():
    print("[TEST] creando BookState y aplicando snapshot básico")
    st = BookState("ESZ5", max_depth=1)
    st.apply_snapshot({
        "eventSymbol": "ESZ5",
        "time": 1726500000000000000,
        "bids": [{"price": 4999.75, "size": 12}, {"price": 4999.5, "size": 8}],
        "asks": [{"price": 5000.0, "size": 10}, {"price": 5000.25, "size": 7}],
    })
    snap = st.snapshot()
    print(f"[TEST] snapshot resultante: {snap}")
    assert snap["ts"] == 1726500000000
    assert snap["bids"] == [[4999.75, 12.0]]
    assert snap["asks"] == [[5000.0, 10.0]]

def test_incremental_updates_add_update_delete():
    print("[TEST] creando BookState y aplicando snapshot vacío")
    st = BookState("ESZ5", max_depth=10)
    st.apply_snapshot({"eventSymbol": "ESZ5", "time": 1726500000000000000, "bids": [], "asks": []})

    print("[TEST] añadiendo niveles bid y ask")
    st.apply_update({"side": "bid", "price": 4999.75, "size": 5, "time": 1726500000001000000})
    st.apply_update({"side": "ask", "price": 5000.00, "size": 7, "time": 1726500000002000000})

    print("[TEST] actualizando tamaño del bid existente")
    st.apply_update({"side": "bid", "price": 4999.75, "size": 12, "time": 1726500000003000000})

    print("[TEST] eliminando ask")
    st.apply_update({"side": "ask", "price": 5000.00, "size": 0, "time": 1726500000004000000})

    snap = st.snapshot()
    print(f"[TEST] snapshot final: {snap}")
    assert snap["ts"] == 1726500000004
    assert snap["bids"][0] == [4999.75, 12.0]
    assert len(snap["asks"]) == 0
