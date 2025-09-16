import asyncio, orjson, pytest

@pytest.mark.asyncio
async def test_cob_incremental_pipeline(nc, cfg, cob_incremental_worker):
    IN = cfg.get("in_orderbook_subj", "market.orderbook")
    OUT = cob_incremental_worker
    symbol = cfg["symbol"]

    q = asyncio.Queue()
    async def cb(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        if d.get("indicator") == "cob" and d.get("symbol") == symbol:
            await q.put(d)

    sub = await nc.subscribe(OUT, cb=cb)
    await nc.flush()

    try:
        print("[TEST] Enviando snapshot inicial")
        snap = {
            "kind": "snapshot",
            "eventSymbol": symbol,
            "time": 1726500000000000000,
            "bids": [[4999.50, 5], [4999.25, 2]],
            "asks": [[5000.00, 3], [5000.25, 4]],
        }
        await nc.publish(IN, orjson.dumps(snap))
        out1 = await asyncio.wait_for(q.get(), timeout=2.0)
        print(f"[TEST] recibido COB tras snapshot: {out1}")
        assert out1["bids"][0] == [4999.5, 5.0]
        assert out1["asks"][0] == [5000.0, 3.0]

        print("[TEST] Enviando update del bid")
        upd1 = {"kind": "update", "eventSymbol": symbol, "side": "bid",
                "price": 4999.50, "size": 12, "time": 1726500000001000000}
        await nc.publish(IN, orjson.dumps(upd1))
        out2 = await asyncio.wait_for(q.get(), timeout=2.0)
        print(f"[TEST] recibido COB tras update bid: {out2}")
        assert out2["bids"][0] == [4999.5, 12.0]

        print("[TEST] Enviando update para eliminar ask")
        upd2 = {"kind": "update", "eventSymbol": symbol, "side": "ask",
                "price": 5000.00, "size": 0, "time": 1726500000002000000}
        await nc.publish(IN, orjson.dumps(upd2))
        out3 = await asyncio.wait_for(q.get(), timeout=2.0)
        print(f"[TEST] recibido COB tras eliminar ask: {out3}")
        assert out3["asks"][0][0] == 5000.25

    finally:
        await sub.unsubscribe()
        await nc.flush()
