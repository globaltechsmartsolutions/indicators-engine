import asyncio, orjson, pytest

@pytest.mark.asyncio
async def test_book_order_pipeline(nc, cfg, book_order_worker):
    IN = cfg.get("in_orderbook_subj", "market.orderbook")
    OUT = book_order_worker
    symbol = cfg["symbol"]

    q = asyncio.Queue()
    async def cb(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        if d.get("indicator") == "book_order" and d.get("symbol") == symbol:
            await q.put(d)

    sub = await nc.subscribe(OUT, cb=cb)
    await nc.flush()

    try:
        payload = {
            "kind": "snapshot",
            "eventSymbol": symbol,
            "time": 1726500000000000000,
            "bids": [[4999.50, 5], [4999.25, 2], [4999.00, 1]],
            "asks": [[5000.00, 3], [5000.25, 4], [5000.50, 2]],
        }
        print("[TEST] Publicando OrderBookSnapshot completo (todos los niveles L2)")
        await nc.publish(IN, orjson.dumps(payload))
        out = await asyncio.wait_for(q.get(), timeout=2.0)
        print(f"[TEST] Recibido BOOK_ORDER: depth(b/a)=({len(out['bids'])}/{len(out['asks'])})")
        assert out["indicator"] == "book_order"
        assert out["ts"] == 1726500000000
        assert out["bids"][0] == [4999.5, 5.0]
        assert out["asks"][0] == [5000.0, 3.0]
        # full depth sin recorte
        assert len(out["bids"]) == 3 and len(out["asks"]) == 3
    finally:
        await sub.unsubscribe()
        await nc.flush()
