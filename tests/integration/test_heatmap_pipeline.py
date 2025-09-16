import asyncio, orjson, pytest

@pytest.mark.asyncio
async def test_heatmap_pipeline(nc, cfg, heatmap_worker):
    IN  = cfg.get("in_orderbook_subj", "market.orderbook")
    OUT = heatmap_worker
    symbol = cfg["symbol"]

    q = asyncio.Queue()
    async def cb(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        if d.get("indicator") == "heatmap" and d.get("symbol") == symbol:
            await q.put(d)

    sub = await nc.subscribe(OUT, cb=cb)
    await nc.flush()
    try:
        # 1) Snapshot con 2x2 niveles
        snap = {
            "kind": "snapshot", "eventSymbol": symbol,
            "time": 1726500000000000000,
            "bids": [[4999.50, 5], [4999.25, 2]],
            "asks": [[5000.00, 3], [5000.25, 4]],
        }
        await nc.publish(IN, orjson.dumps(snap))
        out1 = await asyncio.wait_for(q.get(), timeout=2.0)
        assert out1["indicator"] == "heatmap"
        assert out1["ts"] == 1726500000000
        assert [1726500000000, 4999.5, 5.0] in out1["rows"]

        # 2) Update dentro del mismo bucket: sube size => en el frame debe verse el max
        upd1 = {"kind": "update", "eventSymbol": symbol, "side": "bid",
                "price": 4999.50, "size": 12, "time": 1726500000000500000}
        await nc.publish(IN, orjson.dumps(upd1))
        out2 = await asyncio.wait_for(q.get(), timeout=2.0)
        rows2 = { (p if isinstance(p,(int,float)) else p) : s for _, p, s in out2["rows"] }
        assert out2["ts"] == 1726500000000
        assert rows2[4999.5] == 12.0

        # 3) Update en otro bucket (+1s)
        upd2 = {"kind": "update", "eventSymbol": symbol, "side": "ask",
                "price": 5000.00, "size": 10, "time": 1726500001000000000}
        await nc.publish(IN, orjson.dumps(upd2))
        out3 = await asyncio.wait_for(q.get(), timeout=2.0)
        assert out3["ts"] == 1726500001000
        assert [1726500001000, 5000.0, 10.0] in out3["rows"]
    finally:
        await sub.unsubscribe()
        await nc.flush()
