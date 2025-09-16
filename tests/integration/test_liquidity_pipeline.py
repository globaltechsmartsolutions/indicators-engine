import asyncio, orjson, pytest

@pytest.mark.asyncio
async def test_liquidity_pipeline(nc, cfg, liquidity_worker):
    IN  = cfg.get("in_orderbook_subj", "market.orderbook")
    OUT = liquidity_worker
    symbol = cfg["symbol"]

    q = asyncio.Queue()
    async def cb(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        if d.get("indicator") == "liquidity" and d.get("symbol") == symbol:
            await q.put(d)

    sub = await nc.subscribe(OUT, cb=cb)
    await nc.flush()
    try:
        # 1) Snapshot inicial
        snap = {
            "kind": "snapshot", "eventSymbol": symbol,
            "time": 1726500000000000000,
            "bids": [[4999.50, 5], [4999.25, 2]],
            "asks": [[5000.00, 3], [5000.25, 4]],
        }
        await nc.publish(IN, orjson.dumps(snap))
        out1 = await asyncio.wait_for(q.get(), timeout=2.0)
        assert out1["indicator"] == "liquidity"
        assert out1["ts"] == 1726500000000
        assert out1["depth_levels"] == 10 or out1["depth_levels"] >= 2
        # si depth>=2: 5+2 vs 3+4 → 7 vs 7
        if out1["depth_levels"] >= 2:
            assert abs(out1["bids_depth"] - 7.0) < 1e-9
            assert abs(out1["asks_depth"] - 7.0) < 1e-9

        # 2) Update: aumentar best bid a 12
        upd1 = {"kind":"update","eventSymbol":symbol,"side":"bid","price":4999.50,"size":12,"time":172650000000500000}
        await nc.publish(IN, orjson.dumps(upd1))
        out2 = await asyncio.wait_for(q.get(), timeout=2.0)
        assert out2["ts"] == 172650000000  # mismo segundo (redondeo a ms)
        assert out2["best_bid"] == 4999.5
        assert out2["bid1_size"] == 12.0

        # 3) Update: eliminar best ask
        upd2 = {"kind":"update","eventSymbol":symbol,"side":"ask","price":5000.00,"size":0,"time":1726500001000000000}
        await nc.publish(IN, orjson.dumps(upd2))
        out3 = await asyncio.wait_for(q.get(), timeout=2.0)
        assert out3["ts"] == 1726500001000
        # nuevo best ask pasa a 5000.25
        assert out3["best_ask"] == 5000.25
    finally:
        await sub.unsubscribe()
        await nc.flush()
