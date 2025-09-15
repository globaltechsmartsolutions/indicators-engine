import asyncio
import orjson
import pytest

async def subscribe_out(nc, subject: str, symbol: str, tf: str):
    q = asyncio.Queue()
    async def out_cb(msg):
        try:
            data = orjson.loads(msg.data)
        except Exception:
            return
        if data.get("symbol") == symbol and data.get("tf") == tf:
            await q.put(data)
    sub = await nc.subscribe(subject, cb=out_cb)
    await nc.flush()
    await asyncio.sleep(0.02)
    return sub, q

def assert_vwap_payload(msg: dict):
    for k in ("symbol", "tf", "ts", "indicator", "value"):
        assert k in msg
    assert msg["indicator"].lower() in ("vwap", "volume_weighted_average_price")
    float(msg["value"])

def make_trades(symbol: str, tf: str, ts0: int, price0: float):
    return [
        {"symbol": symbol, "tf": tf, "ts": ts0,   "price": price0,     "size": 2},
        {"symbol": symbol, "tf": tf, "ts": ts0+1, "price": price0+2.0, "size": 1},
        {"symbol": symbol, "tf": tf, "ts": ts0+2, "price": price0-1.0, "size": 1},
    ]
    # VWAP esperado = price0 + 0.25

async def publish_trades(nc, subject: str, trades):
    for t in trades:
        await nc.publish(subject, orjson.dumps(t))
    await nc.flush()
    await asyncio.sleep(0.02)

@pytest.mark.asyncio
async def test_vwap_pipeline(nc, cfg, vwap_worker):
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = vwap_worker
    symbol, tf = cfg["symbol"], cfg["tf"]
    price0 = float(cfg.get("price", 100.0))
    ts0 = int(cfg.get("ts0", 1_700_000_000_000))

    sub_out, q = await subscribe_out(nc, OUT_SUBJ, symbol, tf)
    try:
        trades = make_trades(symbol, tf, ts0, price0)
        await publish_trades(nc, IN_SUBJ, trades)

        got = []
        for _ in range(len(trades)):
            d = await asyncio.wait_for(q.get(), timeout=5.0)
            got.append(d)

        assert got
        for m in (got[0], got[-1]):
            assert_vwap_payload(m)

        expected = price0 + 0.25
        assert abs(got[-1]["value"] - expected) < 1e-6

    finally:
        await sub_out.unsubscribe()
        await nc.flush()
