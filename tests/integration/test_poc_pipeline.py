import asyncio
import orjson
import pytest

async def subscribe_out(nc, subject: str, symbol: str, tf: str):
    q = asyncio.Queue()
    async def cb(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        if d.get("symbol") == symbol and d.get("tf") == tf:
            await q.put(d)
    sub = await nc.subscribe(subject, cb=cb)
    await nc.flush()
    await asyncio.sleep(0.02)
    return sub, q

def assert_poc_payload(m: dict):
    for k in ("symbol", "tf", "ts", "indicator", "value"):
        assert k in m
    assert m["indicator"].lower() == "poc"
    float(m["value"])

def make_trades_for_poc(symbol: str, tf: str, ts0: int, price0: float):
    # Diseñado para que el POC termine en price0+0.25
    # Vol por tick:
    #  price0      -> 2
    #  price0+0.25 -> 3 (ganador)
    return [
        {"symbol": symbol, "tf": tf, "ts": ts0,   "price": price0,      "size": 2},
        {"symbol": symbol, "tf": tf, "ts": ts0+1, "price": price0+0.25, "size": 1},
        {"symbol": symbol, "tf": tf, "ts": ts0+2, "price": price0+0.25, "size": 2},
    ]

async def publish_trades(nc, subject: str, trades):
    for t in trades:
        await nc.publish(subject, orjson.dumps(t))
    await nc.flush()
    await asyncio.sleep(0.02)

@pytest.mark.asyncio
async def test_poc_pipeline(nc, cfg, poc_worker):
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = poc_worker
    symbol, tf = cfg["symbol"], cfg["tf"]
    price0 = float(cfg.get("price", 100.0))
    ts0 = int(cfg.get("ts0", 1_700_000_000_000))

    sub_out, q = await subscribe_out(nc, OUT_SUBJ, symbol, tf)
    try:
        trades = make_trades_for_poc(symbol, tf, ts0, price0)
        await publish_trades(nc, IN_SUBJ, trades)

        got = []
        # esperamos al menos 3 mensajes (uno por trade)
        for _ in range(len(trades)):
            d = await asyncio.wait_for(q.get(), timeout=5.0)
            got.append(d)

        assert got
        for m in (got[0], got[-1]):
            assert_poc_payload(m)

        # El POC final debe ser price0+0.25
        assert abs(got[-1]["value"] - (price0 + 0.25)) < 1e-9

    finally:
        await sub_out.unsubscribe()
        await nc.flush()
