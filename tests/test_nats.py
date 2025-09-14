import asyncio
import orjson
import pytest

@pytest.mark.asyncio
async def test_rsi_pipeline(nc, cfg, make_candles_fn):
    IN_SUBJ  = cfg["in_subj"]
    OUT_SUBJ = f"indicators.candles.1m.rsi{cfg['rsi_period']}"
    symbol, tf = cfg["symbol"], cfg["tf"]
    n, period  = cfg["n"], cfg["rsi_period"]

    q = asyncio.Queue()

    async def cb(msg):
        data = orjson.loads(msg.data)
        if data.get("symbol") == symbol and data.get("tf") == tf:
            await q.put(data)

    # ⬇️ ahora guardamos el Subscription, no un SID
    sub = await nc.subscribe(OUT_SUBJ, cb=cb)
    await nc.flush()

    # Publica velas dummy
    for c in make_candles_fn(
            n=n, tf=tf, price0=cfg["price"], amplitude=cfg.get("amplitude", cfg.get("ampl")),
            pattern=cfg["pattern"], seed=cfg["seed"], symbol=symbol
    ):
        await nc.publish(IN_SUBJ, orjson.dumps(c))
    await nc.flush()

    expected_min = max(0, n - period)
    got = []
    deadline = asyncio.get_event_loop().time() + 6.0
    while len(got) < expected_min and asyncio.get_event_loop().time() < deadline:
        try:
            got.append(await asyncio.wait_for(q.get(), timeout=0.5))
        except asyncio.TimeoutError:
            pass

    # ⬇️ aquí el cambio importante
    await sub.unsubscribe()
    await nc.flush()

    assert len(got) >= expected_min, f"Esperaba ≥{expected_min} RSI; llegaron {len(got)}"
    if got:
        assert "value" in got[-1] and 0.0 <= got[-1]["value"] <= 100.0
