
import asyncio
import orjson
import pytest

def _p(msg):
    print(msg, flush=True)

async def subscribe_out(nc, subject: str, symbol: str, tf: str):
    q = asyncio.Queue()
    async def cb(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            _p("[OUT][RX] (json decode error)")
            return
        if d.get("symbol") == symbol and d.get("tf") == tf:
            _p(f"[OUT][RX] {d}")
            await q.put(d)
        else:
            _p(f"[OUT][RX][IGN] {d}")
    _p(f"[OUT][SUB] subject={subject} symbol={symbol} tf={tf}")
    sub = await nc.subscribe(subject, cb=cb)
    await nc.flush()
    await asyncio.sleep(0.02)
    return sub, q

async def publish(nc, subject: str, payloads):
    _p(f"[PUB] subject={subject} n={len(payloads)}")
    for p in payloads:
        _p(f"[PUB] -> {p}")
        await nc.publish(subject, orjson.dumps(p))
    await nc.flush()
    await asyncio.sleep(0.02)

@pytest.mark.asyncio
async def test_orderflow_pipeline(nc, cfg, orderflow_worker):
    """
    Usa el worker de conftest.py para Order Flow.
    Publica BBO y luego trades y valida delta (BUY-SELL).
    """
    IN_BBO  = cfg.get("bbo_subject") or "market.bbo"
    IN_TRD  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = orderflow_worker
    symbol, tf = cfg.get("symbol") or cfg.get("candle_symbol") or "ESZ5", cfg.get("tf") or cfg.get("candle_tf") or "1m"

    _p(f"[TEST] IN_BBO={IN_BBO}")
    _p(f"[TEST] IN_TRD={IN_TRD}")
    _p(f"[TEST] OUT_SUBJ={OUT_SUBJ}")
    _p(f"[TEST] symbol={symbol} tf={tf}")

    sub_out, q = await subscribe_out(nc, OUT_SUBJ, symbol, tf)

    try:
        ts0 = int(cfg.get("ts0", 1_700_000_000_000))
        base = float(cfg.get("price", cfg.get("candle_price", 100.0)))
        # Publica BBO inicial
        bbo_msgs = [
            {"symbol": symbol, "tf": tf, "ts": ts0, "bid": base, "ask": base + 0.25}
        ]
        await publish(nc, IN_BBO, bbo_msgs)

        # Publica tres trades: @ask BUY x2, @bid SELL x1, near ask BUY x3
        trades = [
            {"symbol": symbol, "tf": tf, "ts": ts0+1, "price": base + 0.25, "size": 2},
            {"symbol": symbol, "tf": tf, "ts": ts0+2, "price": base,         "size": 1},
            {"symbol": symbol, "tf": tf, "ts": ts0+3, "price": base + 0.24,  "size": 3},
        ]
        await publish(nc, IN_TRD, trades)

        got = []
        for _ in range(len(trades)):
            d = await asyncio.wait_for(q.get(), timeout=5.0)
            got.append(d)

        _p(f"[ASSERT] received {len(got)} snapshots, last={got[-1]}")

        assert got and got[-1]["indicator"] == "orderflow", "último mensaje debe ser orderflow"
        # delta esperado: +2 -1 +3 = +4
        assert abs(got[-1]["delta"] - 4.0) < 1e-9
        assert abs(got[-1]["buy"] - 5.0) < 1e-9
        assert abs(got[-1]["sell"] - 1.0) < 1e-9
    finally:
        await sub_out.unsubscribe()
        await nc.flush()
