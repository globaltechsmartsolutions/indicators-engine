# tests/integration/test_volume_profile_pipeline.py

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

async def publish_trades(nc, subject: str, trades):
    _p(f"[IN][PUB] subject={subject} n={len(trades)}")
    for t in trades:
        _p(f"[IN][PUB] -> {t}")
        await nc.publish(subject, orjson.dumps(t))
    await nc.flush()
    await asyncio.sleep(0.02)

@pytest.mark.asyncio
async def test_volume_profile_pipeline(nc, cfg, volume_profile_worker):
    """
    dxFeed/Bookmap-like: cada trade actualiza el bucket de su minuto.
    Si el 3er trade cae en el minuto siguiente, esperamos que 'ts' salte al bucket nuevo.
    """
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = volume_profile_worker
    symbol   = cfg.get("symbol") or cfg.get("candle_symbol") or "ESZ5"
    tf       = cfg.get("tf") or cfg.get("candle_tf") or "1m"
    base     = int(cfg.get("ts0", 1_700_000_000_000))
    price0   = float(cfg.get("price", cfg.get("candle_price", 100.0)))

    bucket_ts = (base // 60_000) * 60_000

    _p(f"[TEST] IN_SUBJ={IN_SUBJ} OUT_SUBJ={OUT_SUBJ} symbol={symbol} tf={tf} bucket_ts={bucket_ts}")

    sub_out, q = await subscribe_out(nc, OUT_SUBJ, symbol, tf)

    try:
        # Deja el 3er trade cruzar al minuto siguiente para reproducir dxFeed/Bookmap
        trades = [
            {"symbol": symbol, "tf": tf, "ts": base + 1_000,  "price": price0,       "size": 2},
            {"symbol": symbol, "tf": tf, "ts": base + 20_000, "price": price0+0.25,  "size": 1},
            {"symbol": symbol, "tf": tf, "ts": base + 40_000, "price": price0+0.25,  "size": 2},
        ]
        await publish_trades(nc, IN_SUBJ, trades)

        got = []
        for _ in range(len(trades)):
            d = await asyncio.wait_for(q.get(), timeout=5.0)
            got.append(d)

        _p(f"[ASSERT] received {len(got)} snapshots")

        # 1º y 2º snapshot del bucket inicial
        assert got[0]["indicator"] == "vp"
        assert got[1]["indicator"] == "vp"
        assert got[0]["ts"] == bucket_ts
        assert got[1]["ts"] == bucket_ts
        # 3º snapshot ya del bucket siguiente
        assert got[2]["indicator"] == "vp"
        assert got[2]["ts"] == bucket_ts + 60_000

        # Chequeos rápidos de bins/volumen como en tu log
        assert got[0]["vtotal"] == 2.0 and got[0]["poc"] == price0
        assert got[1]["vtotal"] == 3.0 and got[1]["poc"] in (price0, price0+0.25)
        assert got[2]["vtotal"] == 2.0 and got[2]["poc"] == price0+0.25

    finally:
        await sub_out.unsubscribe()
        await nc.flush()
