
import asyncio
import orjson
import pytest

from indicators_engine.pipelines.svp import SvpCalc

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

def make_trades_for_svp(symbol: str, tf: str, ts0: int, price0: float):
    # Genera trades para que el POC quede en price0+0.25 con volumen 3 vs 2
    trades = [
        {"symbol": symbol, "tf": tf, "ts": ts0,   "price": price0,      "size": 2},
        {"symbol": symbol, "tf": tf, "ts": ts0+1, "price": price0+0.25, "size": 1},
        {"symbol": symbol, "tf": tf, "ts": ts0+2, "price": price0+0.25, "size": 2},
    ]
    _p(f"[IN][GEN] trades={trades}")
    return trades

async def publish_trades(nc, subject: str, trades):
    _p(f"[IN][PUB] subject={subject} n={len(trades)}")
    for t in trades:
        _p(f"[IN][PUB] -> {t}")
        await nc.publish(subject, orjson.dumps(t))
    await nc.flush()
    await asyncio.sleep(0.02)

@pytest.mark.asyncio
async def test_svp_pipeline(nc, cfg):
    """
    Integra un worker efímero que consume trades (como dxFeed) y publica SVP snapshots.
    """
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = "indicators.svp"

    tick_size = float(cfg.get("poc_tick_size", 0.25))  # reutilizamos setting si existe
    svp = SvpCalc(tick_size=tick_size, reset_daily=True)

    async def on_trade_msg(msg):
        try:
            d = orjson.loads(msg.data)
            _p(f"[IN][RX] {d}")
        except Exception as e:
            _p(f"[IN][RX][ERR] json decode: {e}")
            return

        symbol = d.get("symbol") or d.get("eventSymbol")
        tf     = d.get("tf") or cfg.get("tf") or "-"
        ts     = d.get("ts") or d.get("time")
        # dxFeed nanos → ms
        if isinstance(ts, (int, float)) and ts > 10**15:
            _p(f"[IN][RX] nanos->ms before: {ts}")
            ts = int(ts) // 1_000_000
            _p(f"[IN][RX] nanos->ms after : {ts}")
        ts = int(ts) if ts is not None else None

        price = d.get("price")
        size  = d.get("size", d.get("quantity", 0))
        if symbol is None or ts is None or price is None or size is None:
            _p("[IN][RX][IGN] missing field -> skip")
            return

        snap = svp.on_trade(symbol=symbol, ts=ts, price=float(price), size=float(size), tf=tf)
        _p(f"[SVP][SNAP] ts={ts} poc={snap['poc']} vtotal={snap['vtotal']} bins={snap['bins']}")

        out = {
            "v": 1,
            "source": "indicators-engine",
            "symbol": symbol,
            "tf": tf,
            "ts": ts,
            "indicator": "svp",
            "tick_size": tick_size,
            "poc": snap["poc"],
            "vtotal": snap["vtotal"],
            "bins": snap["bins"],
            "id": f"{symbol}|{tf}|{ts}|svp",
        }
        _p(f"[OUT][PUB] -> {out}")
        await nc.publish(OUT_SUBJ, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    _p(f"[IN][SUB] subject={IN_SUBJ}")
    sub_in = await nc.subscribe(IN_SUBJ, cb=on_trade_msg)
    await nc.flush()

    sub_out, q = await subscribe_out(nc, OUT_SUBJ, cfg["symbol"], cfg["tf"])
    try:
        trades = make_trades_for_svp(cfg["symbol"], cfg["tf"], int(cfg.get("ts0", 1_700_000_000_000)), float(cfg.get("price", 100.0)))
        await publish_trades(nc, IN_SUBJ, trades)

        got = []
        for _ in range(len(trades)):
            d = await asyncio.wait_for(q.get(), timeout=5.0)
            got.append(d)

        _p(f"[ASSERT] received {len(got)} snapshots, last={got[-1]}")
        assert got and got[-1]["indicator"] == "svp", "último mensaje debe ser SVP"
        assert abs(got[-1]["poc"] - (float(cfg.get("price", 100.0)) + 0.25)) < 1e-9, "POC final esperado price+0.25"
        assert abs(got[-1]["vtotal"] - 5.0) < 1e-9, "vtotal esperado 5.0 (2+1+2)"
        prices = [b["price"] for b in got[-1]["bins"]]
        vols = [b["volume"] for b in got[-1]["bins"]]
        _p(f"[ASSERT] bins prices={prices} vols={vols}")
        assert set(prices) == {float(cfg.get("price", 100.0)), float(cfg.get("price", 100.0)) + 0.25}
        assert set(vols) == {2.0, 3.0}
    finally:
        _p("[CLEANUP] unsubscribe ...")
        await sub_in.unsubscribe()
        await sub_out.unsubscribe()
        await nc.flush()
