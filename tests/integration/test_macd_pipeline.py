# tests/test_macd_pipeline.py
import asyncio
import orjson
import pytest
import math

@pytest.mark.asyncio
async def test_macd_pipeline(nc, cfg, make_candles_fn, macd_worker):
    IN_SUBJ  = cfg["in_subj"]
    OUT_SUBJ = macd_worker  # subject exacto que devuelve el fixture del worker MACD
    symbol, tf = cfg["symbol"], cfg["tf"]
    n = cfg["n"]

    print(f"\n[TEST] nats_url={cfg['nats_url']}")
    print(f"[TEST] IN_SUBJ={IN_SUBJ}")
    print(f"[TEST] OUT_SUBJ={OUT_SUBJ}")
    print(f"[TEST] symbol={symbol} tf={tf} n={n}")

    q = asyncio.Queue()

    async def out_cb(msg):
        print(f"📥 [OUT_CB] msg.subject={msg.subject} data={msg.data!r}")
        try:
            data = orjson.loads(msg.data)
        except Exception as e:
            print(f"❌ [OUT_CB] JSON error: {e}")
            return
        if data.get("symbol") == symbol and data.get("tf") == tf:
            print(f"✅ [OUT_CB] match {symbol}/{tf} -> enqueue")
            await q.put(data)
        else:
            print(f"⚠️  [OUT_CB] ignore (symbol/tf no match): {data}")

    print("[TEST] subscribing OUT...")
    sub_out = await nc.subscribe(OUT_SUBJ, cb=out_cb)
    await nc.flush()
    print("[TEST] subscribed ✔, flushing…")
    await asyncio.sleep(0.05)

    amplitude = cfg.get("amplitude", cfg.get("ampl"))
    print(f"[TEST] publishing {n} candles to {IN_SUBJ} (ampl={amplitude})")
    for i, c in enumerate(
            make_candles_fn(
                n=n, tf=tf, price0=cfg["price"], amplitude=amplitude,
                pattern=cfg["pattern"], seed=cfg["seed"], symbol=symbol
            ), 1
    ):
        await nc.publish(IN_SUBJ, orjson.dumps(c))
        if i <= 5 or i == n:
            print(f"➡️  [PUB {i}/{n}] {c}")
        elif i == 6:
            print("… (silenciando logs de publish hasta la última) …")
    await nc.flush()
    print("[TEST] all candles published + flush ✔")
    await asyncio.sleep(0.05)

    # MACD:
    # 1ª barra inicializa EMAs; 2ª “primea” la signal; comienza a emitir desde la 3ª
    expected_min = max(0, n - 2)
    print(f"[TEST] expecting at least {expected_min} MACD msgs")

    got = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 8.0  # un poco más de margen
    poll = 0
    while len(got) < expected_min and loop.time() < deadline:
        poll += 1
        try:
            d = await asyncio.wait_for(q.get(), timeout=0.75)
            print(f"🎯 [DEQ {len(got)+1}/{expected_min}] {d}")
            got.append(d)
        except asyncio.TimeoutError:
            print(f"⏳ [WAIT poll={poll}] queue empty, retrying… (have {len(got)}/{expected_min})")

    print("[TEST] unsubscribing OUT…")
    await sub_out.unsubscribe()
    await nc.flush()
    print("[TEST] unsubscribed ✔, flush ✔")

    print(f"📊 [RESULT] received={len(got)} (expected ≥ {expected_min}) last={got[-1] if got else None}")

    assert len(got) >= expected_min, f"Esperaba ≥{expected_min} MACD; llegaron {len(got)}"
    last = got[-1]
    for k in ("macd", "signal", "hist"):
        assert k in last, f"Falta clave '{k}' en {last}"
        assert isinstance(last[k], (int, float)) and math.isfinite(last[k]), f"Valor inválido en '{k}': {last[k]}"
    assert abs(last["hist"] - (last["macd"] - last["signal"])) < 1e-9
