# tests/integration/test_rsi_pipeline.py
import asyncio
import orjson
import pytest

# ---------- Helpers compartidos ----------

async def subscribe_out(nc, subject: str, symbol: str, tf: str):
    """Suscribe al OUT_SUBJ y devuelve (subscription, queue)."""
    q = asyncio.Queue()

    async def out_cb(msg):
        print(f"📥 [OUT_CB] {msg.subject} data={msg.data!r}")
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

    print(f"[TEST] subscribing OUT {subject} …")
    sub = await nc.subscribe(subject, cb=out_cb)
    await nc.flush()
    print("[TEST] subscribed ✔")
    await asyncio.sleep(0.05)
    return sub, q


async def publish_candles(nc, in_subject: str, make_candles_fn, *, n, tf, price0, amplitude, pattern, seed, symbol):
    """Publica n velas dummy en IN_SUBJ."""
    print(f"[TEST] publishing {n} candles to {in_subject} (ampl={amplitude})")
    for i, c in enumerate(
            make_candles_fn(n=n, tf=tf, price0=price0, amplitude=amplitude, pattern=pattern, seed=seed, symbol=symbol), 1
    ):
        await nc.publish(in_subject, orjson.dumps(c))
        if i <= 5 or i == n:
            print(f"➡️  [PUB {i}/{n}] {c}")
        elif i == 6:
            print("… (silenciando logs de publish hasta la última) …")
    await nc.flush()
    print("[TEST] all candles published + flush ✔")
    await asyncio.sleep(0.05)


async def collect_messages(q: asyncio.Queue, expected_min: int, timeout_sec: float = 8.0, label="RSI"):
    """Espera hasta juntar al menos expected_min mensajes en la cola o hasta timeout."""
    got = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec
    poll = 0
    print(f"[TEST] expecting at least {expected_min} {label} msgs")
    while len(got) < expected_min and loop.time() < deadline:
        poll += 1
        try:
            d = await asyncio.wait_for(q.get(), timeout=0.75)
            got.append(d)
            print(f"🎯 [DEQ {len(got)}/{expected_min}] {d}")
        except asyncio.TimeoutError:
            print(f"⏳ [WAIT poll={poll}] queue empty, retrying… (have {len(got)}/{expected_min})")
    return got


def assert_rsi_payload(last_msg: dict):
    """Valida que el payload RSI tenga 'value' en [0, 100]."""
    assert "value" in last_msg, f"Falta clave 'value' en {last_msg}"
    v = float(last_msg["value"])
    assert 0.0 <= v <= 100.0, f"RSI fuera de rango: {v}"


# ---------- Test ----------

@pytest.mark.asyncio
async def test_rsi_pipeline(nc, cfg, make_candles_fn, rsi_worker):
    IN_SUBJ  = cfg["in_subj"]
    OUT_SUBJ = rsi_worker  # subject publicado por el fixture rsi_worker
    symbol, tf = cfg["symbol"], cfg["tf"]
    n, period  = cfg["n"], cfg["rsi_period"]
    amplitude  = cfg.get("amplitude", cfg.get("ampl"))

    print(f"\n[TEST] nats_url={cfg['nats_url']}")
    print(f"[TEST] IN_SUBJ={IN_SUBJ}")
    print(f"[TEST] OUT_SUBJ={OUT_SUBJ}")
    print(f"[TEST] symbol={symbol} tf={tf} n={n} period={period}")

    # 1) Suscribirse al OUT
    sub_out, q = await subscribe_out(nc, OUT_SUBJ, symbol, tf)

    try:
        # 2) Publicar velas al IN
        await publish_candles(
            nc, IN_SUBJ, make_candles_fn,
            n=n, tf=tf, price0=cfg["price"], amplitude=amplitude,
            pattern=cfg["pattern"], seed=cfg["seed"], symbol=symbol
        )

        # 3) RSI empieza a emitir tras 'period' cambios -> esperamos n - period
        expected_min = max(0, n - period)
        if expected_min == 0:
            pytest.skip(f"No se espera RSI porque n ({n}) <= period ({period})")

        got = await collect_messages(q, expected_min, timeout_sec=8.0, label="RSI")
        print(f"📊 [RESULT] received={len(got)} (expected ≥ {expected_min}) last={got[-1] if got else None}")

        # 4) Asserts
        assert len(got) >= expected_min, f"Esperaba ≥{expected_min} RSI; llegaron {len(got)}"
        assert got, "No llegó ningún RSI"
        assert_rsi_payload(got[-1])

    finally:
        # 5) Limpieza
        print("[TEST] unsubscribing OUT…")
        await sub_out.unsubscribe()
        await nc.flush()
        print("[TEST] unsubscribed ✔, flush ✔")
