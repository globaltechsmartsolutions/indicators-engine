import asyncio
import orjson
import pytest

@pytest.mark.asyncio
async def test_rsi_pipeline(nc, cfg, make_candles_fn, rsi_worker):
    IN_SUBJ  = cfg["in_subj"]
    OUT_SUBJ = rsi_worker
    symbol, tf = cfg["symbol"], cfg["tf"]
    n, period  = cfg["n"], cfg["rsi_period"]

    q = asyncio.Queue()

    async def out_cb(msg):
        print(f"📥 Callback recibió en {msg.subject}: {msg.data!r}")
        try:
            data = orjson.loads(msg.data)
        except Exception as e:
            print(f"❌ Error parseando JSON: {e}")
            return
        if data.get("symbol") == symbol and data.get("tf") == tf:
            print(f"✅ Matched {symbol}/{tf}, encolando: {data}")
            await q.put(data)
        else:
            print(f"⚠️ Ignorado mensaje no matching: {data}")

    # Suscripción y flush para asegurar registro
    sub_out = await nc.subscribe(OUT_SUBJ, cb=out_cb)
    await nc.flush()
    print(f"👂 Suscrito a {OUT_SUBJ}")

    # Publica velas dummy
    amplitude = cfg.get("amplitude", cfg.get("ampl"))
    for i, c in enumerate(
            make_candles_fn(
                n=n,
                tf=tf,
                price0=cfg["price"],
                amplitude=amplitude,
                pattern=cfg["pattern"],
                seed=cfg["seed"],
                symbol=symbol,
            ),
            1,
    ):
        await nc.publish(IN_SUBJ, orjson.dumps(c))
        print(f"➡️  Candle {i}/{n} publicada: {c}")
    await nc.flush()
    print("🚀 Todas las velas publicadas y flush enviado")

    expected_min = max(0, n - period)
    got, loop = [], asyncio.get_running_loop()
    deadline = loop.time() + 6.0

    # Si no se espera ningún RSI (n <= period), no tiene sentido validar
    if expected_min == 0:
        await sub_out.unsubscribe()
        await nc.flush()
        pytest.skip(f"No se espera RSI porque n ({n}) <= period ({period})")

    print(f"⌛ Esperando al menos {expected_min} mensajes RSI...")
    while len(got) < expected_min and loop.time() < deadline:
        try:
            d = await asyncio.wait_for(q.get(), timeout=0.5)
            print(f"🎯 Consumido de la cola: {d}")
            got.append(d)
        except asyncio.TimeoutError:
            print("⏳ Timeout 0.5s esperando en la cola, reintento...")

    # Unsubscribe y validaciones
    print("🔌 Haciendo unsubscribe...")
    await sub_out.unsubscribe()
    await nc.flush()
    print("✅ Unsubscribed y flush confirmado")

    print(f"📊 Recibidos {len(got)} mensajes, últimos: {got[-3:] if got else 'NINGUNO'}")

    assert len(got) >= expected_min, f"Esperaba ≥{expected_min} RSI; llegaron {len(got)}"
    assert got, "No llegó ningún RSI (lista vacía)"
    assert "value" in got[-1] and 0.0 <= got[-1]["value"] <= 100.0
