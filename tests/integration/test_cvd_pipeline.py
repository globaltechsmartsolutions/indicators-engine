# tests/integration/test_cvd_pipeline.py
import asyncio
import orjson
import pytest

# ---------- Helpers compartidos (basados en test_rsi_pipeline) ----------

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


async def collect_messages(q: asyncio.Queue, expected_min: int, timeout_sec: float = 8.0, label="CVD"):
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


def assert_cvd_payload(msg: dict):
    """Valida que el payload CVD tenga formato esperado."""
    for k in ("symbol", "tf", "ts", "indicator", "value"):
        assert k in msg, f"Falta clave '{k}' en {msg}"
    assert msg["indicator"].lower() in ("cvd", "cumulative_volume_delta")
    # value puede ser cualquier float (positivo/negativo)
    float(msg["value"])


# ---------- Generador de trades dummy ----------

def make_trades(*, n: int, ts0: int, symbol: str, tf: str, price0: float):
    """
    Genera una lista de trades de ejemplo con mezcla de casos:
    - con aggressor BUY/SELL
    - cruce por bid/ask
    - en el spread (tick rule / memoria)
    """
    trades = []
    ts = ts0
    price = price0

    # 1) Arranque con aggressor explícito (BUY)
    trades.append({"symbol": symbol, "tf": tf, "ts": ts, "price": price, "size": 2, "aggressor": "BUY"})
    ts += 1

    # 2) Cruce ask (BUY)
    trades.append({"symbol": symbol, "tf": tf, "ts": ts, "price": price+0.25, "size": 3,
                   "bid": price+0.00, "ask": price+0.25})
    ts += 1

    # 3) Cruce bid (SELL)
    trades.append({"symbol": symbol, "tf": tf, "ts": ts, "price": price+0.00, "size": 1,
                   "bid": price+0.00, "ask": price+0.25})
    ts += 1

    # 4) Spread (sube vs last → BUY)
    trades.append({"symbol": symbol, "tf": tf, "ts": ts, "price": price+0.125, "size": 2,
                   "bid": price+0.00, "ask": price+0.25})
    ts += 1

    # 5) aggressor SELL
    trades.append({"symbol": symbol, "tf": tf, "ts": ts, "price": price+0.125, "size": 4, "side": "SELL"})
    ts += 1

    # 6) Repite precio sin info → usa memoria
    trades.append({"symbol": symbol, "tf": tf, "ts": ts, "price": price+0.125, "size": 5})
    ts += 1

    # 7..n) alterna BUY/SELL con bid/ask
    for i in range(len(trades), n):
        if i % 2 == 0:
            # BUY por ask
            price += 0.25
            trades.append({"symbol": symbol, "tf": tf, "ts": ts, "price": price, "size": 1,
                           "bid": price-0.25, "ask": price})
        else:
            # SELL por bid
            price -= 0.25
            trades.append({"symbol": symbol, "tf": tf, "ts": ts, "price": price, "size": 1,
                           "bid": price, "ask": price+0.25})
        ts += 1

    return trades


async def publish_trades(nc, in_subject: str, trades):
    print(f"[TEST] publishing {len(trades)} trades to {in_subject}")
    for i, t in enumerate(trades, 1):
        await nc.publish(in_subject, orjson.dumps(t))
        if i <= 5 or i == len(trades):
            print(f"➡️  [PUB {i}/{len(trades)}] {t}")
        elif i == 6:
            print("… (silenciando logs de publish hasta la última) …")
    await nc.flush()
    print("[TEST] all trades published + flush ✔")
    await asyncio.sleep(0.05)


# ---------- Test de integración ----------

@pytest.mark.asyncio
async def test_cvd_pipeline(nc, cfg, cvd_worker):
    """
    Integra: publicamos trades en IN_SUBJ, el worker CVD publica en OUT_SUBJ.
    Requisitos de fixtures (similares a RSI):
      - nc: cliente NATS conectado
      - cfg: dict con configuración base (usamos symbol, tf, n, nats_url; opcional in_trades_subj/in_subj)
      - cvd_worker: subject OUT que publica el worker de CVD
    """
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = cvd_worker
    symbol, tf = cfg["symbol"], cfg["tf"]
    n = cfg.get("n_trades", cfg.get("n", 12))
    price0 = float(cfg.get("price", 6480.0))
    ts0 = int(cfg.get("ts0", 1_700_000_000_000))

    print(f"\n[TEST] nats_url={cfg['nats_url']}")
    print(f"[TEST] IN_SUBJ={IN_SUBJ}")
    print(f"[TEST] OUT_SUBJ={OUT_SUBJ}")
    print(f"[TEST] symbol={symbol} tf={tf} n_trades={n}")

    # 1) Suscribirse al OUT
    sub_out, q = await subscribe_out(nc, OUT_SUBJ, symbol, tf)

    try:
        # 2) Publicar trades al IN
        trades = make_trades(n=n, ts0=ts0, symbol=symbol, tf=tf, price0=price0)
        await publish_trades(nc, IN_SUBJ, trades)

        # 3) Esperar CVD (1 msg por trade esperado)
        expected_min = len(trades)
        got = await collect_messages(q, expected_min, timeout_sec=10.0, label="CVD")
        print(f"📊 [RESULT] received={len(got)} (expected ≥ {expected_min}) last={got[-1] if got else None}")

        # 4) Asserts básicos
        assert len(got) >= expected_min, f"Esperaba ≥{expected_min} CVD; llegaron {len(got)}"
        assert got, "No llegó ningún CVD"
        for m in (got[0], got[-1]):
            assert_cvd_payload(m)

    finally:
        # 5) Limpieza
        print("[TEST] unsubscribing OUT…")
        await sub_out.unsubscribe()
        await nc.flush()
        print("[TEST] unsubscribed ✔, flush ✔")
