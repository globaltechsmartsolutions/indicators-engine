# tests/integration/test_adx_pipeline.py
import asyncio
import orjson
import pytest

# ---------- Helpers base (mismos que RSI) ----------

async def subscribe_out(nc, subject: str, symbol: str, tf: str):
    q = asyncio.Queue()

    async def out_cb(msg):
        try:
            data = orjson.loads(msg.data)
        except Exception as e:
            print(f"❌ [OUT_CB] JSON error: {e}")
            return
        if data.get("symbol") == symbol and data.get("tf") == tf:
            await q.put(data)

    sub = await nc.subscribe(subject, cb=out_cb)
    await nc.flush()
    await asyncio.sleep(0.02)
    return sub, q

async def publish_candles(nc, in_subject: str, make_candles_fn, *, n, tf, price0, amplitude, pattern, seed, symbol):
    for i, c in enumerate(make_candles_fn(n=n, tf=tf, price0=price0, amplitude=amplitude,
                                          pattern=pattern, seed=seed, symbol=symbol), 1):
        await nc.publish(in_subject, orjson.dumps(c))
        if i == 1:
            first = c
    await nc.flush()
    await asyncio.sleep(0.02)
    return first  # útil para construir ruido compatible

async def collect_messages(q: asyncio.Queue, expected_min: int, timeout_sec: float = 12.0, label="ADX"):
    got = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec
    while len(got) < expected_min and loop.time() < deadline:
        try:
            d = await asyncio.wait_for(q.get(), timeout=0.5)
            got.append(d)
        except asyncio.TimeoutError:
            pass
    return got

# ---------- Helpers extra opcionales ----------

async def publish_noise(nc, in_subject: str, base_candle: dict):
    """
    Publica 4 velas que NO deben afectar a la salida:
      - otro símbolo
      - otro tf
    """
    noise = []
    # misma estructura, distinto symbol/tf
    for sym, tf in [("FAKE", base_candle["tf"]), (base_candle["symbol"], "5m")]:
        d = dict(base_candle)
        d["symbol"] = sym
        d["tf"] = tf
        d["ts"] = int(base_candle["ts"]) + 1  # avanzar un poquito
        noise.append(d)

    # publicar dos veces (sube probabilidad de colarse si el filtro está mal)
    for d in noise * 2:
        await nc.publish(in_subject, orjson.dumps(d))
    await nc.flush()

async def publish_out_of_order(nc, in_subject: str, last_good: dict):
    """
    Publica 1 vela con ts repetido (out-of-order). No debería causar emisión.
    """
    bad = dict(last_good)
    bad["ts"] = int(last_good["ts"]) - 60_000  # retrocede 1 minuto (suponiendo tf=1m)
    await nc.publish(in_subject, orjson.dumps(bad))
    await nc.flush()

def assert_adx_payload(msg: dict, *, period: int):
    ind = (msg.get("indicator") or "").lower()
    assert ind.startswith("adx"), f"Indicator inesperado: {ind!r}"
    assert msg.get("symbol") and msg.get("tf"), f"Meta inválida: {msg}"
    # rangos
    adx_val = float(msg.get("value", msg.get("adx")))
    assert 0.0 <= adx_val <= 100.0
    if "plus_di" in msg:
        pdi = float(msg["plus_di"]); assert 0.0 <= pdi <= 100.0
    if "minus_di" in msg:
        mdi = float(msg["minus_di"]); assert 0.0 <= mdi <= 100.0
    if "dx" in msg:
        dx = float(msg["dx"]); assert 0.0 <= dx <= 100.0

# ---------- Test de integración ADX ----------

@pytest.mark.asyncio
async def test_adx_pipeline(nc, cfg, make_candles_fn, adx_worker):
    """
    Flujo: IN_SUBJ (velas) -> worker ADX -> OUT_SUBJ.
    Extras:
      - ruido de otro símbolo/TF (no debe colarse)
      - out-of-order en IN (no debe emitir)
      - validación de warm-up y monotonía temporal
      - alternancia DI si pattern=randomwalk
    """
    IN_SUBJ  = cfg["in_subj"]
    OUT_SUBJ = adx_worker  # subject publicado por el fixture adx_worker
    symbol, tf = cfg["symbol"], cfg["tf"]
    n, period  = cfg["n"], cfg["adx_period"]
    amplitude  = cfg.get("amplitude", cfg.get("ampl"))
    pattern    = cfg["pattern"]

    sub_out, q = await subscribe_out(nc, OUT_SUBJ, symbol, tf)
    try:
        # 1) Publica velas buenas
        first_candle = await publish_candles(
            nc, IN_SUBJ, make_candles_fn,
            n=n, tf=tf, price0=cfg["price"], amplitude=amplitude,
            pattern=pattern, seed=cfg["seed"], symbol=symbol
        )

        # 2) Publica ruido (otro symbol/TF) que no debe afectar
        await publish_noise(nc, IN_SUBJ, first_candle)

        # 3) Espera mínimos según warm-up (Wilder ≈ 2*period - 1)
        warmup = max(0, 2 * period - 1)
        expected_min = max(0, n - warmup)
        if expected_min == 0:
            pytest.skip(f"No se espera ADX porque n ({n}) <= warmup ({warmup})")

        got = await collect_messages(q, expected_min, timeout_sec=12.0, label="ADX")

        # 4) Inyecta 1 out-of-order y confirma que no llega nada adicional por eso
        prev_len = len(got)
        if got:
            await publish_out_of_order(nc, IN_SUBJ, last_good={"symbol": symbol, "tf": tf, "ts": got[-1]["ts"],
                                                               "open": cfg["price"], "high": cfg["price"]+0.1,
                                                               "low": cfg["price"]-0.1, "close": cfg["price"],
                                                               "volume": 123})
            extra = await collect_messages(q, 1, timeout_sec=0.8, label="ADX")
            assert not extra, "Llegó ADX a raíz de una vela out-of-order"

        # 5) Asserts de cantidad
        assert len(got) >= expected_min, f"Esperaba ≥{expected_min} ADX; llegaron {len(got)}"
        assert got, "No llegó ningún ADX"

        # 6) Payload y monotonía de ts
        last_ts = None
        for m in got:
            assert m.get("symbol") == symbol and m.get("tf") == tf
            assert_adx_payload(m, period=period)
            if "ts" in m:
                t = int(m["ts"])
                if last_ts is not None:
                    assert t >= last_ts, f"ts retrocede: {t} < {last_ts}"
                last_ts = t

        # 7) Warm-up razonable: primer ADX no demasiado pronto (si el worker es Wilder)
        # (Si usas method="ema" en el worker, relaja o comenta este check.)
        assert len(got) <= n - period + 2, "ADX empezó demasiado pronto para Wilder (¿estás usando EMA?)"

        # 8) Alternancia DI si el patrón es randomwalk
        if pattern == "randomwalk":
            has_plus  = any(float(m.get("plus_di", 0.0))  > 1.0 for m in got)
            has_minus = any(float(m.get("minus_di", 0.0)) > 1.0 for m in got)
            assert has_plus and has_minus, "No se observan alternancias de DI en randomwalk"

    finally:
        await sub_out.unsubscribe()
        await nc.flush()
