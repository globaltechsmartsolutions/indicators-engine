# tests/integration/test_adx_pipeline.py
import os
import asyncio
import orjson
import pytest

from indicators_engine.indicators.classic.adx import ADXConfig

DEBUG = os.getenv("ADX_INT_DEBUG") == "1"

def _log(msg: str):
    if DEBUG:
        print(msg)

def _throttle_log_bar(i: int, n: int, kind: str, payload: dict | None):
    """Imprime poco: primeras 3, cada 40 y últimas 2."""
    if not DEBUG or payload is None:
        return
    if i <= 3 or i % 40 == 0 or i > n - 2:
        _log(f"[{kind} {i}/{n}] ADX={float(payload.get('value', payload.get('adx', float('nan')))):.2f} "
             f"+DI={float(payload.get('plus_di', float('nan'))):.2f} "
             f"-DI={float(payload.get('minus_di', float('nan'))):.2f} "
             f"DX={float(payload.get('dx', float('nan'))):.2f} ts={payload.get('ts','-')}")

# ---------- Helpers base (mismos que RSI) ----------

async def subscribe_out(nc, subject: str, symbol: str, tf: str):
    q = asyncio.Queue()

    async def out_cb(msg):
        try:
            data = orjson.loads(msg.data)
        except Exception as e:
            _log(f"❌ [OUT_CB] JSON error: {e}")
            return
        if data.get("symbol") == symbol and data.get("tf") == tf:
            await q.put(data)

    _log(f"[SUB] -> {subject} (filter {symbol}/{tf})")
    sub = await nc.subscribe(subject, cb=out_cb)
    await nc.flush()
    await asyncio.sleep(0.02)
    _log("[SUB] ready ✔")
    return sub, q

async def publish_candles(nc, in_subject: str, make_candles_fn, *, n, tf, price0, amplitude, pattern, seed, symbol):
    _log(f"[PUB] {n} candles to {in_subject} (tf={tf} ampl={amplitude} pattern={pattern} seed={seed})")
    first = None
    for i, c in enumerate(make_candles_fn(n=n, tf=tf, price0=price0, amplitude=amplitude,
                                          pattern=pattern, seed=seed, symbol=symbol), 1):
        await nc.publish(in_subject, orjson.dumps(c))
        if i == 1:
            first = c
        if DEBUG and (i <= 2 or i == n):
            _log(f"  ↳ [PUB {i}/{n}] {c}")
    await nc.flush()
    await asyncio.sleep(0.02)
    _log("[PUB] flush ✔")
    return first  # útil para construir ruido compatible

async def collect_messages(q: asyncio.Queue, expected_min: int, timeout_sec: float = 12.0, label="ADX"):
    got = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec
    polls = 0
    _log(f"[COL] waiting ≥{expected_min} {label} (timeout={timeout_sec}s)")
    while len(got) < expected_min and loop.time() < deadline:
        polls += 1
        try:
            d = await asyncio.wait_for(q.get(), timeout=0.5)
            got.append(d)
            _throttle_log_bar(len(got), expected_min, "DEQ", d)
        except asyncio.TimeoutError:
            if DEBUG:
                _log(f"[COL] … ({len(got)}/{expected_min})")
    # Drenar restos que ya estuvieran en cola
    while True:
        try:
            d = await asyncio.wait_for(q.get(), timeout=0.05)
            got.append(d)
            _throttle_log_bar(len(got), expected_min, "DEQ", d)
        except asyncio.TimeoutError:
            break
    _log(f"[COL] done: got={len(got)}")
    return got

# ---------- Helpers extra opcionales ----------

async def publish_noise(nc, in_subject: str, base_candle: dict):
    """Publica 4 velas que NO deben afectar a la salida: otro símbolo y otro tf."""
    _log("[NOISE] publishing 4 noise candles (other symbol/tf)")
    noise = []
    for sym, tf in [("FAKE", base_candle["tf"]), (base_candle["symbol"], "5m")]:
        d = dict(base_candle)
        d["symbol"] = sym
        d["tf"] = tf
        d["ts"] = int(base_candle["ts"]) + 1
        noise.append(d)
    for d in noise * 2:
        await nc.publish(in_subject, orjson.dumps(d))
    await nc.flush()
    _log("[NOISE] flush ✔")

async def publish_out_of_order(nc, in_subject: str, last_good: dict):
    """Publica 1 vela con ts repetido/menor. No debería causar emisión."""
    _log("[OOO] publish out-of-order candle (ts back 60s)")
    bad = dict(last_good)
    bad["ts"] = int(last_good["ts"]) - 60_000
    await nc.publish(in_subject, orjson.dumps(bad))
    await nc.flush()
    _log("[OOO] flush ✔")

def assert_adx_payload(msg: dict, *, period: int):
    ind = (msg.get("indicator") or "").lower()
    assert ind.startswith("adx"), f"Indicator inesperado: {ind!r}"
    assert msg.get("symbol") and msg.get("tf"), f"Meta inválida: {msg}"
    # tolerancia para flotantes
    EPS = 1e-9
    adx_val = float(msg.get("value", msg.get("adx")))
    assert -EPS <= adx_val <= 100.0 + EPS, f"ADX fuera de rango: {adx_val}"
    if "plus_di" in msg:
        pdi = float(msg["plus_di"]); assert -EPS <= pdi <= 100.0 + EPS, f"+DI fuera de rango: {pdi}"
    if "minus_di" in msg:
        mdi = float(msg["minus_di"]); assert -EPS <= mdi <= 100.0 + EPS, f"-DI fuera de rango: {mdi}"
    if "dx" in msg:
        dx = float(msg["dx"]); assert -EPS <= dx <= 100.0 + EPS, f"DX fuera de rango: {dx}"

# ---------- Test de integración ADX ----------

@pytest.mark.asyncio
async def test_adx_pipeline(nc, cfg, make_candles_fn, adx_worker):
    IN_SUBJ  = cfg["in_subj"]
    OUT_SUBJ = adx_worker
    symbol, tf = cfg["symbol"], cfg["tf"]
    n, period  = cfg["n"], cfg["adx_period"]
    amplitude  = cfg.get("amplitude", cfg.get("ampl"))
    pattern    = cfg["pattern"]

    _log(f"[TEST] tf={tf} symbol={symbol} n={n} period={period} subj_out={OUT_SUBJ}")

    sub_out, q = await subscribe_out(nc, OUT_SUBJ, symbol, tf)
    try:
        # 1) Buenas
        first_candle = await publish_candles(
            nc, IN_SUBJ, make_candles_fn,
            n=n, tf=tf, price0=cfg["price"], amplitude=amplitude,
            pattern=pattern, seed=cfg["seed"], symbol=symbol
        )

        # 2) Ruido
        await publish_noise(nc, IN_SUBJ, first_candle)

        # 3) Warm-up y colecta
        adx_cfg = ADXConfig(period=period)
        warmup = max(0, adx_cfg.period + adx_cfg.warmup_extra)
        expected_min = max(0, n - warmup)
        if expected_min == 0:
            pytest.skip(f"No se espera ADX porque n ({n}) <= warmup ({warmup})")

        got = await collect_messages(q, expected_min, timeout_sec=12.0, label="ADX")

        # 4) Out-of-order
        if got:
            await publish_out_of_order(nc, IN_SUBJ, last_good={
                "symbol": symbol, "tf": tf, "ts": got[-1]["ts"],
                "open": cfg["price"], "high": cfg["price"]+0.1,
                "low": cfg["price"]-0.1, "close": cfg["price"], "volume": 123
            })
            extra = await collect_messages(q, 1, timeout_sec=0.8, label="ADX")
            _log(f"[OOO] extra after out-of-order: {len(extra)}")
            assert not extra, "Llegó ADX a raíz de una vela out-of-order"

        # 5) Cantidad
        _log(f"[RES] got={len(got)} expected_min={expected_min}")
        assert len(got) >= expected_min, f"Esperaba ≥{expected_min} ADX; llegaron {len(got)}"
        assert got, "No llegó ningún ADX"

        # 6) Payload + monotonic ts (y trazas throttle de los emitidos)
        last_ts = None
        for i, m in enumerate(got, 1):
            assert m.get("symbol") == symbol and m.get("tf") == tf
            assert_adx_payload(m, period=period)
            if "ts" in m:
                t = int(m["ts"])
                if last_ts is not None:
                    assert t >= last_ts, f"ts retrocede: {t} < {last_ts}"
                last_ts = t
            _throttle_log_bar(i, len(got), "OUT", m)

        # 7) Warm-up razonable (Wilder)
        assert len(got) <= n - period + 2, "ADX empezó demasiado pronto para Wilder (¿estás usando EMA?)"

        # 8) Alternancia DI si randomwalk
        if pattern == "randomwalk":
            has_plus  = any(float(m.get("plus_di", 0.0))  > 1.0 for m in got)
            has_minus = any(float(m.get("minus_di", 0.0)) > 1.0 for m in got)
            _log(f"[ALT] +DI? {has_plus}  -DI? {has_minus}")
            assert has_plus and has_minus, "No se observan alternancias de DI en randomwalk"

        _log("[TEST] OK ✔")

    finally:
        await sub_out.unsubscribe()
        await nc.flush()
