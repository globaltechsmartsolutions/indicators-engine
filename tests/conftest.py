# tests/conftest.py
import time, re, random
import orjson
import asyncio, pytest, pytest_asyncio
from nats.aio.client import Client as NatsClient
from indicators_engine.pipelines.rsi import RsiCalc
from indicators_engine.pipelines.macd import MacdCalc
from indicators_engine.pipelines.adx import AdxCalc
from indicators_engine.pipelines.cvd import CvdCalc
from indicators_engine.pipelines.vwap import VwapCalc
from indicators_engine.pipelines.poc import PocCalc
# ------------------------- pytest.ini options -------------------------
def pytest_addoption(parser):
    parser.addini("nats_url", "NATS URL", default="nats://127.0.0.1:4222")
    # IN (velas y trades)
    parser.addini("candles_subject", "Subject IN candles", default="market.candles.1m")
    parser.addini("in_trades_subject", "Subject IN trades", default="market.trades")
    # OUT (indicadores)
    parser.addini("rsi_subject", "Subject OUT rsi",    default="indicators.candles.1m.rsi10")
    parser.addini("macd_subject", "Subject OUT macd",  default="indicators.candles.1m.macd")
    parser.addini("adx_subject", "Subject OUT adx",    default="indicators.candles.1m.adx14")
    parser.addini("out_cvd_subject",  "Subject OUT cvd",  default="indicators.cvd")
    parser.addini("out_vwap_subject", "Subject OUT vwap", default="indicators.vwap")
    # Serie de velas sintéticas
    parser.addini("candle_symbol", "Symbol", default="ESZ5")
    parser.addini("candle_tf", "TF", default="1m")
    parser.addini("candle_n", "N candles", default="20")
    parser.addini("candle_price", "Start price", default="648.0")
    parser.addini("candle_amplitude", "Amplitude", default="0.35")
    parser.addini("candle_pattern", "Pattern", default="zigzag")
    parser.addini("candle_seed", "Seed", default="1")
    # Parámetros indicadores
    parser.addini("rsi_period", "RSI period", default="14")
    parser.addini("adx_period", "ADX period", default="30")
    parser.addini("cvd_reset_daily", "CVD reset diario", default="true")
    parser.addini("vwap_reset_daily", "VWAP reset diario", default="true")

# ------------------------- helpers -------------------------
def _tf_to_ms(tf: str) -> int:
    m = re.fullmatch(r"(\d+)([smhd])", tf)
    if not m:
        raise ValueError(f"TF inválido: {tf}")
    n, u = int(m.group(1)), m.group(2)
    mult = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[u]
    return n * mult

def _ini_bool(pytestconfig, key: str, default: bool) -> bool:
    raw = pytestconfig.getini(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")

# ------------------------- cfg fixture -------------------------
@pytest.fixture(scope="session")
def cfg(pytestconfig):
    get = pytestconfig.getini
    return {
        "nats_url":        get("nats_url"),
        "in_subj":         get("candles_subject"),
        "in_trades_subj":  get("in_trades_subject"),
        "out_subj_rsi":    get("rsi_subject"),
        "out_subj_macd":   get("macd_subject"),
        "out_subj_adx":    get("adx_subject"),
        "out_cvd_subj":    get("out_cvd_subject"),
        "out_vwap_subj":   get("out_vwap_subject"),
        "symbol":          get("candle_symbol"),
        "tf":              get("candle_tf"),
        "n":               int(get("candle_n")),
        "price":           float(get("candle_price")),
        "amplitude":       float(get("candle_amplitude")),
        "pattern":         get("candle_pattern"),
        "seed":            int(get("candle_seed")),
        "rsi_period":      int(get("rsi_period")),
        "adx_period":      int(get("adx_period")),
        "cvd_reset_daily": _ini_bool(pytestconfig, "cvd_reset_daily", True),
        "vwap_reset_daily":_ini_bool(pytestconfig, "vwap_reset_daily", True),
    }

# ------------------------- NATS client -------------------------
@pytest_asyncio.fixture
async def nc(cfg):
    url = cfg["nats_url"]
    print(f"[nc] intentando conectar a {url} ...")
    client = NatsClient()
    last_err = None
    for attempt in range(1, 6):
        try:
            print(f"[nc] intento {attempt}/5 ...")
            await asyncio.wait_for(
                client.connect(url, name="pytest-indicators", allow_reconnect=False),
                timeout=5.0,
            )
            print("[nc] conectado ✔")
            break
        except Exception as e:
            last_err = e
            print(f"[nc] fallo intento {attempt}: {repr(e)}")
            await asyncio.sleep(0.8)
    else:
        pytest.skip(f"[nc] no se pudo conectar a {url} tras 5 intentos: {repr(last_err)}")

    try:
        yield client
    finally:
        try:
            await client.drain()
            print("[nc] drained ✔")
        except Exception as e:
            print(f"[nc] error en drain: {e}")

# ------------------------- data generators -------------------------
def make_candles(n, tf, price0, amplitude, pattern, seed, symbol):
    rnd = random.Random(seed)
    tf_ms = _tf_to_ms(tf)
    base_ts = (int(time.time() * 1000) // tf_ms) * tf_ms  # ms
    price = float(price0)
    for i in range(n):
        ts = base_ts + i * tf_ms
        if pattern == "zigzag":
            step = (-amplitude, 0.0, amplitude)[i % 3]
        elif pattern == "randomwalk":
            step = rnd.uniform(-amplitude, amplitude)
        else:
            step = 0.0
        o = price
        c = round(o + step, 4)
        h = round(max(o, c) + amplitude/4, 4)
        l = round(min(o, c) - amplitude/4, 4)
        v = 12_345 + i
        price = c
        yield {"symbol": symbol, "tf": tf, "ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}

@pytest.fixture
def make_candles_fn():
    return make_candles

# ------------------------- workers -------------------------
@pytest_asyncio.fixture
async def rsi_worker(nc, cfg):
    rsi = RsiCalc(period=cfg["rsi_period"])
    in_subj  = cfg["in_subj"]
    out_subj = cfg["out_subj_rsi"]

    async def handler(msg):
        c = orjson.loads(msg.data)
        if c.get("tf") != cfg["tf"]:
            return
        v = rsi.on_bar(c["symbol"], c["tf"], int(c["ts"]), float(c["close"]))
        if v is not None:
            out = {
                "v": 1, "source": "pytest-rsi-worker",
                "symbol": c["symbol"], "tf": c["tf"], "ts": int(c["ts"]),
                "indicator": f"rsi{cfg['rsi_period']}",
                "value": float(v),
                "id": f"{c['symbol']}|{c['tf']}|{int(c['ts'])}|rsi{cfg['rsi_period']}",
            }
            await nc.publish(out_subj, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(in_subj, cb=handler)
    await nc.flush()
    try:
        yield out_subj
    finally:
        await sub.unsubscribe()
        await nc.flush()

@pytest_asyncio.fixture
async def macd_worker(nc, cfg):
    macd = MacdCalc()
    in_subj  = cfg["in_subj"]
    out_subj = cfg["out_subj_macd"]

    async def handler(msg):
        c = orjson.loads(msg.data)
        if c.get("tf") != cfg["tf"]:
            return
        v = macd.on_bar(c["symbol"], c["tf"], int(c["ts"]), float(c["close"]))
        if v is not None:
            out = {
                "v": 1, "source": "pytest-macd-worker",
                "symbol": c["symbol"], "tf": c["tf"], "ts": int(c["ts"]),
                "indicator": "macd",
                "macd": float(v["macd"]), "signal": float(v["signal"]), "hist": float(v["hist"]),
                "id": f"{c['symbol']}|{c['tf']}|{int(c['ts'])}|macd",
            }
            await nc.publish(out_subj, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(in_subj, cb=handler)
    await nc.flush()
    try:
        yield out_subj
    finally:
        await sub.unsubscribe()
        await nc.flush()

@pytest_asyncio.fixture
async def adx_worker(nc, cfg):
    """Worker ADX integración (Wilder por defecto)."""
    period = cfg["adx_period"]
    adx = AdxCalc(period=period, method="wilder")
    in_subj  = cfg["in_subj"]
    out_subj = cfg["out_subj_adx"]

    async def handler(msg):
        try:
            c = orjson.loads(msg.data)
        except Exception:
            return
        if c.get("tf") != cfg["tf"]:
            return
        v = adx.on_bar(
            c["symbol"], c["tf"], int(c["ts"]),
            float(c["high"]), float(c["low"]), float(c["close"])
        )
        if v is None:
            return
        out = {
            "v": 1, "source": "pytest-adx-worker",
            "symbol": c["symbol"], "tf": c["tf"], "ts": int(c["ts"]),
            "indicator": f"adx{period}",
            "plus_di": float(v["plus_di"]), "minus_di": float(v["minus_di"]),
            "dx": float(v["dx"]), "adx": float(v["adx"]),
            "value": float(v.get("value", v["adx"])),
            "id": f"{c['symbol']}|{c['tf']}|{int(c['ts'])}|adx{period}",
        }
        await nc.publish(out_subj, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(in_subj, cb=handler)
    await nc.flush()
    try:
        yield out_subj
    finally:
        await sub.unsubscribe()
        await nc.flush()

@pytest_asyncio.fixture
async def cvd_worker(nc, cfg):
    """Worker CVD: lee trades y publica CVD."""
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = cfg.get("out_cvd_subj")
    cvd = CvdCalc(reset_daily=cfg.get("cvd_reset_daily", True))

    async def on_trade_msg(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception as e:
            print(f"❌ [CVD_WORKER] JSON error: {e} data={msg.data!r}")
            return

        symbol = d.get("symbol") or d.get("eventSymbol")
        tf     = d.get("tf") or cfg.get("tf") or "-"
        ts     = d.get("ts") or d.get("time")
        # dxFeed nanos → ms
        if isinstance(ts, (int, float)) and ts > 10**15:
            ts = int(ts) // 1_000_000
        ts = int(ts) if ts is not None else None

        price = d.get("price")
        size  = d.get("size", d.get("quantity", 0))

        bid = d.get("bid") or d.get("bidPrice")
        ask = d.get("ask") or d.get("askPrice")
        bid = float(bid) if bid is not None else None
        ask = float(ask) if ask is not None else None

        side = d.get("side")
        aggressor = d.get("aggressor") or d.get("aggressorSide")

        if symbol is None or ts is None or price is None or size is None:
            print(f"⚠️  [CVD_WORKER] faltan campos: {d}")
            return

        value = cvd.on_trade(
            symbol=symbol, ts=ts, price=float(price), size=float(size),
            tf=tf, bid=bid, ask=ask, side=side, aggressor=aggressor
        )
        out = {
            "v": 1, "source": "indicators-engine",
            "symbol": symbol, "tf": tf, "ts": ts,
            "indicator": "cvd", "value": float(value),
            "id": f"{symbol}|{tf}|{ts}|cvd",
        }
        await nc.publish(OUT_SUBJ, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    print(f"[CVD_WORKER] subscribing IN={IN_SUBJ} ; OUT={OUT_SUBJ}")
    sub = await nc.subscribe(IN_SUBJ, cb=on_trade_msg)
    await nc.flush()
    try:
        yield OUT_SUBJ
    finally:
        await sub.unsubscribe()
        await nc.flush()

@pytest_asyncio.fixture
async def vwap_worker(nc, cfg):
    """Worker VWAP: lee trades y publica VWAP."""
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = cfg.get("out_vwap_subj")
    vcalc = VwapCalc(reset_daily=cfg.get("vwap_reset_daily", True))

    async def on_trade_msg(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol") or d.get("eventSymbol")
        tf     = d.get("tf") or cfg.get("tf") or "-"
        ts     = d.get("ts") or d.get("time")
        if isinstance(ts, (int, float)) and ts > 10**15:
            ts = int(ts) // 1_000_000
        ts = int(ts) if ts is not None else None

        price = d.get("price")
        size  = d.get("size", d.get("quantity", 0))
        if symbol is None or ts is None or price is None or size is None:
            return

        value = vcalc.on_trade(symbol=symbol, ts=ts, price=float(price), size=float(size), tf=tf)
        out = {
            "v": 1, "source": "indicators-engine",
            "symbol": symbol, "tf": tf, "ts": ts,
            "indicator": "vwap",
            "value": float(value) if value is not None else None,
            "id": f"{symbol}|{tf}|{ts}|vwap",
        }
        await nc.publish(OUT_SUBJ, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(IN_SUBJ, cb=on_trade_msg)
    await nc.flush()
    try:
        yield OUT_SUBJ
    finally:
        await sub.unsubscribe()
        await nc.flush()

@pytest_asyncio.fixture
async def poc_worker(nc, cfg):
    """
    Worker POC: lee trades en IN_TRADES y publica POC (precio) en OUT.
    Requiere cfg:
      - in_trades_subj (o in_subj como fallback)
      - out_poc_subj (default: "indicators.poc")
      - poc_tick_size (p.ej. 0.25 para ES)
      - poc_reset_daily (bool)
    """
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = cfg.get("out_poc_subj", "indicators.poc")

    tick_size = float(cfg.get("poc_tick_size", 0.25))
    poc = PocCalc(tick_size=tick_size, reset_daily=cfg.get("poc_reset_daily", True))

    async def on_trade_msg(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return

        symbol = d.get("symbol") or d.get("eventSymbol")
        tf     = d.get("tf") or cfg.get("tf") or "-"
        ts     = d.get("ts") or d.get("time")
        # dxFeed nanos -> ms
        if isinstance(ts, (int, float)) and ts > 10**15:
            ts = int(ts) // 1_000_000
        ts = int(ts) if ts is not None else None

        price = d.get("price")
        size  = d.get("size", d.get("quantity", 0))

        if symbol is None or ts is None or price is None or size is None:
            return

        value = poc.on_trade(symbol=symbol, ts=ts, price=float(price), size=float(size), tf=tf)
        if value is None:
            return

        out = {
            "v": 1,
            "source": "indicators-engine",
            "symbol": symbol,
            "tf": tf,
            "ts": ts,
            "indicator": "poc",
            "value": float(value),  # precio del POC
            "id": f"{symbol}|{tf}|{ts}|poc",
        }
        await nc.publish(OUT_SUBJ, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(IN_SUBJ, cb=on_trade_msg)
    await nc.flush()
    try:
        yield OUT_SUBJ
    finally:
        await sub.unsubscribe()
        await nc.flush()