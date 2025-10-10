# tests/conftest.py
import time, re, random
import orjson
import asyncio, pytest, pytest_asyncio
from nats.aio.client import Client as NatsClient

from indicators_engine.indicators.classic.rsi import RsiCalc
from indicators_engine.indicators.classic.macd import MacdCalc
from indicators_engine.indicators.classic.adx import AdxCalc
from indicators_engine.pipelines.cvd import CvdCalc
from indicators_engine.indicators.classic.vwap_bar import VwapCalc
from indicators_engine.pipelines.poc import PocCalc
from indicators_engine.pipelines.cob_state import BookState
from indicators_engine.pipelines.orderflow import OrderFlowCalc
from indicators_engine.pipelines.book_order import normalize_dxfeed_book_order
from indicators_engine.pipelines.heatmap import HeatmapState
from indicators_engine.pipelines.volume_profile import VolumeProfileCalc
from indicators_engine.pipelines.liquidity import LiquidityState


# ------------------------- pytest.ini options -------------------------
def pytest_addoption(parser):
    # --- Infra ---
    parser.addini("nats_url", "NATS URL", default="nats://127.0.0.1:4222")

    # --- IN (velas y trades) ---
    parser.addini("candles_subject",  "Subject IN candles", default="market.candles.1m")
    parser.addini("in_trades_subject","Subject IN trades",  default="market.trades")

    # --- IN (order book/BBO) ---
    parser.addini("in_orderbook_subject", "Subject IN orderbook", default="market.orderbook")
    parser.addini("bbo_subject",          "Subject IN BBO",        default="market.bbo")

    # --- OUT (indicadores) ---
    parser.addini("rsi_subject",         "Subject OUT rsi",         default="indicators.candles.1m.rsi10")
    parser.addini("macd_subject",        "Subject OUT macd",        default="indicators.candles.1m.macd")
    parser.addini("adx_subject",         "Subject OUT adx",         default="indicators.candles.1m.adx14")
    parser.addini("out_cvd_subject",     "Subject OUT CVD",         default="indicators.cvd")
    parser.addini("out_vwap_subject",    "Subject OUT VWAP",        default="indicators.vwap")
    parser.addini("out_poc_subj",        "Subject OUT POC",         default="indicators.poc")
    parser.addini("out_orderflow_subj",  "Subject OUT OrderFlow",   default="indicators.orderflow")
    parser.addini("out_cob_subject",     "Subject OUT COB",         default="indicators.cob")
    parser.addini("out_book_order_subject","Subject OUT BookOrder", default="indicators.book_order")
    parser.addini("out_heatmap_subject", "Subject OUT Heatmap", default="indicators.heatmap")
    parser.addini("out_liquidity_subject", "Subject OUT Liquidity", default="indicators.liquidity")


    # --- Serie de velas sintéticas ---
    parser.addini("candle_symbol",    "Symbol",        default="ESZ5")
    parser.addini("candle_tf",        "TF",            default="1m")
    parser.addini("candle_n",         "N candles",     default="20")
    parser.addini("candle_price",     "Start price",   default="648.0")
    parser.addini("candle_amplitude", "Amplitude",     default="0.35")
    parser.addini("candle_pattern",   "Pattern",       default="zigzag")
    parser.addini("candle_seed",      "Seed",          default="1")

    # --- Parámetros indicadores ---
    parser.addini("rsi_period",          "RSI period",           default="14")
    parser.addini("adx_period",          "ADX period",           default="30")
    parser.addini("cvd_reset_daily",     "CVD reset diario",     default="true")
    parser.addini("vwap_reset_daily",    "VWAP reset diario",    default="true")
    parser.addini("poc_tick_size",       "POC tick size",        default="0.25")
    parser.addini("poc_reset_daily",     "POC reset diario",     default="true")
    parser.addini("orderflow_reset_daily","OrderFlow reset diario", default="true")
    parser.addini("liquidity_depth_levels","Depth levels for liquidity", default="10")


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
        # Infra
        "nats_url":          get("nats_url"),

        # IN
        "in_subj":           get("candles_subject"),
        "in_trades_subj":    get("in_trades_subject"),
        "in_orderbook_subj": get("in_orderbook_subject"),
        "bbo_subject":       get("bbo_subject"),

        # OUT
        "out_subj_rsi":      get("rsi_subject"),
        "out_subj_macd":     get("macd_subject"),
        "out_subj_adx":      get("adx_subject"),
        "out_cvd_subj":      get("out_cvd_subject"),
        "out_vwap_subj":     get("out_vwap_subject"),
        "out_poc_subj":      get("out_poc_subj"),
        "out_orderflow_subj":get("out_orderflow_subj"),
        "out_cob_subj":      get("out_cob_subject"),
        "out_book_order_subj": get("out_book_order_subject"),
        "out_heatmap_subj": get("out_heatmap_subject"),
        "out_liquidity_subj": get("out_liquidity_subject"),


        # Serie de velas
        "symbol":            get("candle_symbol"),
        "tf":                get("candle_tf"),
        "n":                 int(get("candle_n")),
        "price":             float(get("candle_price")),
        "amplitude":         float(get("candle_amplitude")),
        "pattern":           get("candle_pattern"),
        "seed":              int(get("candle_seed")),

        # Parámetros indicadores
        "rsi_period":        int(get("rsi_period")),
        "adx_period":        int(get("adx_period")),
        "cvd_reset_daily":   _ini_bool(pytestconfig, "cvd_reset_daily", True),
        "vwap_reset_daily":  _ini_bool(pytestconfig, "vwap_reset_daily", True),
        "poc_tick_size":     float(get("poc_tick_size")),
        "poc_reset_daily":   _ini_bool(pytestconfig, "poc_reset_daily", True),
        "orderflow_reset_daily": _ini_bool(pytestconfig, "orderflow_reset_daily", True),
        "liquidity_depth_levels": int(get("liquidity_depth_levels")),
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


@pytest_asyncio.fixture
async def cob_incremental_worker(nc, cfg):
    IN = cfg.get("in_orderbook_subj", "market.orderbook")
    OUT = cfg.get("out_cob_subj", "indicators.cob")

    states: dict[str, BookState] = {}

    async def on_msg(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            print("[COB_WORKER] error parseando JSON")
            return

        symbol = d.get("symbol") or d.get("eventSymbol")
        if not symbol:
            print("[COB_WORKER] ignorado: no hay symbol")
            return

        st = states.get(symbol)
        if st is None:
            st = BookState(symbol, max_depth=10)
            states[symbol] = st
            print(f"[COB_WORKER] creado estado nuevo para {symbol}")

        kind = (d.get("kind") or "").lower()
        if kind == "snapshot":
            print(f"[COB_WORKER] recibido SNAPSHOT de {symbol}")
            st.apply_snapshot(d)
        elif kind == "update":
            print(f"[COB_WORKER] recibido UPDATE de {symbol}")
            st.apply_update(d)
        else:
            print(f"[COB_WORKER] recibido mensaje sin kind de {symbol}, infiriendo…")
            if "bids" in d or "asks" in d or "bidLevels" in d or "askLevels" in d:
                st.apply_snapshot(d)
            else:
                st.apply_update(d)

        out = st.snapshot()
        print(f"[COB_WORKER] publicando COB {out['id']} con {len(out['bids'])} bids / {len(out['asks'])} asks")
        if out["ts"] > 0:
            await nc.publish(OUT, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(IN, cb=on_msg)
    await nc.flush()
    try:
        yield OUT
    finally:
        await sub.unsubscribe()
        await nc.flush()


@pytest_asyncio.fixture
async def orderflow_worker(nc, cfg):
    """
    Worker Order Flow (agresivo): consume BBO + trades y publica delta BUY-SELL.
    IN:
      - bbo_subject         (default: "market.bbo")
      - in_trades_subj      (o "in_subj")
    OUT:
      - out_orderflow_subj  (default: "indicators.orderflow")
    Opcional:
      - orderflow_reset_daily (bool, default True)

    Publica:
      {"indicator":"orderflow","delta":..., "buy":..., "sell":..., "symbol":..., "tf":..., "ts":...}
    """
    IN_BBO = cfg.get("bbo_subject") or "market.bbo"
    IN_TRD = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT    = cfg.get("out_orderflow_subj", "indicators.orderflow")

    of = OrderFlowCalc(reset_daily=cfg.get("orderflow_reset_daily", True))

    def _to_ms(ts):
        # dxFeed time en nanos → ms
        if isinstance(ts, (int, float)) and ts > 10**15:
            return int(ts) // 1_000_000
        return int(ts) if ts is not None else None

    async def on_bbo(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol") or d.get("eventSymbol")
        tf     = d.get("tf") or cfg.get("tf") or "-"
        ts     = _to_ms(d.get("ts") or d.get("time"))

        # Campos BBO frecuentes (dxFeed u otros)
        bid = d.get("bid")
        ask = d.get("ask")
        if bid is None: bid = d.get("bidPrice", d.get("bestBidPrice"))
        if ask is None: ask = d.get("askPrice", d.get("bestAskPrice"))

        if symbol is None or ts is None or (bid is None and ask is None):
            return
        of.on_bbo(symbol=symbol, ts=ts, bid=bid, ask=ask, tf=tf)

    async def on_trade(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol") or d.get("eventSymbol")
        tf     = d.get("tf") or cfg.get("tf") or "-"
        ts     = _to_ms(d.get("ts") or d.get("time"))
        price  = d.get("price")
        size   = d.get("size", d.get("quantity", 0))

        # Señales de agresor si vienen en el trade
        aggressor = d.get("aggressor") or d.get("side")  # "B"/"S", "BUY"/"SELL", etc.
        is_buyer_initiator = d.get("isBuyerInitiator")
        # Si solo viene isBidAggressor (nomenclatura ambigua), interpretamos:
        # True => agresor en el lado bid (venta agresiva) ⇒ buyer_initiator = False
        if is_buyer_initiator is None and "isBidAggressor" in d:
            try:
                is_buyer_initiator = not bool(d["isBidAggressor"])
            except Exception:
                pass

        if symbol is None or ts is None or price is None or size is None:
            return

        snap = of.on_trade(
            symbol=symbol, ts=ts, price=float(price), size=float(size), tf=tf,
            aggressor=aggressor, is_buyer_initiator=is_buyer_initiator
        )

        out = {
            "v": 1,
            "source": "indicators-engine",
            "symbol": symbol,
            "tf": tf,
            "ts": ts,
            "indicator": "orderflow",
            "delta": snap["delta"],
            "buy": snap["buy"],
            "sell": snap["sell"],
            "id": f"{symbol}|{tf}|{ts}|orderflow",
        }
        await nc.publish(OUT, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub_bbo = await nc.subscribe(IN_BBO, cb=on_bbo)
    sub_trd = await nc.subscribe(IN_TRD, cb=on_trade)
    await nc.flush()
    try:
        yield OUT
    finally:
        await sub_bbo.unsubscribe()
        await sub_trd.unsubscribe()
        await nc.flush()


@pytest_asyncio.fixture
async def book_order_worker(nc, cfg):
    """
    Worker Book Order: emite TODOS los niveles L2 cuando recibe OrderBookSnapshot.
    Ignora updates incrementales (para eso usa el COB incremental).
    Usa subjects del pytest.ini:
      - in_orderbook_subject (default: market.orderbook)
      - out_book_order_subject (default: indicators.book_order)
    """
    IN = cfg.get("in_orderbook_subj", "market.orderbook")
    OUT = cfg.get("out_book_order_subj", "indicators.book_order")

    async def on_msg(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            print("[BOOK_ORDER_WORKER] JSON inválido")
            return

        kind = (d.get("kind") or "").lower()
        # Solo snapshots: si no viene 'kind', inferimos snapshot si trae bids/asks
        if kind not in ("snapshot", "") and not any(k in d for k in ("bids","asks","bidLevels","askLevels")):
            return

        out = normalize_dxfeed_book_order(d)
        if not out:
            print("[BOOK_ORDER_WORKER] snapshot no normalizable")
            return

        print(f"[BOOK_ORDER_WORKER] publicando {out['id']} depth(b/a)=({len(out['bids'])}/{len(out['asks'])})")
        await nc.publish(OUT, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    print(f"[BOOK_ORDER_WORKER] subscribing IN={IN} OUT={OUT}")
    sub = await nc.subscribe(IN, cb=on_msg)
    await nc.flush()
    try:
        yield OUT
    finally:
        await sub.unsubscribe()
        await nc.flush()

@pytest_asyncio.fixture
async def heatmap_worker(nc, cfg):
    """
    Worker Heatmap: escucha OrderBook (snapshot + updates) y publica frames sparse precio×tiempo.
    IN:  in_orderbook_subj (dxFeed OrderBook/OrderBookSnapshot)
    OUT: out_heatmap_subj  (default: indicators.heatmap)
    """
    IN  = cfg.get("in_orderbook_subj", "market.orderbook")
    OUT = cfg.get("out_heatmap_subj", "indicators.heatmap")
    symbol = cfg["symbol"]

    state = HeatmapState(symbol, tick_size=float(cfg.get("poc_tick_size", 0.25)), bucket_ms=1000, max_prices=20)

    async def on_msg(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return

        kind = (d.get("kind") or "").lower()
        # inferencia básica si no viene 'kind'
        is_snapshot_like = any(k in d for k in ("bids","asks","bidLevels","askLevels"))
        if kind == "snapshot" or (kind == "" and is_snapshot_like):
            state.apply_snapshot(d)
        elif kind == "update" or not is_snapshot_like:
            state.apply_update(d)
        else:
            return

        out = state.frame()
        await nc.publish(OUT, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(IN, cb=on_msg)
    await nc.flush()
    try:
        yield OUT
    finally:
        await sub.unsubscribe()
        await nc.flush()

@pytest_asyncio.fixture
async def liquidity_worker(nc, cfg):
    """
    Worker Liquidity:
      - Escucha OrderBook (snapshot + updates)
      - Mantiene estado y publica métricas de liquidez (depth & imbalance)
    IN:  in_orderbook_subj (default: market.orderbook)
    OUT: out_liquidity_subj (default: indicators.liquidity)
    """
    IN  = cfg.get("in_orderbook_subj", "market.orderbook")
    OUT = cfg.get("out_liquidity_subj", "indicators.liquidity")
    symbol = cfg["symbol"]

    state = LiquidityState(symbol, depth_levels=int(cfg.get("liquidity_depth_levels", 10) or 10))

    async def on_msg(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return

        kind = (d.get("kind") or "").lower()
        is_snapshot_like = any(k in d for k in ("bids","asks","bidLevels","askLevels"))
        if kind == "snapshot" or (kind == "" and is_snapshot_like):
            state.apply_snapshot(d)
        elif kind == "update" or not is_snapshot_like:
            state.apply_update(d)
        else:
            return

        out = state.snapshot()
        if out["ts"] > 0:
            await nc.publish(OUT, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(IN, cb=on_msg)
    await nc.flush()
    try:
        yield OUT
    finally:
        await sub.unsubscribe()
        await nc.flush()

@pytest_asyncio.fixture
async def volume_profile_worker(nc, cfg):
    """
    Worker Volume Profile: agrega volumen por precio en buckets temporales (tf).
    IN:   in_trades_subj (o in_subj)
    OUT:  out_vp_subj (default: indicators.vp)
    Params opcionales:
      - vp_tick_size (float, default 0.25; cae a poc_tick_size si existe)
      - vp_tf (str, default cfg["tf"] o "1m")
      - vp_max_buckets (int, default 5)
    Publica:
      {"indicator":"vp","symbol":...,"tf":...,"ts":<bucket_start>,
       "tick_size":..., "vtotal":..., "bins":[{"price":..,"volume":..},...], "poc":...}
    """
    IN_SUBJ  = cfg.get("in_trades_subj") or cfg["in_subj"]
    OUT_SUBJ = cfg.get("out_vp_subj", "indicators.vp")
    tick_size = float(cfg.get("vp_tick_size", cfg.get("poc_tick_size", 0.25)))
    tf = cfg.get("vp_tf") or cfg.get("tf") or cfg.get("candle_tf") or "1m"
    max_buckets = int(cfg.get("vp_max_buckets", 5))

    vp = VolumeProfileCalc(tick_size=tick_size, tf=tf, max_buckets=max_buckets)

    def _to_ms(ts):
        # dxFeed nanos → ms si hace falta
        if isinstance(ts, (int, float)) and ts > 10**15:
            return int(ts) // 1_000_000
        return int(ts) if ts is not None else None

    async def on_trade_msg(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return

        symbol = d.get("symbol") or d.get("eventSymbol")
        tf_msg = d.get("tf") or tf
        ts     = _to_ms(d.get("ts") or d.get("time"))
        price  = d.get("price")
        size   = d.get("size", d.get("quantity", 0))

        if symbol is None or ts is None or price is None or size is None:
            return

        snap = vp.on_trade(symbol=symbol, ts=ts, price=float(price), size=float(size), tf=tf_msg)
        out = {
            "v": 1,
            "source": "indicators-engine",
            "symbol": symbol,
            "tf": tf_msg,
            "ts": snap["bucket_start"],  # inicio del bucket
            "indicator": "vp",
            "tick_size": tick_size,
            "vtotal": snap["vtotal"],
            "bins": snap["bins"],
            "poc": snap["poc"],
            "id": f"{symbol}|{tf_msg}|{snap['bucket_start']}|vp",
        }
        await nc.publish(OUT_SUBJ, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(IN_SUBJ, cb=on_trade_msg)
    await nc.flush()
    try:
        yield OUT_SUBJ
    finally:
        await sub.unsubscribe()
        await nc.flush()