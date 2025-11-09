# tests/conftest.py
"""
Conftest for indicators-engine tests.
Updated to work with new hybrid architecture (Rust + Python).
"""
import time
import random
import orjson
import math
from collections import defaultdict, deque
import asyncio
import pytest
import pytest_asyncio
from nats.aio.client import Client as NatsClient

# Import new indicator classes
from indicators_engine.indicators.classic.rsi import RSI, RSIConfig
from indicators_engine.indicators.classic.macd import MACD, MACDConfig
from indicators_engine.indicators.classic.adx import ADX, ADXConfig
from indicators_engine.indicators.classic.vwap_bar import VWAPBar
from indicators_engine.indicators.volume.svp import SVP, SVPConfig
from indicators_engine.indicators.volume.volume_profile import VolumeProfile
from indicators_engine.core.types import Bar, Trade

def _ts_to_ms(ts: int) -> int:
    if ts > 10**12:
        return int(ts // 1_000_000)
    return int(ts)

def _normalize_levels(levels):
    return [[float(price), float(size)] for price, size in levels]

def _make_id(symbol: str, tf: str, ts: int, indicator: str) -> str:
    return f"{symbol}|{tf}|{int(ts)}|{indicator}"

# Try to import hybrid engine
try:
    from indicators_engine.hybrid_engine import HybridIndicatorEngine
    HYBRID_AVAILABLE = True
except ImportError:
    HYBRID_AVAILABLE = False


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
    parser.addini("adx_subject",           "Subject OUT adx",         default="indicators.candles.1m.adx14")
    
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
    parser.addini("adx_period",          "ADX period",           default="14")


# ------------------------- helpers -------------------------
def _tf_to_ms(tf: str) -> int:
    import re
    m = re.fullmatch(r"(\d+)([smhd])", tf)
    if not m:
        raise ValueError(f"TF inválido: {tf}")
    n, u = int(m.group(1)), m.group(2)
    mult = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[u]
    return n * mult


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
    base_ts = (int(time.time() * 1000) // tf_ms) * tf_ms
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
    """Worker RSI using new RSI class"""
    rsi = RSI(RSIConfig(period=cfg["rsi_period"]))
    in_subj  = cfg["in_subj"]
    out_subj = cfg["out_subj_rsi"]

    async def handler(msg):
        c = orjson.loads(msg.data)
        if c.get("tf") != cfg["tf"]:
            return
        # Convert dict to Bar
        bar = Bar(
            ts=c["ts"],
            symbol=c["symbol"],
            tf=c["tf"],
            open=c["open"],
            high=c["high"],
            low=c["low"],
            close=c["close"],
            volume=c.get("volume", 0.0)
        )
        v = rsi.on_bar(bar)
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
    """Worker MACD using new MACD class"""
    macd = MACD(MACDConfig())
    in_subj  = cfg["in_subj"]
    out_subj = cfg["out_subj_macd"]

    async def handler(msg):
        c = orjson.loads(msg.data)
        if c.get("tf") != cfg["tf"]:
            return
        # Convert dict to Bar
        bar = Bar(
            ts=c["ts"],
            symbol=c["symbol"],
            tf=c["tf"],
            open=c["open"],
            high=c["high"],
            low=c["low"],
            close=c["close"],
            volume=c.get("volume", 0.0)
        )
        v = macd.on_bar(bar)
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


# ------------------------- Additional pipeline workers -------------------------

@pytest_asyncio.fixture
async def vwap_worker(nc, cfg):
    out_subj = "indicators.pipeline.vwap"
    in_subj = cfg.get("in_trades_subj") or cfg["in_subj"]
    totals = defaultdict(lambda: {"pv": 0.0, "vol": 0.0})

    async def handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol")
        tf = d.get("tf", cfg["tf"])
        ts = d.get("ts")
        price = d.get("price")
        size = d.get("size", d.get("volume", 0.0))
        if symbol is None or ts is None or price is None or size is None:
            return
        size = float(size)
        if size <= 0.0:
            return
        key = (symbol, tf)
        totals[key]["pv"] += float(price) * size
        totals[key]["vol"] += size
        vol = totals[key]["vol"]
        if vol <= 0.0:
            return
        value = totals[key]["pv"] / vol
        out = {
            "v": 1,
            "source": "pytest-vwap-worker",
            "symbol": symbol,
            "tf": tf,
            "ts": int(ts),
            "indicator": "vwap",
            "value": float(value),
            "id": _make_id(symbol, tf, ts, "vwap"),
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
    out_subj = "indicators.pipeline.cvd"
    in_subj = cfg.get("in_trades_subj") or cfg["in_subj"]
    totals = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
    last_quotes = {}

    async def handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol")
        tf = d.get("tf", cfg["tf"])
        ts = d.get("ts")
        price = d.get("price")
        size = d.get("size", d.get("quantity", 0.0))
        if symbol is None or ts is None or price is None or size is None:
            return
        size = float(size)
        side = d.get("side") or d.get("aggressor")
        bid = d.get("bid")
        ask = d.get("ask")
        if bid is not None and ask is not None:
            last_quotes[(symbol, tf)] = (float(bid), float(ask))
        elif (symbol, tf) in last_quotes:
            bid, ask = last_quotes[(symbol, tf)]
        else:
            bid = ask = None

        trade_side = None
        if isinstance(side, str):
            side_upper = side.upper()
            if "BUY" in side_upper:
                trade_side = "buy"
            elif "SELL" in side_upper:
                trade_side = "sell"
        if trade_side is None and bid is not None and ask is not None:
            if float(price) >= ask:
                trade_side = "buy"
            elif float(price) <= bid:
                trade_side = "sell"
            else:
                trade_side = "buy" if float(price) >= (bid + ask) / 2 else "sell"
        if trade_side is None:
            trade_side = "buy"

        key = (symbol, tf)
        if trade_side == "buy":
            totals[key]["buy"] += size
        else:
            totals[key]["sell"] += size
        value = totals[key]["buy"] - totals[key]["sell"]
        out = {
            "v": 1,
            "source": "pytest-cvd-worker",
            "symbol": symbol,
            "tf": tf,
            "ts": int(ts),
            "indicator": "cvd",
            "value": float(value),
            "buy": float(totals[key]["buy"]),
            "sell": float(totals[key]["sell"]),
            "id": _make_id(symbol, tf, ts, "cvd"),
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
async def orderflow_worker(nc, cfg):
    out_subj = "indicators.pipeline.orderflow"
    trades_subj = cfg.get("in_trades_subj") or cfg["in_subj"]
    bbo_subj = cfg.get("bbo_subject", "market.bbo")
    state = defaultdict(lambda: {"bid": None, "ask": None, "buy": 0.0, "sell": 0.0})

    async def bbo_handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol")
        tf = d.get("tf", cfg.get("tf"))
        if symbol is None or tf is None:
            return
        key = (symbol, tf)
        state[key]["bid"] = float(d.get("bid", state[key]["bid"] or 0.0))
        state[key]["ask"] = float(d.get("ask", state[key]["ask"] or 0.0))

    async def trade_handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol")
        tf = d.get("tf", cfg.get("tf"))
        ts = d.get("ts")
        price = d.get("price")
        size = d.get("size", d.get("quantity", 0.0))
        if symbol is None or tf is None or ts is None or price is None or size is None:
            return
        size = float(size)
        key = (symbol, tf)
        bid = state[key]["bid"]
        ask = state[key]["ask"]
        side = d.get("side") or d.get("aggressor")
        trade_side = None
        if isinstance(side, str):
            if "BUY" in side.upper():
                trade_side = "buy"
            elif "SELL" in side.upper():
                trade_side = "sell"
        if trade_side is None and bid is not None and ask is not None:
            if float(price) >= ask:
                trade_side = "buy"
            elif float(price) <= bid:
                trade_side = "sell"
            else:
                trade_side = "buy" if float(price) >= (bid + ask) / 2 else "sell"
        if trade_side is None:
            trade_side = "buy"
        state[key][trade_side] += size
        delta = state[key]["buy"] - state[key]["sell"]
        out = {
            "v": 1,
            "source": "pytest-orderflow-worker",
            "symbol": symbol,
            "tf": tf,
            "ts": int(ts),
            "indicator": "orderflow",
            "buy": float(state[key]["buy"]),
            "sell": float(state[key]["sell"]),
            "delta": float(delta),
            "id": _make_id(symbol, tf, ts, "orderflow"),
        }
        await nc.publish(out_subj, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub_bbo = await nc.subscribe(bbo_subj, cb=bbo_handler)
    sub_trades = await nc.subscribe(trades_subj, cb=trade_handler)
    await nc.flush()
    try:
        yield out_subj
    finally:
        await sub_bbo.unsubscribe()
        await sub_trades.unsubscribe()
        await nc.flush()


@pytest_asyncio.fixture
async def book_order_worker(nc, cfg):
    out_subj = "indicators.pipeline.book_order"
    in_subj = cfg.get("in_orderbook_subj", "market.orderbook")

    async def handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        if d.get("kind") != "snapshot":
            return
        symbol = d.get("eventSymbol") or d.get("symbol")
        if symbol is None:
            return
        ts = d.get("time")
        bids = d.get("bids", [])
        asks = d.get("asks", [])
        out = {
            "v": 1,
            "source": "pytest-book-order-worker",
            "symbol": symbol,
            "tf": cfg.get("tf"),
            "ts": _ts_to_ms(int(ts)),
            "indicator": "book_order",
            "bids": _normalize_levels(bids),
            "asks": _normalize_levels(asks),
            "id": _make_id(symbol, cfg.get("tf"), _ts_to_ms(int(ts)), "book_order"),
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
async def cob_incremental_worker(nc, cfg):
    out_subj = "indicators.pipeline.cob"
    in_subj = cfg.get("in_orderbook_subj", "market.orderbook")
    books = defaultdict(lambda: {"bids": {}, "asks": {}})

    def _snapshot(symbol, tf, ts_ms):
        book = books[(symbol, tf)]
        bids = sorted(((p, v) for p, v in book["bids"].items() if v > 0), reverse=True)
        asks = sorted(((p, v) for p, v in book["asks"].items() if v > 0))
        return {
            "v": 1,
            "source": "pytest-cob-worker",
            "symbol": symbol,
            "tf": tf,
            "ts": ts_ms,
            "indicator": "cob",
            "bids": _normalize_levels(bids),
            "asks": _normalize_levels(asks),
            "id": _make_id(symbol, tf, ts_ms, "cob"),
        }

    async def handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("eventSymbol") or d.get("symbol")
        tf = cfg.get("tf")
        if symbol is None or tf is None:
            return
        ts = d.get("time")
        if ts is None:
            return
        ts_ms = _ts_to_ms(int(ts))
        key = (symbol, tf)
        book = books[key]
        kind = d.get("kind")
        if kind == "snapshot":
            book["bids"] = {float(p): float(v) for p, v in d.get("bids", [])}
            book["asks"] = {float(p): float(v) for p, v in d.get("asks", [])}
        elif kind == "update":
            side = d.get("side", "").lower()
            price = float(d.get("price"))
            size = float(d.get("size", 0.0))
            target = book["bids"] if side == "bid" else book["asks"]
            if size <= 0.0:
                target.pop(price, None)
            else:
                target[price] = size
        out = _snapshot(symbol, tf, ts_ms)
        await nc.publish(out_subj, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(in_subj, cb=handler)
    await nc.flush()
    try:
        yield out_subj
    finally:
        await sub.unsubscribe()
        await nc.flush()


@pytest_asyncio.fixture
async def liquidity_worker(nc, cfg):
    out_subj = "indicators.pipeline.liquidity"
    in_subj = cfg.get("in_orderbook_subj", "market.orderbook")
    depth_levels = int(cfg.get("liquidity_depth_levels", 10))
    books = defaultdict(lambda: {"bids": {}, "asks": {}})

    async def handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("eventSymbol") or d.get("symbol")
        tf = cfg.get("tf")
        if symbol is None or tf is None:
            return
        ts = d.get("time")
        if ts is None:
            return
        ts_ms = _ts_to_ms(int(ts))
        key = (symbol, tf)
        book = books[key]
        kind = d.get("kind")
        if kind == "snapshot":
            book["bids"] = {float(p): float(v) for p, v in d.get("bids", [])}
            book["asks"] = {float(p): float(v) for p, v in d.get("asks", [])}
        elif kind == "update":
            side = d.get("side", "").lower()
            price = float(d.get("price"))
            size = float(d.get("size", 0.0))
            target = book["bids"] if side == "bid" else book["asks"]
            if size <= 0.0:
                target.pop(price, None)
            else:
                target[price] = size
        bids_sorted = sorted(((p, v) for p, v in book["bids"].items() if v > 0), reverse=True)
        asks_sorted = sorted(((p, v) for p, v in book["asks"].items() if v > 0))
        best_bid = bids_sorted[0][0] if bids_sorted else None
        best_ask = asks_sorted[0][0] if asks_sorted else None
        bid_depth = sum(v for _, v in bids_sorted[:depth_levels])
        ask_depth = sum(v for _, v in asks_sorted[:depth_levels])
        out = {
            "v": 1,
            "source": "pytest-liquidity-worker",
            "symbol": symbol,
            "tf": tf,
            "ts": ts_ms,
            "indicator": "liquidity",
            "depth_levels": depth_levels,
            "bids_depth": float(bid_depth),
            "asks_depth": float(ask_depth),
            "best_bid": float(best_bid) if best_bid is not None else None,
            "best_ask": float(best_ask) if best_ask is not None else None,
            "bid1_size": float(bids_sorted[0][1]) if bids_sorted else None,
            "ask1_size": float(asks_sorted[0][1]) if asks_sorted else None,
            "id": _make_id(symbol, tf, ts_ms, "liquidity"),
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
async def heatmap_worker(nc, cfg):
    out_subj = "indicators.pipeline.heatmap"
    in_subj = cfg.get("in_orderbook_subj", "market.orderbook")
    books = defaultdict(lambda: {"rows": {}})

    async def handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("eventSymbol") or d.get("symbol")
        tf = cfg.get("tf")
        if symbol is None or tf is None:
            return
        ts = d.get("time")
        if ts is None:
            return
        ts_ms = _ts_to_ms(int(ts))
        key = (symbol, tf)
        kind = d.get("kind")
        rows = books[key]["rows"]
        if kind == "snapshot":
            rows.clear()
            for price, size in d.get("bids", []):
                rows[(ts_ms, float(price))] = float(size)
            for price, size in d.get("asks", []):
                rows[(ts_ms, float(price))] = float(size)
        elif kind == "update":
            price = float(d.get("price"))
            size = float(d.get("size", 0.0))
            rows[(ts_ms, price)] = float(size)
        out_rows = [[ts, price, float(size)] for (ts, price), size in sorted(rows.items())]
        out = {
            "v": 1,
            "source": "pytest-heatmap-worker",
            "symbol": symbol,
            "tf": tf,
            "ts": ts_ms,
            "indicator": "heatmap",
            "rows": out_rows,
            "id": _make_id(symbol, tf, ts_ms, "heatmap"),
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
async def volume_profile_worker(nc, cfg):
    out_subj = "indicators.pipeline.vp"
    in_subj = cfg.get("in_trades_subj") or cfg["in_subj"]
    tick_size = float(cfg.get("tick_size", 0.25))
    buckets = defaultdict(lambda: defaultdict(float))

    def price_bin(price: float) -> float:
        return round(round(price / tick_size) * tick_size, 10)

    async def handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol")
        tf = d.get("tf", cfg.get("tf"))
        ts = d.get("ts")
        price = d.get("price")
        size = d.get("size", d.get("quantity", 0.0))
        if symbol is None or tf is None or ts is None or price is None or size is None:
            return
        size = float(size)
        if size <= 0:
            return
        bucket_ts = (int(ts) // 60_000) * 60_000
        bins = buckets[(symbol, tf, bucket_ts)]
        b = price_bin(float(price))
        bins[b] += size
        vtotal = sum(bins.values())
        poc_price = max(bins, key=lambda k: bins[k])
        out_bins = [{"price": float(p), "volume": float(v)} for p, v in sorted(bins.items())]
        out = {
            "v": 1,
            "source": "pytest-volume-profile-worker",
            "symbol": symbol,
            "tf": tf,
            "ts": bucket_ts,
            "indicator": "vp",
            "vtotal": float(vtotal),
            "poc": float(poc_price),
            "bins": out_bins,
            "id": _make_id(symbol, tf, bucket_ts, "vp"),
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
async def poc_worker(nc, cfg):
    out_subj = "indicators.pipeline.poc"
    in_subj = cfg.get("in_trades_subj") or cfg["in_subj"]
    tick_size = float(cfg.get("tick_size", 0.25))
    volumes = defaultdict(lambda: defaultdict(float))

    def price_bin(price: float) -> float:
        return round(round(price / tick_size) * tick_size, 10)

    async def handler(msg):
        try:
            d = orjson.loads(msg.data)
        except Exception:
            return
        symbol = d.get("symbol")
        tf = d.get("tf", cfg.get("tf"))
        ts = d.get("ts")
        price = d.get("price")
        size = d.get("size", d.get("quantity", 0.0))
        if symbol is None or tf is None or ts is None or price is None or size is None:
            return
        size = float(size)
        if size <= 0:
            return
        key = (symbol, tf)
        bin_price = price_bin(float(price))
        volumes[key][bin_price] += size
        poc_price = max(volumes[key], key=lambda k: volumes[key][k])
        out = {
            "v": 1,
            "source": "pytest-poc-worker",
            "symbol": symbol,
            "tf": tf,
            "ts": int(ts),
            "indicator": "poc",
            "value": float(poc_price),
            "id": _make_id(symbol, tf, ts, "poc"),
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
    """Worker ADX using new ADX class"""
    period = cfg["adx_period"]
    adx = ADX(ADXConfig(period=period))
    in_subj  = cfg["in_subj"]
    out_subj = cfg["out_subj_adx"]

    async def handler(msg):
        try:
            c = orjson.loads(msg.data)
        except Exception:
            return
        if c.get("tf") != cfg["tf"]:
            return
        
        # Convert dict to Bar
        bar = Bar(
            ts=c["ts"],
            symbol=c["symbol"],
            tf=c["tf"],
            open=c["open"],
            high=c["high"],
            low=c["low"],
            close=c["close"],
            volume=c.get("volume", 0.0)
        )
        v = adx.on_bar(bar)
        if v is None:
            return
        out = {
            "v": 1, "source": "pytest-adx-worker",
            "symbol": c["symbol"], "tf": c["tf"], "ts": int(c["ts"]),
            "indicator": f"adx{period}",
            "plus_di": float(v["plus_di"]), "minus_di": float(v["minus_di"]),
            "adx": float(v["adx"]),
            "value": float(v["adx"]),
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


# ------------------------- Hybrid Engine Fixture -------------------------
@pytest.fixture
def hybrid_engine():
    """Hybrid engine fixture"""
    if HYBRID_AVAILABLE:
        return HybridIndicatorEngine()
    else:
        pytest.skip("Hybrid engine not available")
