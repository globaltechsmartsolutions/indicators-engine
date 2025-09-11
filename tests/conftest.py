# tests/conftest.py
import time, re, random
import pytest
import pytest_asyncio
import orjson
from nats.aio.client import Client as NatsClient
from indicators_engine.pipelines.rsi import RsiCalc

def pytest_addoption(parser):
    parser.addini("nats_url", "NATS URL", default="nats://127.0.0.1:4222")
    parser.addini("candles_subject", "Subject IN candles", default="market.candles.1m")
    parser.addini("rsi_subject", "Subject OUT rsi", default="indicators.candles.1m.rsi14")
    parser.addini("candle_symbol", "Symbol", default="ESZ5")
    parser.addini("candle_tf", "TF", default="1m")
    parser.addini("candle_n", "N candles", default="20")
    parser.addini("candle_price", "Start price", default="648.0")
    parser.addini("candle_amplitude", "Amplitude", default="0.35")
    parser.addini("candle_pattern", "Pattern", default="zigzag")
    parser.addini("candle_seed", "Seed", default="1")
    parser.addini("rsi_period", "RSI period", default="14")

def _tf_to_ms(tf: str) -> int:
    m = re.fullmatch(r"(\d+)([smhd])", tf)
    if not m:
        raise ValueError(f"TF inválido: {tf}")
    n, u = int(m.group(1)), m.group(2)
    mult = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[u]
    return n * mult

@pytest.fixture(scope="session")
def cfg(pytestconfig):
    get = pytestconfig.getini
    return {
        "nats_url": get("nats_url"),
        "in_subj":  get("candles_subject"),
        "out_subj": get("rsi_subject"),
        "symbol":   get("candle_symbol"),
        "tf":       get("candle_tf"),
        "n":        int(get("candle_n")),
        "price":    float(get("candle_price")),
        "amplitude":     float(get("candle_amplitude")),
        "pattern":  get("candle_pattern"),
        "seed":     int(get("candle_seed")),
        "rsi_period": int(get("rsi_period")),
    }

@pytest_asyncio.fixture
async def nc(cfg):
    nc = NatsClient()
    await nc.connect(cfg["nats_url"], name="pytest-indicators")
    try:
        yield nc
    finally:
        await nc.drain()

def make_candles(n, tf, price0, amplitude, pattern, seed, symbol):
    rnd = random.Random(seed)
    tf_ms = _tf_to_ms(tf)
    base_ts = (int(time.time()) // (tf_ms // 1000)) * tf_ms
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

@pytest_asyncio.fixture
async def rsi_worker(nc, cfg):
    rsi = RsiCalc(period=cfg["rsi_period"])
    in_subj  = cfg["in_subj"]
    out_subj = cfg["out_subj"]

    async def handler(msg):
        c = orjson.loads(msg.data)
        if c.get("tf") != cfg["tf"]:
            return
        v = rsi.on_bar(c["symbol"], c["tf"], int(c["ts"]), float(c["close"]))
        if v is not None:
            out = {
                "v": 1, "source": "pytest-rsi-worker",
                "symbol": c["symbol"], "tf": c["tf"], "ts": c["ts"],
                "indicator": f"rsi{cfg['rsi_period']}",
                "value": float(v),
                "id": f"{c['symbol']}|{c['tf']}|{c['ts']}|rsi{cfg['rsi_period']}",
            }
            await nc.publish(out_subj, orjson.dumps(out), headers={"Nats-Msg-Id": out["id"]})

    sub = await nc.subscribe(in_subj, cb=handler)
    try:
        yield out_subj
    finally:
        await sub.unsubscribe()
