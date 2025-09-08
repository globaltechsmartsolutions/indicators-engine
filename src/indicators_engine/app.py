import asyncio
import orjson
from .config import Config
from .nats_io import connect_nats
from .pipelines.rsi import RsiCalc

OUT_SUBJECT = "indicators.candles.1m.rsi14"
IN_SUBJECT = "market.candles.1m"

async def main():
    cfg = Config()
    nc, js = await connect_nats(cfg.NATS_URL)
    print(f"✅ Conectado a NATS en {cfg.NATS_URL}")

    rsi = RsiCalc(period=3)

    async def handler(msg):
        try:
            c = orjson.loads(msg.data)
            symbol = c["symbol"]
            tf = c["tf"]
            ts = int(c["ts"])
            close = float(c["close"])

            val = rsi.on_bar(symbol, tf, ts, close)
            if val is not None:
                out = {
                    "v": 1,
                    "source": "indicators-engine",
                    "symbol": symbol,
                    "tf": tf,
                    "ts": ts,
                    "indicator": "rsi14",
                    "value": val,
                    "id": f"{symbol}|{tf}|{ts}|rsi14"
                }
                await js.publish(OUT_SUBJECT, orjson.dumps(out),
                                 headers={"Nats-Msg-Id": out["id"]})
                print("→ RSI publicado:", out)
        except Exception as e:
            print("Error procesando candle:", e)

    await nc.subscribe(IN_SUBJECT, cb=handler)
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
