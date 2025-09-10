import asyncio
import time
import orjson
from nats.aio.client import Client as NATS

NATS_URL = "nats://127.0.0.1:4222"

async def main():
    nc = NATS()
    await nc.connect(NATS_URL)
    print(f"✅ Dummy conectado a: {nc.connected_url.geturl()}")

    base_ts = (int(time.time()) // 60) * 60 * 1000  # inicio del minuto (ms)
    symbol = "ESZ5"
    tf = "1m"
    price = 648.0

    for i in range(20):
        ts = base_ts + i * 60_000  # +1 minuto por vela
        o = price
        step = (-0.05, 0.0, 0.05)[i % 3]  # variación suave determinista
        c = round(o + step, 4)
        h = round(max(o, c) + 0.2, 4)
        l = round(min(o, c) - 0.2, 4)
        v = 12345 + i

        msg = {
            "symbol": symbol,
            "tf": tf,
            "ts": ts,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v
        }

        await nc.publish("market.candles.1m", orjson.dumps(msg))
        print(f"✅ Enviada candle {i+1:02d}/20:", msg)

        price = c  # siguiente vela arranca desde el close anterior

    await nc.flush()
    await nc.drain()
    print("🎯 Listo: 20 velas publicadas.")

if __name__ == "__main__":
    asyncio.run(main())
