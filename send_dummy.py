import asyncio
import time
import orjson
from nats.aio.client import Client as NATS

NATS_URL = "nats://127.0.0.1:4222"

async def main():
    nc = NATS()
    await nc.connect(NATS_URL)
    print(f"✅ Dummy conectado a: {nc.connected_url.geturl()}")

    ts = int(time.time() * 1000)
    msg = {
        "symbol": "ESZ5",
        "tf": "1m",
        "ts": ts,
        "open": 648.2,
        "high": 648.5,
        "low": 647.9,
        "close": 648.1,
        "volume": 12345
    }

    await nc.publish("market.candles.1m", orjson.dumps(msg))
    await nc.flush()
    print("✅ Enviada candle dummy:", msg)

    await nc.drain()

if __name__ == "__main__":
    asyncio.run(main())
