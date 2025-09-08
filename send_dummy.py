import asyncio, orjson, time
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect("nats://172.29.57.199:4222")

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
    print("✅ Enviada candle dummy:", msg)
    await nc.drain()

if __name__ == "__main__":
    asyncio.run(main())
