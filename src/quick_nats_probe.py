# quick_nats_probe.py
import asyncio
from nats.aio.client import Client as NatsClient

async def main():
    nc = NatsClient()
    print("[probe] connecting…")
    await nc.connect("nats://127.0.0.1:4222", name="probe")
    print("[probe] connected ✔")
    await nc.close()
    print("[probe] closed ✔")

asyncio.run(main())
