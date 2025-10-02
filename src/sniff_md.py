import asyncio, orjson
from nats.aio.client import Client as NATS

SUBJ = "md.>"
URL  = "nats://127.0.0.1:4222"

async def main():
    nc = NATS()
    await nc.connect(servers=[URL])
    print(f"✅ sniff conectado a {URL}, escuchando {SUBJ}")

    async def cb(msg):
        try:
            data = orjson.loads(msg.data)
        except Exception:
            data = msg.data.decode(errors="ignore")
        print(f"• {msg.subject}: {data}")

    await nc.subscribe(SUBJ, cb=cb)
    await nc.flush()
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
