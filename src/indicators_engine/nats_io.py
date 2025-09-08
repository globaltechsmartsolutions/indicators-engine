from nats.aio.client import Client as NATS

async def connect_nats(url: str):
    nc = NATS()
    await nc.connect(servers=[url])
    js = nc.jetstream()
    return nc, js
