import os

class Config:
    NATS_URL = os.getenv("NATS_URL", "nats://172.29.57.199:4222")
