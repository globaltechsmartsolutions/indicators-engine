import os

class Config:
    NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
