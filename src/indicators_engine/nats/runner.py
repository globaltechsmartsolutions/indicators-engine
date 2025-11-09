from __future__ import annotations
import asyncio, os

from indicators_engine.nats.publisher import IndicatorPublisher
from indicators_engine.nats.subscriber import NATSSubscriber
from indicators_engine.engine import IndicatorsEngine

async def main():
    ini = os.getenv("ENGINE_INI", "settings.ini")

    # Componentes
    sub = NATSSubscriber(ini)
    await sub.connect()

    pub = IndicatorPublisher(sub.nc, out_prefix=_read_out_prefix(ini))
    eng = IndicatorsEngine(pub)

    # Wirear callbacks
    sub.cb_candle = eng.on_candle_dict
    sub.cb_trade  = eng.on_trade_dict       # md.trades.vwap
    sub.cb_oflow  = eng.on_oflow_frame_dict # md.trades.oflow   ← NUEVO
    sub.cb_book   = eng.on_book_dict

    # Lanzar subscriber (sin renderer)
    try:
        await sub.run()
    finally:
        pass

def _read_out_prefix(ini_path: str) -> str:
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(ini_path)
    return cfg.get("IndicatorsOut", "prefix", fallback="indicators")

if __name__ == "__main__":
    asyncio.run(main())
