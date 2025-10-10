from __future__ import annotations
import asyncio, os, logging

from indicators_engine.logs.liveRenderer import LiveRenderer
from indicators_engine.nats.publisher import IndicatorPublisher
from indicators_engine.nats.subscriber import NATSSubscriber
from indicators_engine.engine import IndicatorsEngine

log = logging.getLogger("runner")
logging.basicConfig(
    level=getattr(logging, os.getenv("ENGINE_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)-7s [runner] %(message)s",
)

async def main():
    ini = os.getenv("ENGINE_INI", "settings.ini")

    # Componentes
    sub = NATSSubscriber(ini)
    await sub.connect()

    pub = IndicatorPublisher(sub.nc, out_prefix=_read_out_prefix(ini))
    ui  = LiveRenderer(fps=4.0)
    eng = IndicatorsEngine(pub, ui)

    # Wirear callbacks
    sub.cb_candle = eng.on_candle_dict
    sub.cb_trade  = eng.on_trade_dict       # md.trades.vwap
    sub.cb_oflow  = eng.on_oflow_frame_dict # md.trades.oflow   ← NUEVO
    sub.cb_book   = eng.on_book_dict

    # Lanzar renderer y subscriber
    renderer_task = asyncio.create_task(ui.run())
    try:
        await sub.run()
    finally:
        renderer_task.cancel()

def _read_out_prefix(ini_path: str) -> str:
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(ini_path)
    return cfg.get("IndicatorsOut", "prefix", fallback="indicators")

if __name__ == "__main__":
    asyncio.run(main())
