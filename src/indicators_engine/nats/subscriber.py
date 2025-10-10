from __future__ import annotations
import asyncio, configparser, logging, re
from typing import Callable, Optional, Dict, Any

import orjson
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

log = logging.getLogger("subscriber")

# md.candles.<tf>[...]
_CANDLE_TF_RE = re.compile(r"^md\.candles\.([^\.]+)(?:\.|$)")

def _tf_from_subject(subj: str) -> Optional[str]:
    m = _CANDLE_TF_RE.match(subj)
    return m.group(1) if m else None


class NATSSubscriber:
    """
    Encargado de:
      - leer settings.ini
      - conectar a NATS
      - suscribirse a los subjects
      - enrutar mensajes a callbacks (ya parseados como dict)
    """
    def __init__(self, ini_path: str):
        self.cfg = configparser.ConfigParser()
        if not self.cfg.read(ini_path):
            raise FileNotFoundError(f"No se pudo leer el INI: {ini_path}")

        self.url = self.cfg.get("NATS", "url", fallback="nats://127.0.0.1:4222")

        s_in = self.cfg["SubjectsIn"]
        self.subj_bbo    = s_in.get("bbo", "md.bbo.frame")
        self.subj_book   = s_in.get("book", "md.book.frame")
        self.subj_candle = s_in.get("candles", "md.candles.>")
        self.subj_tvwap  = s_in.get("trades_vwap", "md.trades.vwap")
        self.subj_toflow = s_in.get("trades_oflow", "md.trades.oflow")

        self.nc = NATS()

        # Callbacks (se asignan desde fuera)
        self.cb_candle: Optional[Callable[[Dict[str, Any]], None]] = None
        self.cb_trade : Optional[Callable[[Dict[str, Any]], None]] = None   # trades "reales"
        self.cb_oflow : Optional[Callable[[Dict[str, Any]], None]] = None   # frames agregados buy/sell/delta
        self.cb_book  : Optional[Callable[[Dict[str, Any]], None]] = None

    async def connect(self):
        await self.nc.connect(servers=[self.url])
        log.info(f"[NATS] Conectado → {self.url}")

    async def _handle_tvwap(self, msg: Msg):
        d = orjson.loads(msg.data)
        try:
            if self.cb_trade:
                await self.cb_trade(d)
        except Exception as e:
            log.error(f"[tvwap] callback error: {e} payload={d}", exc_info=True)

    async def _handle_toflow(self, msg: Msg):
        d = orjson.loads(msg.data)
        try:
            if self.cb_oflow:
                await self.cb_oflow(d)
            elif self.cb_trade:
                # compat: si no definiste cb_oflow, reusa cb_trade
                await self.cb_trade(d)
        except Exception as e:
            log.error(f"[oflow] callback error: {e} payload={d}", exc_info=True)

    async def _handle_candle(self, msg: Msg):
        d = orjson.loads(msg.data)
        if not d.get("tf"):
            tf = _tf_from_subject(msg.subject)
            if tf:
                d["tf"] = tf
        try:
            if self.cb_candle:
                await self.cb_candle(d)
        except Exception as e:
            log.error(f"[candle] callback error: {e} payload={d}", exc_info=True)

    async def _handle_book(self, msg: Msg):
        d = orjson.loads(msg.data)
        try:
            if self.cb_book:
                await self.cb_book(d)
        except Exception as e:
            log.error(f"[book] callback error: {e} payload={d}", exc_info=True)

    async def run(self):
        # Suscripciones
        await self.nc.subscribe(self.subj_candle, cb=self._handle_candle)
        await self.nc.subscribe(self.subj_tvwap,  cb=self._handle_tvwap)
        await self.nc.subscribe(self.subj_toflow, cb=self._handle_toflow)
        await self.nc.subscribe(self.subj_book,   cb=self._handle_book)
        await self.nc.subscribe(self.subj_bbo,    cb=self._handle_book)

        log.info("Suscripciones activas:")
        log.info(f"  • {self.subj_candle}")
        log.info(f"  • {self.subj_tvwap}")
        log.info(f"  • {self.subj_toflow}")
        log.info(f"  • {self.subj_book}")
        log.info(f"  • {self.subj_bbo}")

        try:
            while True:
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass
        finally:
            await self.nc.drain()
            log.info("[NATS] cerrado")
