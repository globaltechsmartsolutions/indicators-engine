# src/indicators_engine/nats/publisher.py
from __future__ import annotations
from typing import Any, Dict
import orjson
from nats.aio.client import Client as NATS


class IndicatorPublisher:
    """
    Clase responsable de publicar los resultados de indicadores en NATS.
    No depende de la lógica de cálculo (separación clara de responsabilidades).
    """

    def __init__(self, nc: NATS, out_prefix: str = "indicators"):
        self.nc = nc
        self.out_prefix = out_prefix.rstrip(".")

    async def publish_candle(self, tf: str, name: str, symbol: str, payload: Dict[str, Any]):
        """
        Publica indicadores basados en velas (RSI, MACD, ADX, etc.).
        """
        subj = f"{self.out_prefix}.candles.{tf}.{name}"
        await self.nc.publish(subj, orjson.dumps({**payload, "symbol": symbol, "tf": tf}))

    async def publish_book(self, name: str, symbol: str, payload: Dict[str, Any]):
        """
        Publica indicadores derivados del libro de órdenes (liquidity, heatmap).
        """
        subj = f"{self.out_prefix}.book.{name}"
        await self.nc.publish(subj, orjson.dumps({**payload, "symbol": symbol}))

    async def publish_trades(self, name: str, symbol: str, payload: Dict[str, Any]):
        """
        Publica indicadores derivados de trades (vwap, cvd, etc.).
        """
        subj = f"{self.out_prefix}.trades.{name}"
        await self.nc.publish(subj, orjson.dumps({**payload, "symbol": symbol}))
