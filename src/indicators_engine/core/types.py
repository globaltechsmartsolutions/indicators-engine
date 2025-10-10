from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Optional, TypedDict, List, Dict, Any


# ==== Datos base y tipados ====

TF = str  # p.ej. "1m", "5m", "1h", "-"


@dataclass(slots=True)
class Bar:
    """Barra OHLCV atómica (una unidad temporal)."""
    ts: int           # epoch millis
    open: float
    high: float
    low: float
    close: float
    volume: float
    tf: TF
    symbol: str


@dataclass(slots=True)
class Trade:
    """Trade tick (por si algún indicador lo necesita)."""
    ts: int
    price: float
    size: float
    symbol: str
    exchange: Optional[str] = None
    side: Optional[str] = None  # opcional (BUY/SELL) si lo publicas desde extractor


@dataclass(slots=True)
class BookSnapshot:
    """
    Snapshot de libro L2, usado por indicadores 'book'
    como Liquidity y Heatmap.
    Espera listas tipo [{'p':precio, 'v':size}, ...].
    """
    ts: int
    symbol: str
    bids: List[Dict[str, Any]]
    asks: List[Dict[str, Any]]


# ==== Snapshots genéricos para indicadores ====

class MacdSnapshot(TypedDict, total=False):
    macd: float
    signal: float
    hist: float


class AdxSnapshot(TypedDict, total=False):
    plus_di: float
    minus_di: float
    adx: float
