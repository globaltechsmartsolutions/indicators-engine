from __future__ import annotations
import math
from typing import Iterable, Optional
from indicators_engine.logs.logger import get_logger
logger = get_logger(__name__)


# ========= helpers numéricos =========

def is_finite(x) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception:
        return False

def safe_float(x, default: float | None = None) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default

def safe_div(n: float, d: float, default: float = 0.0) -> float:
    try:
        if d == 0:
            return default
        v = n / d
        return v if math.isfinite(v) else default
    except Exception:
        return default


# ========= medias exponenciales / Wilder =========

def ema_step(prev: float | None, x: float, period: int) -> float:
    """
    Paso de EMA con periodo N (alpha = 2/(N+1)).
    Si prev es None -> devuelve x (semilla).
    """
    if prev is None:
        return x
    alpha = 2.0 / (period + 1.0)
    return (1.0 - alpha) * prev + alpha * x

def rma_step(prev: float | None, x: float, period: int) -> float:
    """
    RMA de Wilder (equivalente a EMA(alpha=1/N)).
    Si prev es None -> devuelve x (semilla).
    """
    if prev is None:
        return x
    alpha = 1.0 / float(period)
    return (1.0 - alpha) * prev + alpha * x


# ========= buffers simples =========

class RingBuffer:
    """Buffer circular simple para ventanas fijas."""
    __slots__ = ("size", "_buf", "_i", "_count")

    def __init__(self, size: int):
        assert size > 0
        self.size = int(size)
        self._buf = [0.0] * self.size
        self._i = 0
        self._count = 0

    def push(self, x: float) -> None:
        self._buf[self._i] = float(x)
        self._i = (self._i + 1) % self.size
        self._count = min(self._count + 1, self.size)

    def sum(self) -> float:
        return float(__builtins__["sum"](self._buf[: self._count]))

    def __len__(self) -> int:
        return self._count

    def values(self) -> Iterable[float]:
        if self._count < self.size:
            return self._buf[: self._count]
        # orden cronológico
        i = self._i
        return self._buf[i:] + self._buf[:i]