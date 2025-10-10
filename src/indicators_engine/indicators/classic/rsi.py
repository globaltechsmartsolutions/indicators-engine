from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from ...core.base import StreamIndicator, WarmupState, TsOrderedMixin
from ...core.types import Bar
from ...core.utils import rma_step, is_finite, safe_div


@dataclass(slots=True)
class RSIConfig:
    period: int = 14      # RSI N
    warmup_extra: int = 1 # mínimo recomendado: N + 1


class RSI(StreamIndicator, TsOrderedMixin):
    """
    RSI de Wilder (RMA). Devuelve float o None durante warm-up.
    """
    def __init__(self, cfg: RSIConfig = RSIConfig()):
        TsOrderedMixin.__init__(self)
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self._prev_close: Optional[float] = None
        self._avg_gain: Optional[float] = None
        self._avg_loss: Optional[float] = None
        self._wu = WarmupState(self.cfg.period + self.cfg.warmup_extra)

    def on_bar(self, bar: Bar) -> Optional[float]:
        if not self.allow_ts(bar.ts):  # descarta ts antiguos
            return None
        c = bar.close
        if not is_finite(c):
            return None

        if self._prev_close is None:
            self._prev_close = c
            self._wu.tick()
            return None

        change = c - self._prev_close
        self._prev_close = c
        gain = change if change > 0 else 0.0
        loss = -change if change < 0 else 0.0

        self._avg_gain = rma_step(self._avg_gain, gain, self.cfg.period)
        self._avg_loss = rma_step(self._avg_loss, loss, self.cfg.period)

        # warm-up
        if self._wu.tick():
            return None

        if not self._avg_loss or self._avg_loss == 0.0:
            return 100.0 if (self._avg_gain or 0.0) > 0 else 50.0

        rs = safe_div(self._avg_gain or 0.0, self._avg_loss, default=0.0)
        rsi = 100.0 - 100.0 / (1.0 + rs)
        return float(rsi)
