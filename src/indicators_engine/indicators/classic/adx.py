from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from ...core.base import StreamIndicator, WarmupState, TsOrderedMixin
from ...core.types import Bar, AdxSnapshot           # ← usamos TypedDict
from ...core.utils import rma_step, safe_div, is_finite


@dataclass(slots=True)
class ADXConfig:
    period: int = 14
    warmup_extra: int = 1


class ADX(StreamIndicator, TsOrderedMixin):
    """
    ADX de Wilder.
    Devuelve AdxSnapshot o None durante warm-up.
    """
    def __init__(self, cfg: ADXConfig = ADXConfig()):
        TsOrderedMixin.__init__(self)
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self._prev_h: Optional[float] = None
        self._prev_l: Optional[float] = None
        self._prev_c: Optional[float] = None
        self._tr: Optional[float] = None
        self._dm_p: Optional[float] = None
        self._dm_m: Optional[float] = None
        self._adx: Optional[float] = None
        self._wu = WarmupState(self.cfg.period + self.cfg.warmup_extra)

    def on_bar(self, bar: Bar) -> Optional[AdxSnapshot]:   # ← TypedDict
        if not self.allow_ts(bar.ts):
            return None
        h, l, c = bar.high, bar.low, bar.close
        if not (is_finite(h) and is_finite(l) and is_finite(c)):
            return None

        if self._prev_c is None:
            self._prev_h, self._prev_l, self._prev_c = h, l, c
            self._wu.tick()
            return None

        ph, pl, pc = self._prev_h, self._prev_l, self._prev_c
        self._prev_h, self._prev_l, self._prev_c = h, l, c

        tr   = max(h - l, abs(h - pc), abs(l - pc))
        up   = h - ph
        down = pl - l
        dm_p = up if (up > 0.0 and up > down) else 0.0
        dm_m = down if (down > 0.0 and down > up) else 0.0

        self._tr   = rma_step(self._tr,   tr,   self.cfg.period)
        self._dm_p = rma_step(self._dm_p, dm_p, self.cfg.period)
        self._dm_m = rma_step(self._dm_m, dm_m, self.cfg.period)

        if self._wu.tick():
            return None
        if not self._tr or self._tr == 0.0:
            return None

        pdi = 100.0 * safe_div(self._dm_p or 0.0, self._tr, default=0.0)
        mdi = 100.0 * safe_div(self._dm_m or 0.0, self._tr, default=0.0)
        denom = pdi + mdi
        if denom == 0.0:
            return None

        dx = 100.0 * abs(pdi - mdi) / denom
        self._adx = rma_step(self._adx, dx, self.cfg.period)

        if self._adx is None:
            return None

        out: AdxSnapshot = {
            "plus_di": float(pdi),
            "minus_di": float(mdi),
            "adx": float(self._adx),
        }
        return out
