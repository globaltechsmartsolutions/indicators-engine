from __future__ import annotations
from typing import Optional
from dataclasses import dataclass

from ...core.base import StreamIndicator, TsOrderedMixin
from ...core.types import Bar
from ...core.utils import is_finite, safe_div


@dataclass(slots=True)
class VWAPBarConfig:
    """
    VWAP de 'la barra' usando precio típico (H+L+C)/3 * volumen de la barra.
    Reinicia por cada nueva barra (ts distinto).
    """
    pass


class VWAPBar(StreamIndicator, TsOrderedMixin):
    def __init__(self, cfg: VWAPBarConfig = VWAPBarConfig()):
        TsOrderedMixin.__init__(self)
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self._current_ts: Optional[int] = None
        self._tpv_sum: float = 0.0
        self._v_sum: float = 0.0

    def on_bar(self, bar: Bar) -> Optional[float]:
        # permitimos overwrite si llega misma ts (TsOrderedMixin ya lo maneja)
        if self._current_ts != bar.ts:
            self._current_ts = bar.ts
            self._tpv_sum = 0.0
            self._v_sum = 0.0

        if not (is_finite(bar.high) and is_finite(bar.low) and is_finite(bar.close)):
            return None

        tp = (bar.high + bar.low + bar.close) / 3.0
        vol = bar.volume if is_finite(bar.volume) else 0.0

        self._tpv_sum += tp * vol
        self._v_sum   += vol

        if self._v_sum <= 0.0:
            return None
        return safe_div(self._tpv_sum, self._v_sum, default=None)