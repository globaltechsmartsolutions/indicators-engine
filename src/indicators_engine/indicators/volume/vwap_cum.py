from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from ...core.base import StreamIndicator, TsOrderedMixin
from ...core.types import Bar, Trade
from ...core.utils import is_finite, safe_div

@dataclass(slots=True)
class VWAPCumConfig:
    """
    VWAP acumulado por sesión.
    - Si hay trades: usa sum(price*size)/sum(size)
    - Si no hay trades: on_bar usa TP=(H+L+C)/3 * V como aproximación
    - session_key_fn(ts_ms) → id de sesión (e.g. 'YYYY-MM-DD' en tz de mercado)
    """
    session_key_fn: Optional[callable] = None  # si None, no reinicia automáticamente

class VWAPCum(StreamIndicator, TsOrderedMixin):
    def __init__(self, cfg: VWAPCumConfig = VWAPCumConfig()):
        TsOrderedMixin.__init__(self)
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self._session_id = None
        self._pv_sum = 0.0
        self._v_sum = 0.0

    def reset_session(self) -> None:
        self._pv_sum = 0.0
        self._v_sum = 0.0

    def _maybe_roll_session(self, ts_ms: int) -> None:
        if not self.cfg.session_key_fn:
            return
        sid = self.cfg.session_key_fn(ts_ms)
        if sid != self._session_id:
            self._session_id = sid
            self.reset_session()

    # ---- API ----
    def on_bar(self, bar: Bar) -> Optional[float]:
        if not self.allow_ts(bar.ts):
            return None
        self._maybe_roll_session(bar.ts)

        if not (is_finite(bar.high) and is_finite(bar.low) and is_finite(bar.close)):
            return None
        tp = (bar.high + bar.low + bar.close) / 3.0
        vol = bar.volume if is_finite(bar.volume) else 0.0

        self._pv_sum += tp * vol
        self._v_sum  += vol
        if self._v_sum <= 0.0:
            return None
        return float(safe_div(self._pv_sum, self._v_sum, default=0.0))

    def on_trade(self, trade: Trade) -> Optional[float]:
        self._maybe_roll_session(trade.ts)
        if not (is_finite(trade.price) and is_finite(trade.size)):
            return None
        self._pv_sum += float(trade.price) * float(trade.size)
        self._v_sum  += float(trade.size)
        if self._v_sum <= 0.0:
            return None
        return float(safe_div(self._pv_sum, self._v_sum, default=0.0))

    def snapshot(self) -> dict:
        vwap = safe_div(self._pv_sum, self._v_sum, default=0.0) if self._v_sum > 0 else None
        return {
            "session_id": self._session_id,
            "pv_sum": self._pv_sum,
            "v_sum": self._v_sum,
            "vwap": vwap,
        }
