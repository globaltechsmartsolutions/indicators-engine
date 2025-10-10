from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict

from ...core.base import StreamIndicator, WarmupState, TsOrderedMixin
from ...core.types import Bar
from ...core.utils import ema_step, is_finite

@dataclass(slots=True)
class MACDConfig:
    fast: int = 12
    slow: int = 26
    signal: int = 9
    # regla práctica: esperar a tener la señal estable
    warmup_extra: int = 0  # puedes subirlo a 1–2 si quieres

class MACD(StreamIndicator, TsOrderedMixin):
    """
    MACD clásico (EMA12, EMA26, SIGNAL9). Devuelve:
      { 'macd': float, 'signal': float, 'hist': float }
    """
    def __init__(self, cfg: MACDConfig = MACDConfig()):
        TsOrderedMixin.__init__(self)
        assert cfg.fast > 0 and cfg.slow > 0 and cfg.signal > 0
        assert cfg.fast < cfg.slow  # convención estándar
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self._ema_fast: Optional[float] = None
        self._ema_slow: Optional[float] = None
        self._signal: Optional[float] = None
        # warm-up razonable: que exista EMA_slow y SIGNAL
        need = self.cfg.slow + self.cfg.signal + self.cfg.warmup_extra
        self._wu = WarmupState(need)

    def on_bar(self, bar: Bar) -> Optional[Dict[str, float]]:
        if not self.allow_ts(bar.ts):
            return None
        c = bar.close
        if not is_finite(c):
            return None

        self._ema_fast = ema_step(self._ema_fast, c, self.cfg.fast)
        self._ema_slow = ema_step(self._ema_slow, c, self.cfg.slow)

        macd_val = None
        if self._ema_fast is not None and self._ema_slow is not None:
            macd_val = self._ema_fast - self._ema_slow
            self._signal = ema_step(self._signal, macd_val, self.cfg.signal)

        if self._wu.tick():
            return None
        if macd_val is None or self._signal is None:
            return None

        hist = macd_val - self._signal
        return {"macd": float(macd_val), "signal": float(self._signal), "hist": float(hist)}
