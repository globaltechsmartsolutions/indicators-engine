# src/indicators_engine/pipelines/macd.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, TypedDict, cast

class MacdPoint(TypedDict):
    macd: float
    signal: float
    hist: float

@dataclass
class _MacdState:
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    signal: Optional[float] = None
    last_ts: Optional[int] = None

class MacdCalc:
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        if fast < 1 or slow < 1 or signal < 1:
            raise ValueError("MACD periods must be >= 1")
        if fast >= slow:
            raise ValueError("fast must be < slow")
        self.fast = fast
        self.slow = slow
        self.signal_period = signal
        self.k_fast = 2.0 / (fast + 1.0)
        self.k_slow = 2.0 / (slow + 1.0)
        self.k_signal = 2.0 / (signal + 1.0)
        self._state: Dict[str, _MacdState] = {}

    def on_bar(self, symbol: str, tf: str, ts: int, close: float) -> Optional[MacdPoint]:
        key = f"{symbol}|{tf}"
        s = self._state.setdefault(key, _MacdState())

        if s.last_ts is not None and ts <= s.last_ts:
            return None
        s.last_ts = ts

        if s.ema_fast is None or s.ema_slow is None:
            s.ema_fast = close
            s.ema_slow = close
            return None

        ema_fast = (close - s.ema_fast) * self.k_fast + s.ema_fast
        ema_slow = (close - s.ema_slow) * self.k_slow + s.ema_slow
        s.ema_fast, s.ema_slow = ema_fast, ema_slow

        macd_val = ema_fast - ema_slow

        if s.signal is None:
            s.signal = macd_val
            return None  # emite desde la siguiente barra

        signal_val = (macd_val - s.signal) * self.k_signal + s.signal
        s.signal = signal_val
        hist = macd_val - signal_val

        return cast(MacdPoint, {"macd": float(macd_val),
                                "signal": float(signal_val),
                                "hist": float(hist)})
