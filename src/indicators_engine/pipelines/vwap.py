from __future__ import annotations
from collections import defaultdict
from typing import Optional

class VwapCalc:
    """
    VWAP incremental por clave (symbol|tf).
    Mantiene sumatorios:
      - cum_pv = Σ (price * size)
      - cum_vol = Σ size
    vwap = cum_pv / cum_vol  (si cum_vol > 0)
    """

    def __init__(self, *, reset_daily: bool = True, reset_on_session_id: bool = False):
        self.reset_daily = reset_daily
        self.reset_on_session_id = reset_on_session_id
        self.state = defaultdict(lambda: {
            "last_ts": None,
            "day": None,
            "session_id": None,
            "cum_pv": 0.0,
            "cum_vol": 0.0,
            "vwap": None,
        })

    def _key(self, symbol: str, tf: Optional[str]) -> str:
        return f"{symbol}|{tf or '-'}"

    @staticmethod
    def _day_from_ts(ts_ms: int) -> int:
        return int(ts_ms // 86_400_000)

    def reset(self, symbol: str, tf: Optional[str] = None):
        self.state.pop(self._key(symbol, tf), None)

    def get_value(self, symbol: str, tf: Optional[str] = None) -> Optional[float]:
        return self.state[self._key(symbol, tf)]["vwap"]

    def on_trade(
            self,
            *,
            symbol: str,
            ts: int,
            price: float,
            size: float,
            tf: Optional[str] = None,
            session_id: Optional[str] = None,
    ) -> Optional[float]:
        if size is None or price is None:
            return self.get_value(symbol, tf)
        if float(size) <= 0.0:
            return self.get_value(symbol, tf)

        key = self._key(symbol, tf)
        s = self.state[key]

        # Reset por día
        if self.reset_daily and ts is not None:
            day = self._day_from_ts(int(ts))
            if s["day"] is not None and day != s["day"]:
                s.update({"last_ts": None, "day": day, "session_id": None,
                          "cum_pv": 0.0, "cum_vol": 0.0, "vwap": None})
            else:
                s["day"] = day

        # Reset por session_id explícito
        if self.reset_on_session_id and session_id is not None:
            if s["session_id"] is not None and session_id != s["session_id"]:
                s.update({"last_ts": None, "day": s.get("day"), "session_id": session_id,
                          "cum_pv": 0.0, "cum_vol": 0.0, "vwap": None})
            else:
                s["session_id"] = session_id

        if s["last_ts"] is not None and ts < s["last_ts"]:
            return s["vwap"]
        s["last_ts"] = ts

        s["cum_pv"] += float(price) * float(size)
        s["cum_vol"] += float(size)

        if s["cum_vol"] > 0.0:
            s["vwap"] = s["cum_pv"] / s["cum_vol"]
        else:
            s["vwap"] = None

        return s["vwap"]
