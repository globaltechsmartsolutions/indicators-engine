# src/indicators_engine/pipelines/cvd.py
from __future__ import annotations
from collections import defaultdict
from typing import Optional, Dict, Any

class CvdCalc:
    """
    Cumulative Volume Delta (CVD) incremental, sin pandas.
    Mantiene estado por clave (symbol|tf). Suma (BuyVol - SellVol) por trade.
    - Si hay 'side'/'aggressor' la usa (B/BUY, S/SELL).
    - Si hay bid/ask, clasifica por cruce con el spread (>=ask -> buy, <=bid -> sell, en medio -> tick rule).
    - Si no hay nada, usa tick rule respecto al último precio.
    Reinicia al cambiar de día si se configura reset_daily=True.
    """

    def __init__(self, *, reset_daily: bool = True):
        self.reset_daily = reset_daily
        self.state = defaultdict(lambda: {
            "last_ts": None,
            "last_price": None,
            "last_dir": 0,      # 1=buy, -1=sell, 0=no definido
            "cum": 0.0,
            "day": None,
        })

    def _key(self, symbol: str, tf: Optional[str]) -> str:
        return f"{symbol}|{tf or '-'}"

    @staticmethod
    def _day_from_ts(ts_ms: int) -> int:
        # Día en epoch-days (UTC) para resets por sesión simple
        return int(ts_ms // 86_400_000)

    @staticmethod
    def _norm_side(side: Optional[str]) -> Optional[int]:
        if side is None:
            return None
        s = str(side).strip().lower()
        if s in ("b", "buy", "bid", "aggressor_buy", "buyer"):
            return 1
        if s in ("s", "sell", "ask", "aggressor_sell", "seller"):
            return -1
        return None

    def _infer_side(
            self,
            price: float,
            size: float,
            bid: Optional[float],
            ask: Optional[float],
            last_price: Optional[float],
            last_dir: int,
    ) -> int:
        # 1) Con bid/ask
        if bid is not None and ask is not None:
            if price >= ask:
                return 1
            if price <= bid:
                return -1
            # En el spread -> tick rule
        # 2) Tick rule básica
        if last_price is not None:
            if price > last_price:
                return 1
            if price < last_price:
                return -1
        # 3) Igual precio o sin referencia previa:
        #    - Si no hay last_price (primer trade sin info) => asume BUY para inicializar memoria
        #    - Si hay mismo precio => reutiliza última dirección
        if last_price is None:
            return 1
        return last_dir or 0

    def reset(self, symbol: str, tf: Optional[str] = None):
        self.state.pop(self._key(symbol, tf), None)

    def get_value(self, symbol: str, tf: Optional[str] = None) -> float:
        return float(self.state[self._key(symbol, tf)]["cum"])

    def on_trade(
            self,
            symbol: str,
            ts: int,
            price: float,
            size: float,
            *,
            tf: Optional[str] = None,
            bid: Optional[float] = None,
            ask: Optional[float] = None,
            side: Optional[str] = None,         # 'B'/'S' o 'BUY'/'SELL'
            aggressor: Optional[str] = None,    # alias de side
    ) -> float:
        key = self._key(symbol, tf)
        s = self.state[key]

        # Reset diario simple
        if self.reset_daily:
            day = self._day_from_ts(ts)
            if s["day"] is not None and day != s["day"]:
                s.update({"last_ts": None, "last_price": None, "last_dir": 0, "cum": 0.0})
            s["day"] = day

        # Ignora mensajes fuera de orden (permitimos iguales)
        if s["last_ts"] is not None and ts < s["last_ts"]:
            return s["cum"]
        s["last_ts"] = ts

        # Determinar dirección
        dir_from_side = self._norm_side(side) or self._norm_side(aggressor)
        if dir_from_side is None:
            trade_dir = self._infer_side(price, size, bid, ask, s["last_price"], s["last_dir"])
        else:
            trade_dir = dir_from_side

        # Acumular
        if trade_dir > 0:
            s["cum"] += float(size)
        elif trade_dir < 0:
            s["cum"] -= float(size)
        # dir==0 -> neutro, no acumula

        # Actualizar últimos
        if s["last_price"] is None or price != s["last_price"]:
            s["last_dir"] = trade_dir if trade_dir != 0 else s["last_dir"]
        s["last_price"] = price

        return float(s["cum"])
