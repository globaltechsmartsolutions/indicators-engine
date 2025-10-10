from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal, Callable, Dict

from ...core.base import StreamIndicator
from ...core.types import Trade
from ...core.utils import is_finite


Side = Literal["BUY", "SELL", "NA"]


@dataclass(slots=True)
class CVDConfig:
    """
    Cumulative Volume Delta:
    - Si 'side' viene en Trade → se usa directamente.
    - Si falta, puedes estimar usando:
        1) un 'quote_resolver' opcional (symbol -> dict(bid, ask))
        2) o un 'mid_resolver' (symbol -> mid)
      Reglas:
        price >= ask -> BUY
        price <= bid -> SELL
        en medio, usa mid: price > mid -> BUY, price < mid -> SELL, si empata -> NA
    """
    quote_resolver: Optional[Callable[[str], Optional[Dict[str, float]]]] = None
    mid_resolver: Optional[Callable[[str], Optional[float]]] = None
    reset_on_symbol_change: bool = False  # útil si usas un mismo objeto para varios symbols


class CVD(StreamIndicator):
    def __init__(self, cfg: CVDConfig = CVDConfig()):
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self._cvd: float = 0.0
        self._sym: Optional[str] = None

    def _resolve_side(self, t: Trade) -> Side:
        # 1) lado ya viene en el trade
        side: Optional[str] = getattr(t, "side", None)
        if side and isinstance(side, str):
            s = side.upper()
            if s in ("BUY", "SELL"):
                return s  # respeta la fuente

        # 2) intenta con quote_resolver (bid/ask)
        if self.cfg.quote_resolver:
            q = self.cfg.quote_resolver(t.symbol)
            if q and is_finite(q.get("bid")) and is_finite(q.get("ask")):
                bid = float(q["bid"]); ask = float(q["ask"])
                if t.price >= ask:  return "BUY"
                if t.price <= bid:  return "SELL"
                mid = (bid + ask) / 2.0
                if t.price > mid:   return "BUY"
                if t.price < mid:   return "SELL"
                return "NA"

        # 3) intenta con mid_resolver
        if self.cfg.mid_resolver:
            mid = self.cfg.mid_resolver(t.symbol)
            if is_finite(mid):
                if t.price > mid:  return "BUY"
                if t.price < mid:  return "SELL"
                return "NA"

        return "NA"

    # ---- API streaming ----
    def on_trade(self, t: Trade) -> Optional[dict]:
        if self.cfg.reset_on_symbol_change and self._sym and t.symbol != self._sym:
            self.reset()
        self._sym = t.symbol

        if not (is_finite(t.price) and is_finite(t.size)):
            return None

        side = self._resolve_side(t)
        if side == "BUY":
            self._cvd += float(t.size)
        elif side == "SELL":
            self._cvd -= float(t.size)

        return {"cvd": self._cvd, "last_side": side, "last_size": float(t.size)}
