# src/indicators_engine/indicators/volume/svp.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Callable, Hashable

from ...core.base import StreamIndicator, TsOrderedMixin
from ...core.types import Bar, Trade
from ...core.utils import is_finite


@dataclass(slots=True)
class SVPConfig:
    """
    Session Volume Profile (reinicia por sesión/día).
    - session_key_fn(ts_ms) -> id de sesión (e.g., 'YYYY-MM-DD').
    - price_step: tamaño de bin; si None -> tick_size.
    - bar_mode: 'typical' o 'close' para acumular con velas si no hay ticks.
    - top_n: cuántos niveles devolver en snapshot_top(); 0 = todos.
    """
    session_key_fn: Callable[[int], Hashable]
    price_step: Optional[float] = None
    tick_size: float = 0.01
    bar_mode: str = "typical"
    top_n: int = 10


class SessionVolumeProfile(StreamIndicator, TsOrderedMixin):
    def __init__(self, cfg: SVPConfig):
        TsOrderedMixin.__init__(self)
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        # ── AHORA: estado por símbolo ───────────────────────────────────────────
        self._session_by_sym: Dict[str, Hashable] = {}
        self._bins_by_sym: Dict[str, Dict[float, float]] = {}
        # Para compatibilidad: último símbolo visto si no se pasa 'symbol' en snapshot*
        self._last_symbol: Optional[str] = None

    # ---- cuantización ----
    def _step(self) -> float:
        return self.cfg.price_step if (self.cfg.price_step and self.cfg.price_step > 0) else self.cfg.tick_size

    def _bin_for(self, price: float) -> float:
        s = self._step()
        # cuantiza al múltiplo más cercano de s, con redondeo estable
        return round(round(price / s) * s, 10)

    def _roll_if_needed(self, sym: str, ts: int) -> None:
        sid = self.cfg.session_key_fn(ts)
        if self._session_by_sym.get(sym) != sid:
            self._session_by_sym[sym] = sid
            self._bins_by_sym[sym] = {}

    def _bins(self, sym: str) -> Dict[float, float]:
        return self._bins_by_sym.setdefault(sym, {})

    # ---- API ----
    def on_bar(self, bar: Bar) -> Optional[dict]:
        # orden temporal (si aplica)
        if not self.allow_ts(bar.ts):
            return None
        sym = getattr(bar, "symbol", None)
        if not sym:
            return None
        self._last_symbol = sym
        self._roll_if_needed(sym, bar.ts)

        vol = bar.volume if is_finite(bar.volume) else 0.0
        if vol <= 0.0:
            return None

        if self.cfg.bar_mode == "close":
            px = bar.close
        else:
            if not (is_finite(bar.high) and is_finite(bar.low) and is_finite(bar.close)):
                return None
            px = (bar.high + bar.low + bar.close) / 3.0

        if not is_finite(px):
            return None

        b = self._bin_for(px)
        bins = self._bins(sym)
        bins[b] = bins.get(b, 0.0) + float(vol)
        return None  # snapshot bajo demanda

    def on_trade(self, trade: Trade) -> Optional[dict]:
        sym = getattr(trade, "symbol", None)
        if not sym:
            return None
        self._last_symbol = sym
        self._roll_if_needed(sym, trade.ts)

        if not (is_finite(trade.price) and is_finite(trade.size)):
            return None
        b = self._bin_for(trade.price)
        bins = self._bins(sym)
        bins[b] = bins.get(b, 0.0) + float(trade.size)
        return None

    # ---- snapshots ----
    def snapshot(self, symbol: Optional[str] = None) -> dict:
        sym = symbol or self._last_symbol
        items = sorted(self._bins_by_sym.get(sym, {}).items())
        if not items:
            return {"symbol": sym, "bins": [], "total_v": 0.0, "poc": None}

        poc_price, poc_vol = max(items, key=lambda kv: kv[1])
        total_v = float(sum(v for _, v in items))
        return {"symbol": sym, "bins": items, "total_v": total_v, "poc": (poc_price, poc_vol)}

    def snapshot_top(self, n: Optional[int] = None, symbol: Optional[str] = None) -> List[Tuple[float, float]]:
        n = n if n is not None else self.cfg.top_n
        sym = symbol or self._last_symbol
        items = sorted(self._bins_by_sym.get(sym, {}).items(), key=lambda kv: kv[1], reverse=True)
        return items[:n] if (n and n > 0) else items


# Mantén compatibilidad con import SVP:
class SVP(SessionVolumeProfile):
    pass
