from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

from ...core.base import StreamIndicator, TsOrderedMixin
from ...core.types import Bar, Trade
from ...core.utils import is_finite


@dataclass(slots=True)
class VolumeProfileConfig:
    """
    Perfil de volumen (histograma) continuo.
    - price_step: tamaño del bin en precios (p.ej. 0.05). Si None -> usa tick_size.
    - tick_size: para cuantizar precios si no se define price_step.
    - bar_mode: 'close' suma todo el volumen al bin del close; 'typical' al (H+L+C)/3.
    """
    price_step: Optional[float] = None
    tick_size: float = 0.01
    bar_mode: str = "typical"  # 'close' | 'typical'
    top_n: int = 0             # 0 = no limitar; >0 para snapshot_top


class VolumeProfile(StreamIndicator, TsOrderedMixin):
    def __init__(self, cfg: VolumeProfileConfig = VolumeProfileConfig()):
        TsOrderedMixin.__init__(self)
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self._bins: Dict[float, float] = {}  # price_bin -> volume
        self._last_ts: Optional[int] = None

    # ---- cuantización ----
    def _step(self) -> float:
        return self.cfg.price_step if self.cfg.price_step and self.cfg.price_step > 0 else self.cfg.tick_size

    def _bin_for(self, price: float) -> float:
        s = self._step()
        return round(round(price / s) * s, 10)

    # ---- API ----
    def on_bar(self, bar: Bar) -> Optional[dict]:
        if not self.allow_ts(bar.ts):
            return None
        vol = bar.volume if is_finite(bar.volume) else 0.0
        if vol <= 0.0:
            return None

        if self.cfg.bar_mode == "close":
            px = bar.close
        else:  # 'typical'
            if not (is_finite(bar.high) and is_finite(bar.low) and is_finite(bar.close)):
                return None
            px = (bar.high + bar.low + bar.close) / 3.0

        if not is_finite(px):
            return None
        b = self._bin_for(px)
        self._bins[b] = self._bins.get(b, 0.0) + vol
        return None

    def on_trade(self, trade: Trade) -> Optional[dict]:
        if not (is_finite(trade.price) and is_finite(trade.size)):
            return None
        b = self._bin_for(trade.price)
        self._bins[b] = self._bins.get(b, 0.0) + trade.size
        return None

    # ---- snapshots ----
    def snapshot(self) -> dict:
        items = sorted(self._bins.items())  # asc por precio
        total_v = sum(v for _, v in items)
        return {"bins": items, "total_v": total_v}

    def snapshot_top(self, n: Optional[int] = None) -> List[Tuple[float, float]]:
        n = n if n is not None else self.cfg.top_n
        items = sorted(self._bins.items(), key=lambda kv: kv[1], reverse=True)
        return items[:n] if (n and n > 0) else items
