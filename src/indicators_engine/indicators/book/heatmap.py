from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, List, Any

from ...core.base import StreamIndicator
from ...core.types import BookSnapshot
from ...core.utils import is_finite


@dataclass(slots=True)
class HeatmapConfig:
    """
    Acumula tamaño por (bucket temporal, precio cuantizado, lado).
    - bucket_ms: tamaño del cubo temporal (p.ej. 1000 ms)
    - tick_size: tamaño de cuantización de precio
    - max_rows_per_bucket: limita filas por bucket (para render rápido)
    """
    bucket_ms: int = 1000
    tick_size: float = 0.01
    max_rows_per_bucket: int = 200


def _bin_price(px: float, tick: float) -> float:
    return round(round(px / tick) * tick, 10)


def _norm_levels(side) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    if not side:
        return out
    for lv in side:
        if isinstance(lv, dict):
            p = lv.get("p", lv.get("price"))
            v = lv.get("v", lv.get("size"))
        else:
            try:
                p, v = lv[0], lv[1]
            except Exception:
                continue
        if is_finite(p) and is_finite(v):
            out.append((float(p), float(v)))
    return out


class Heatmap(StreamIndicator):
    """
    Cada snapshot L2 produce acumulaciones en el 'current bucket' = ts // bucket_ms.
    Devuelve snapshot compacto del bucket actual:
        { "bucket_ts": <inicio_bucket_ms>,
          "bucket_ms": <tam>,
          "rows": [ [price_bin, "bid"/"ask", size], ... ],
          "max_sz": float
        }
    """
    def __init__(self, cfg: HeatmapConfig = HeatmapConfig()):
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self._bucket_ts: Optional[int] = None
        self._grid: Dict[Tuple[float, str], float] = {}  # (price_bin, side) -> size acumulado

    def _roll_bucket_if_needed(self, ts_ms: int) -> None:
        bts = (ts_ms // self.cfg.bucket_ms) * self.cfg.bucket_ms
        if self._bucket_ts is None:
            self._bucket_ts = bts
            self._grid.clear()
            return
        if bts != self._bucket_ts:
            # empezamos bucket nuevo
            self._bucket_ts = bts
            self._grid.clear()

    def on_snapshot(self, snap: BookSnapshot) -> Optional[Dict[str, Any]]:
        bids = _norm_levels(getattr(snap, "bids", None))
        asks = _norm_levels(getattr(snap, "asks", None))
        if not bids and not asks:
            return None

        self._roll_bucket_if_needed(int(snap.ts))
        tick = self.cfg.tick_size

        # agregamos tamaños por bin
        for p, v in bids:
            b = _bin_price(p, tick)
            self._grid[(b, "bid")] = self._grid.get((b, "bid"), 0.0) + v
        for p, v in asks:
            b = _bin_price(p, tick)
            self._grid[(b, "ask")] = self._grid.get((b, "ask"), 0.0) + v

        # construimos filas ordenadas por precio, limitado
        items = sorted(self._grid.items(), key=lambda kv: kv[0][0])  # por precio
        rows: List[List[Any]] = [[price, side, sz] for (price, side), sz in items]
        if self.cfg.max_rows_per_bucket and len(rows) > self.cfg.max_rows_per_bucket:
            rows = rows[: self.cfg.max_rows_per_bucket]

        max_sz = max((r[2] for r in rows), default=0.0)
        return {
            "bucket_ts": self._bucket_ts,
            "bucket_ms": self.cfg.bucket_ms,
            "rows": rows,
            "max_sz": max_sz,
        }
