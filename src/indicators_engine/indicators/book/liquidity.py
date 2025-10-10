from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Iterable, Tuple, Dict, Any, List

from ...core.base import StreamIndicator
from ...core.types import BookSnapshot
from ...core.utils import is_finite


@dataclass(slots=True)
class LiquidityConfig:
    """
    Métrica compacta de libro:
    - depth_levels: hasta cuántos niveles sumar en cada lado
    - top_k_imbalance: cuántos niveles para el “top imbalance” (por defecto 1 = mejor nivel)
    """
    depth_levels: int = 10
    top_k_imbalance: int = 1


def _norm_levels(side) -> List[Tuple[float, float]]:
    """
    Convierte niveles a [(price, size), ...]
    side puede venir como [{'p':..., 'v':...}, ...] o [(p,v), ...].
    Filtra valores no finitos.
    """
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


class Liquidity(StreamIndicator):
    def __init__(self, cfg: LiquidityConfig = LiquidityConfig()):
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        pass

    def on_snapshot(self, snap: BookSnapshot) -> Optional[Dict[str, Any]]:
        """
        Espera un BookSnapshot con .symbol, .ts, .bids, .asks
        - bids: lista descendente por precio
        - asks: lista ascendente por precio
        """
        bids = _norm_levels(getattr(snap, "bids", None))
        asks = _norm_levels(getattr(snap, "asks", None))

        b1 = bids[0] if bids else None
        a1 = asks[0] if asks else None
        if not b1 or not a1:
            return None

        mid = (b1[0] + a1[0]) / 2.0
        spread = a1[0] - b1[0]

        # depth hasta N niveles
        nb = min(self.cfg.depth_levels, len(bids))
        na = min(self.cfg.depth_levels, len(asks))
        bids_depth = sum(v for _, v in bids[:nb])
        asks_depth = sum(v for _, v in asks[:na])

        # imbalance de profundidad (b-a)/(b+a)
        denom = bids_depth + asks_depth
        depth_imbalance = ((bids_depth - asks_depth) / denom) if denom > 0 else 0.0

        # top imbalance en los primeros K niveles (por tamaño total)
        k = max(1, self.cfg.top_k_imbalance)
        tb = sum(v for _, v in bids[:k])
        ta = sum(v for _, v in asks[:k])
        tden = tb + ta
        top_imbalance = ((tb - ta) / tden) if tden > 0 else 0.0

        out = {
            "mid": mid,
            "spread": spread,
            "bids_depth": bids_depth,
            "asks_depth": asks_depth,
            "depth_imbalance": depth_imbalance,
            "top_imbalance": top_imbalance,
            "best_bid": b1[0],
            "best_ask": a1[0],
            "bid1_size": b1[1],
            "ask1_size": a1[1],
            "levels": f"{len(bids)}/{len(asks)}",
        }
        return out
