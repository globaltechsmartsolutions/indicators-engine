from __future__ import annotations
from typing import Any, Dict, List, Tuple

Price = float
Size = float

def _to_ms(ts: int | float | None) -> int:
    if ts is None:
        return 0
    ts = int(ts)
    # dxFeed suele venir en nanos
    return ts // 1_000_000 if ts > 10**15 else ts

def _levels_from_any(x: Any) -> List[Tuple[Price, Size]]:
    """
    Acepta:
      - [{"price":..., "size":...}]
      - [{"price":..., "quantity":...}]
      - [[price, size], ...]
    Devuelve [(price, size), ...] con size>0.
    """
    out: List[Tuple[Price, Size]] = []
    if not x:
        return out
    for lvl in x:
        if isinstance(lvl, dict):
            p = lvl.get("price")
            s = lvl.get("size", lvl.get("quantity"))
        elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
            p, s = lvl[0], lvl[1]
        else:
            continue
        try:
            p = float(p); s = float(s)
        except Exception:
            continue
        if s > 0:
            out.append((p, s))
    return out

class LiquidityState:
    """
    Mantiene libro (bids/asks) y calcula:
      - Depth por lado (suma de sizes top-N niveles)
      - Imbalance de depth: (bids_depth - asks_depth)/(bids_depth + asks_depth)
      - Top-of-book: best_bid/ask y su imbalance de nivel-1
    """
    def __init__(self, symbol: str, depth_levels: int = 10):
        self.symbol = symbol
        self.depth_levels = int(depth_levels)
        self.ts_ms: int = 0
        self._bids: Dict[Price, Size] = {}
        self._asks: Dict[Price, Size] = {}

    def _recalc(self) -> Dict[str, Any]:
        bids_sorted = sorted(self._bids.items(), key=lambda x: x[0], reverse=True)
        asks_sorted = sorted(self._asks.items(), key=lambda x: x[0])

        top_bids = bids_sorted[: self.depth_levels]
        top_asks = asks_sorted[: self.depth_levels]

        bids_depth = float(sum(s for _, s in top_bids))
        asks_depth = float(sum(s for _, s in top_asks))
        denom = bids_depth + asks_depth
        depth_imb = (bids_depth - asks_depth) / denom if denom > 0 else 0.0

        best_bid = float(top_bids[0][0]) if top_bids else None
        best_ask = float(top_asks[0][0]) if top_asks else None
        bid1_sz  = float(top_bids[0][s_idx]) if (top_bids and (s_idx:=1) is not None) else None  # noqa
        ask1_sz  = float(top_asks[0][1]) if top_asks else None
        denom1   = (bid1_sz or 0.0) + (ask1_sz or 0.0)
        top_imb  = ((bid1_sz or 0.0) - (ask1_sz or 0.0)) / denom1 if denom1 > 0 else 0.0

        return {
            "bids_depth": bids_depth,
            "asks_depth": asks_depth,
            "depth_imbalance": depth_imb,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid1_size": bid1_sz,
            "ask1_size": ask1_sz,
            "top_imbalance": top_imb,
        }

    def apply_snapshot(self, d: Dict[str, Any]) -> None:
        self.symbol = d.get("symbol") or d.get("eventSymbol") or self.symbol
        self.ts_ms = _to_ms(d.get("ts") or d.get("time"))
        self._bids.clear(); self._asks.clear()
        for p, s in _levels_from_any(d.get("bids") or d.get("bidLevels")):
            self._bids[p] = s
        for p, s in _levels_from_any(d.get("asks") or d.get("askLevels")):
            self._asks[p] = s
        print(f"[LIQ] SNAPSHOT {self.symbol} ts={self.ts_ms} depth(b/a)=({len(self._bids)}/{len(self._asks)})")

    def apply_update(self, u: Dict[str, Any]) -> None:
        side = (u.get("side") or u.get("action") or "").lower()  # 'bid'/'ask'
        p = u.get("price")
        s = u.get("size", u.get("quantity"))
        ts = u.get("ts") or u.get("time")
        if ts is not None:
            self.ts_ms = _to_ms(ts)
        if side not in ("bid", "ask") or p is None or s is None:
            return
        p = float(p); s = float(s)
        book = self._bids if side == "bid" else self._asks
        if s <= 0:
            if p in book:
                del book[p]
        else:
            book[p] = s
        print(f"[LIQ] UPDATE {side}@{p}={s} ts={self.ts_ms}")

    def snapshot(self) -> Dict[str, Any]:
        metrics = self._recalc()
        out = {
            "v": 1,
            "source": "indicators-engine",
            "symbol": self.symbol,
            "tf": "-",
            "ts": self.ts_ms,
            "indicator": "liquidity",
            "depth_levels": self.depth_levels,
            **metrics,
            "id": f"{self.symbol}|-|{self.ts_ms}|liquidity",
        }
        print(f"[LIQ] SNAPSHOT OUT: {out}")
        return out
