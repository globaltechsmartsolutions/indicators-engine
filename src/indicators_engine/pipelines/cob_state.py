from __future__ import annotations
from typing import Dict, List, Tuple, Any

Price = float
Size = float


class BookState:
    """
    Mantiene el estado del libro (bids/asks) aplicando:
      - Snapshot completo
      - Updates incrementales por nivel (precio, size absoluto)
    Compatibles con dxFeed:
      - snapshot: {"eventSymbol"/"symbol", "time"/"ts", "bids": [[p,s]...] ...}
      - update:   {"eventSymbol"/"symbol", "time"/"ts", "side": "bid"/"ask", "price": p, "size": s}
                  (size==0 => eliminar nivel)
    """

    def __init__(self, symbol: str, max_depth: int | None = 10):
        self.symbol = symbol
        self.max_depth = max_depth
        self.ts_ms: int = 0
        self._bids: Dict[Price, Size] = {}
        self._asks: Dict[Price, Size] = {}

    @staticmethod
    def _to_ms(ts: int | float | None) -> int:
        if ts is None:
            return 0
        ts = int(ts)
        return ts // 1_000_000 if ts > 10**15 else ts  # dxFeed nanos→ms

    @staticmethod
    def _norm_levels(arr: Any) -> List[Tuple[Price, Size]]:
        out: List[Tuple[Price, Size]] = []
        if not arr:
            return out
        for lvl in arr:
            if isinstance(lvl, dict):
                p = lvl.get("price")
                s = lvl.get("size", lvl.get("quantity"))
            elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                p, s = lvl[0], lvl[1]
            else:
                continue
            try:
                p = float(p)
                s = float(s)
            except Exception:
                continue
            if s >= 0:
                out.append((p, s))
        return out

    def apply_snapshot(self, d: Dict[str, Any]) -> None:
        self.symbol = d.get("symbol") or d.get("eventSymbol") or self.symbol
        self.ts_ms = self._to_ms(d.get("ts") or d.get("time") or self.ts_ms)

        self._bids.clear()
        self._asks.clear()

        print(f"[COB] SNAPSHOT para {self.symbol} @ {self.ts_ms}ms")

        for p, s in self._norm_levels(d.get("bids") or d.get("bidLevels")):
            if s > 0:
                self._bids[p] = s
        for p, s in self._norm_levels(d.get("asks") or d.get("askLevels")):
            if s > 0:
                self._asks[p] = s

        print(f"[COB] -> {len(self._bids)} bids / {len(self._asks)} asks")

    def apply_update(self, u: Dict[str, Any]) -> None:
        side = (u.get("side") or u.get("action") or "").lower()
        p = u.get("price")
        s = u.get("size", u.get("quantity"))
        ts = u.get("ts") or u.get("time")
        if ts is not None:
            self.ts_ms = self._to_ms(ts)

        print(f"[COB] UPDATE {side.upper()} price={p} size={s} ts={self.ts_ms}")

        if p is None or s is None or side not in ("bid", "ask"):
            return

        try:
            p = float(p)
            s = float(s)
        except Exception:
            return

        book = self._bids if side == "bid" else self._asks
        if s <= 0:
            if p in book:
                del book[p]
                print(f"[COB]   eliminado nivel {side}@{p}")
        else:
            book[p] = s
            print(f"[COB]   actualizado {side}@{p}={s}")

    def snapshot(self) -> dict:
        bids = sorted(self._bids.items(), key=lambda x: x[0], reverse=True)
        asks = sorted(self._asks.items(), key=lambda x: x[0])
        if self.max_depth and self.max_depth > 0:
            bids = bids[: self.max_depth]
            asks = asks[: self.max_depth]
        snap = {
            "v": 1,
            "source": "indicators-engine",
            "symbol": self.symbol,
            "tf": "-",
            "ts": self.ts_ms,
            "indicator": "cob",
            "bids": [[float(p), float(s)] for p, s in bids],
            "asks": [[float(p), float(s)] for p, s in asks],
            "id": f"{self.symbol}|-|{self.ts_ms}|cob",
        }
        print(f"[COB] SNAPSHOT generado: {snap}")
        return snap
