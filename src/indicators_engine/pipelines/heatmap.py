from __future__ import annotations
from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
import math

Price = float
Size = float

def _to_ms(ts: int | float | None) -> int:
    if ts is None:
        return 0
    ts = int(ts)
    return ts // 1_000_000 if ts > 10**15 else ts  # dxFeed nanos → ms

def _round_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return float(price)
    return round(round(price / tick) * tick, 10)

def _levels_from_any(x: Any) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
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

class HeatmapState:
    """
    Construye un Heatmap de liquidez a partir de OrderBook:
      - Mantiene el libro (bids/asks) actual.
      - Por cada bucket temporal (p.ej. 1000 ms) registra el **máximo size** visto por nivel.
      - Emite 'frames' (sparse): filas [ts_bucket, price, size].

    Estrategia:
      - apply_snapshot(d): resetea libro a d['bids']/d['asks'].
      - apply_update(d): upsert/delete un nivel (size=0 => delete).
      - commit(ts): vuelca el frame del bucket al buffer de salida.

    Salida snapshot():
    {
      "v":1, "source":"indicators-engine", "indicator":"heatmap",
      "symbol":..., "tf":"-", "ts": ts_bucket,
      "tick_size": <float>, "bucket_ms": <int>,
      "rows": [[ts_bucket, price, size], ...]  # sparse
    }
    """

    def __init__(self, symbol: str, *, tick_size: float = 0.25, bucket_ms: int = 1000, max_prices: int | None = None):
        self.symbol = symbol
        self.tick = float(tick_size)
        self.bucket_ms = int(bucket_ms)
        self.max_prices = max_prices  # opcional: limitar nº de niveles por lado en cada frame (top-N por size)

        self.ts_bucket: int = 0
        self._bids: Dict[Price, Size] = {}
        self._asks: Dict[Price, Size] = {}

        # acumulador por bucket: price -> max_size
        self._acc: DefaultDict[Price, Size] = defaultdict(float)

    def _bucket_of(self, ts_ms: int) -> int:
        if self.bucket_ms <= 0:
            return ts_ms
        return (ts_ms // self.bucket_ms) * self.bucket_ms

    def _touch_bucket(self, ts_ms: int) -> None:
        b = self._bucket_of(ts_ms)
        if self.ts_bucket == 0:
            self.ts_bucket = b
        elif b != self.ts_bucket:
            # nuevo bucket: limpiamos acumulador
            self._acc.clear()
            self.ts_bucket = b

    def _accumulate_current_book(self) -> None:
        # Registrar máximo size por nivel (bids y asks)
        for p, s in self._bids.items():
            if s > self._acc[p]:
                self._acc[p] = s
        for p, s in self._asks.items():
            if s > self._acc[p]:
                self._acc[p] = s

        # Si se quiere limitar nº de precios por lado (top-N por size)
        if self.max_prices and self.max_prices > 0:
            # separar por lado alrededor del mid aproximado
            if self._bids and self._asks:
                best_bid = max(self._bids) if self._bids else None
                best_ask = min(self._asks) if self._asks else None
                mid = (best_bid + best_ask) / 2.0 if (best_bid is not None and best_ask is not None) else None
            else:
                mid = None

            if mid is not None:
                bids_items = [(p, s) for p, s in self._acc.items() if p <= mid]
                asks_items = [(p, s) for p, s in self._acc.items() if p > mid]
                bids_items.sort(key=lambda x: x[1], reverse=True)
                asks_items.sort(key=lambda x: x[1], reverse=True)
                keep = {p for p, _ in bids_items[:self.max_prices]} | {p for p, _ in asks_items[:self.max_prices]}
                # recorta el acumulador
                for p in list(self._acc.keys()):
                    if p not in keep:
                        del self._acc[p]

    def apply_snapshot(self, d: Dict[str, Any]) -> None:
        sym = d.get("symbol") or d.get("eventSymbol") or self.symbol
        ts = _to_ms(d.get("ts") or d.get("time"))
        if not sym or ts <= 0:
            return
        self.symbol = sym
        self._bids.clear(); self._asks.clear()
        for p, s in _levels_from_any(d.get("bids") or d.get("bidLevels")):
            self._bids[_round_to_tick(p, self.tick)] = s
        for p, s in _levels_from_any(d.get("asks") or d.get("askLevels")):
            self._asks[_round_to_tick(p, self.tick)] = s
        self._touch_bucket(ts)
        self._accumulate_current_book()
        print(f"[HEATMAP] SNAPSHOT {self.symbol} ts={ts} bucket={self.ts_bucket} depth(b/a)=({len(self._bids)}/{len(self._asks)})")

    def apply_update(self, u: Dict[str, Any]) -> None:
        sym = u.get("symbol") or u.get("eventSymbol") or self.symbol
        ts = _to_ms(u.get("ts") or u.get("time"))
        side = (u.get("side") or u.get("action") or "").lower()  # 'bid' / 'ask'
        p = u.get("price")
        s = u.get("size", u.get("quantity"))
        if not sym or ts <= 0 or p is None or s is None or side not in ("bid", "ask"):
            return
        self.symbol = sym
        self._touch_bucket(ts)
        p = _round_to_tick(float(p), self.tick)
        s = float(s)
        book = self._bids if side == "bid" else self._asks
        if s <= 0:
            if p in book:
                del book[p]
        else:
            book[p] = s
        self._accumulate_current_book()
        print(f"[HEATMAP] UPDATE {side}@{p}={s} bucket={self.ts_bucket}")

    def frame(self) -> dict:
        """
        Devuelve un frame del bucket actual (sparse).
        """
        rows = [[self.ts_bucket, float(p), float(s)] for p, s in sorted(self._acc.items())]
        out = {
            "v": 1,
            "source": "indicators-engine",
            "symbol": self.symbol,
            "tf": "-",
            "ts": self.ts_bucket,
            "indicator": "heatmap",
            "tick_size": self.tick,
            "bucket_ms": self.bucket_ms,
            "rows": rows,
            "id": f"{self.symbol}|-|{self.ts_bucket}|heatmap",
        }
        print(f"[HEATMAP] FRAME rows={len(rows)} ts_bucket={self.ts_bucket}")
        return out
