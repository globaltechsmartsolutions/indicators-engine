from __future__ import annotations
from typing import Any, Dict, List, Tuple

def _to_ms(ts: int | float | None) -> int | None:
    if ts is None:
        return None
    ts = int(ts)
    # dxFeed suele entregar nanos
    return ts // 1_000_000 if ts > 10**15 else ts

def _levels_from_any(x: Any) -> List[Tuple[float, float]]:
    """
    Acepta:
      - [{"price":..., "size":...}]
      - [{"price":..., "quantity":...}]
      - [[price, size], ...]
    Devuelve [(price, size), ...] con floats, sin niveles de size<=0.
    """
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
            p = float(p)
            s = float(s)
        except Exception:
            continue
        if s > 0:
            out.append((p, s))
    return out

def normalize_dxfeed_book_order(d: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Normaliza un OrderBookSnapshot de dxFeed a un mensaje 'book_order' con **todos los niveles L2**.
    - No recorta profundidad.
    - Orden: bids desc, asks asc.
    Salida:
    {
      "v": 1, "source": "indicators-engine",
      "symbol": str, "tf": "-", "ts": ms, "indicator": "book_order",
      "bids": [[p, s], ...], "asks": [[p, s], ...],
      "id": f"{symbol}|-|{ts}|book_order"
    }
    """
    if not isinstance(d, dict):
        return None

    symbol = d.get("symbol") or d.get("eventSymbol")
    ts = _to_ms(d.get("ts") or d.get("time"))
    if not symbol or ts is None:
        return None

    bids_raw = d.get("bids") or d.get("bidLevels")
    asks_raw = d.get("asks") or d.get("askLevels")

    bids = _levels_from_any(bids_raw)
    asks = _levels_from_any(asks_raw)

    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    out = {
        "v": 1,
        "source": "indicators-engine",
        "symbol": symbol,
        "tf": "-",
        "ts": ts,
        "indicator": "book_order",
        "bids": [[p, s] for p, s in bids],
        "asks": [[p, s] for p, s in asks],
        "id": f"{symbol}|-|{ts}|book_order",
    }
    print(f"[BOOK_ORDER] snapshot normalizado: symbol={symbol} ts={ts} depth(b/a)=({len(bids)}/{len(asks)})")
    return out
