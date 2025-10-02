from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple


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


def normalize_dxfeed_book_order(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Corregido: aceptar tanto "symbol" como "eventSymbol"
    symbol = d.get("symbol") or d.get("eventSymbol")
    if not symbol:
        return None
    bids = _levels_from_any(d.get("bids"))
    asks = _levels_from_any(d.get("asks"))
    return {
        "symbol": symbol,
        "ts": d.get("eventTime", d.get("ts")),
        "bids": sorted(bids, key=lambda x: -x[0]),
        "asks": sorted(asks, key=lambda x: x[0]),
    }


def normalize_dxfeed_book_update(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Corregido: aceptar tanto "symbol" como "eventSymbol"
    symbol = d.get("symbol") or d.get("eventSymbol")
    if not symbol:
        return None
    return {
        "symbol": symbol,
        "ts": d.get("eventTime", d.get("ts")),
        "bids": _levels_from_any(d.get("bids")),
        "asks": _levels_from_any(d.get("asks")),
    }
