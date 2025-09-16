
from __future__ import annotations
from collections import defaultdict
from typing import Optional, Dict

class OrderFlowCalc:
    """
    Order Flow agresivo (BuyVol - SellVol) incremental por clave (symbol|tf).

    - Usa BBO (best bid/ask) para inferir agresor cuando no viene en el trade.
    - Reglas de agresor:
        * Si price >= ask -> BUY (agresivo contra la oferta)
        * Si price <= bid -> SELL (agresivo contra la demanda)
        * Si bid y ask existen y price está entre ambos:
              - Lado más cercano (distancia a ask -> BUY, distancia a bid -> SELL)
              - Si empate exacto, BUY (convención estable)
        * Si no hay BBO, se respeta `side`/`aggressor` si viene en el trade; si no, se ignora el trade.
    - Reset por día (UTC) opcional.
    - reset_on_session_id opcional.
    - on_bbo(...) actualiza el libro topo.
    - on_trade(...) acumula y retorna snapshot:
        {
          "delta": float,    # buy - sell acumulado
          "buy": float,      # volumen comprador acumulado
          "sell": float,     # volumen vendedor acumulado
          "bid": float|None,
          "ask": float|None
        }
    """

    def __init__(self, *, reset_daily: bool = True, reset_on_session_id: bool = False):
        self.reset_daily = reset_daily
        self.reset_on_session_id = reset_on_session_id
        self.state: Dict[str, Dict] = defaultdict(lambda: {
            "last_ts": None,
            "day": None,
            "session_id": None,
            "bid": None,
            "ask": None,
            "buy": 0.0,
            "sell": 0.0,
            "delta": 0.0,
        })

    def _key(self, symbol: str, tf: Optional[str]) -> str:
        return f"{symbol}|{tf or '-'}"

    @staticmethod
    def _day_from_ts(ts_ms: int) -> int:
        return int(ts_ms // 86_400_000)

    def reset(self, symbol: str, tf: Optional[str] = None):
        self.state.pop(self._key(symbol, tf), None)

    def get_snapshot(self, symbol: str, tf: Optional[str] = None) -> Dict:
        s = self.state[self._key(symbol, tf)]
        return {
            "delta": float(s["delta"]),
            "buy": float(s["buy"]),
            "sell": float(s["sell"]),
            "bid": None if s["bid"] is None else float(s["bid"]),
            "ask": None if s["ask"] is None else float(s["ask"]),
        }

    def on_bbo(self, *, symbol: str, ts: int, bid: Optional[float], ask: Optional[float], tf: Optional[str] = None, session_id: Optional[str] = None) -> Dict:
        key = self._key(symbol, tf)
        s = self.state[key]

        # Reset por día
        if self.reset_daily and ts is not None:
            day = self._day_from_ts(int(ts))
            if s["day"] is not None and day != s["day"]:
                s.update({
                    "last_ts": None, "day": day, "session_id": None,
                    "bid": None, "ask": None,
                    "buy": 0.0, "sell": 0.0, "delta": 0.0,
                })
            else:
                s["day"] = day

        # Reset por session
        if self.reset_on_session_id and session_id is not None:
            if s["session_id"] is not None and session_id != s["session_id"]:
                s.update({
                    "last_ts": None, "day": s.get("day"), "session_id": session_id,
                    "bid": None, "ask": None,
                    "buy": 0.0, "sell": 0.0, "delta": 0.0,
                })
            else:
                s["session_id"] = session_id

        # Orden temporal: ignorar retrocesos
        if s["last_ts"] is not None and ts < s["last_ts"]:
            return self.get_snapshot(symbol, tf)
        s["last_ts"] = ts

        s["bid"] = None if bid is None else float(bid)
        s["ask"] = None if ask is None else float(ask)

        return self.get_snapshot(symbol, tf)

    def on_trade(self, *, symbol: str, ts: int, price: float, size: float, tf: Optional[str] = None,
                 aggressor: Optional[str] = None, is_buyer_initiator: Optional[bool] = None,
                 session_id: Optional[str] = None) -> Dict:
        if size is None or price is None or float(size) <= 0.0:
            return self.get_snapshot(symbol, tf)

        key = self._key(symbol, tf)
        s = self.state[key]

        # Reset por día
        if self.reset_daily and ts is not None:
            day = self._day_from_ts(int(ts))
            if s["day"] is not None and day != s["day"]:
                s.update({
                    "last_ts": None, "day": day, "session_id": None,
                    "bid": None, "ask": None,
                    "buy": 0.0, "sell": 0.0, "delta": 0.0,
                })
            else:
                s["day"] = day

        # Reset por session
        if self.reset_on_session_id and session_id is not None:
            if s["session_id"] is not None and session_id != s["session_id"]:
                s.update({
                    "last_ts": None, "day": s.get("day"), "session_id": session_id,
                    "bid": None, "ask": None,
                    "buy": 0.0, "sell": 0.0, "delta": 0.0,
                })
            else:
                s["session_id"] = session_id

        # Orden temporal: ignorar retrocesos
        if s["last_ts"] is not None and ts < s["last_ts"]:
            return self.get_snapshot(symbol, tf)
        s["last_ts"] = ts

        price = float(price)
        size = float(size)

        # Determinar agresor
        side = None
        if aggressor:
            a = str(aggressor).upper()
            if a in ("B", "BUY", "BUYER", "BUYER_INITIATOR"):
                side = "B"
            elif a in ("S", "SELL", "SELLER", "SELLER_INITIATOR"):
                side = "S"
        if side is None and is_buyer_initiator is not None:
            side = "B" if is_buyer_initiator else "S"
        if side is None:
            bid = s.get("bid")
            ask = s.get("ask")
            if ask is not None and price >= ask - 0.0:
                side = "B"
            elif bid is not None and price <= bid + 0.0:
                side = "S"
            elif bid is not None and ask is not None:
                # precio entre bid-ask: lado más cercano; empate -> BUY
                if abs(price - ask) < abs(price - bid):
                    side = "B"
                elif abs(price - ask) > abs(price - bid):
                    side = "S"
                else:
                    side = "B"
            else:
                # no hay información suficiente; ignoramos el trade
                return self.get_snapshot(symbol, tf)

        if side == "B":
            s["buy"] += size
            s["delta"] += size
        else:
            s["sell"] += size
            s["delta"] -= size

        return self.get_snapshot(symbol, tf)
