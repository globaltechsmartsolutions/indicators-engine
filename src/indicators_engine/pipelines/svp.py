
from __future__ import annotations
from collections import defaultdict
from typing import Optional, Dict, List, Tuple

class SvpCalc:
    """
    SVP (Session Volume Profile) incremental por clave (symbol|tf).

    - Acumula volumen por nivel de precio discretizado con `tick_size`.
    - Reset por día (UTC) opcional: reset_daily=True por defecto
    - Reset por session_id opcional (si quieres anclar por RTH/ETH, etc.)
    - Ignora trades con size <= 0.
    - Retornos:
        * on_trade(...) -> dict con snapshot:
            {
              "poc": float | None,
              "vtotal": float,
              "bins": List[{"price": float, "volume": float}]  # ordenado por precio asc
            }
        * get_profile(...) -> mismo dict que arriba (sin mutar estado)
    """

    def __init__(self, *, tick_size: float, reset_daily: bool = True, reset_on_session_id: bool = False):
        if tick_size <= 0:
            raise ValueError("tick_size debe ser > 0")
        self.tick_size = float(tick_size)
        self.reset_daily = reset_daily
        self.reset_on_session_id = reset_on_session_id

        # Estado por clave
        self.state: Dict[str, Dict] = defaultdict(lambda: {
            "last_ts": None,
            "day": None,
            "session_id": None,
            "vol_by_tick": defaultdict(float),  # tick(int) -> vol(float)
            "last_price_tick": None,
            "poc_tick": None,
            "vtotal": 0.0,
            "last_update_tick": None,
        })

    # -------- helpers --------
    def _key(self, symbol: str, tf: Optional[str]) -> str:
        return f"{symbol}|{tf or '-'}"

    @staticmethod
    def _day_from_ts(ts_ms: int) -> int:
        return int(ts_ms // 86_400_000)

    def _to_tick(self, price: float) -> int:
        return int(round(float(price) / self.tick_size))

    def _tick_to_price(self, tick: int) -> float:
        return float(tick) * self.tick_size

    def _choose_poc_tick(self, s: Dict) -> Optional[int]:
        if not s["vol_by_tick"]:
            return None
        max_vol = max(s["vol_by_tick"].values())
        candidates = [t for t, v in s["vol_by_tick"].items() if v == max_vol]
        if len(candidates) == 1:
            return candidates[0]
        # tie-break 1: el nivel ACTUALIZADO más recientemente (developing POC)
        # guardamos last_update_tick en state; si falta, cae al segundo criterio
        last_upd = s.get("last_update_tick")
        if last_upd in candidates:
            return last_upd
        # tie-break 2: el precio más alto entre los candidatos
        return max(candidates)

    def _snapshot(self, s: Dict) -> Dict:
        bins = [{"price": self._tick_to_price(t), "volume": float(v)}
                for t, v in s["vol_by_tick"].items() if v > 0.0]
        bins.sort(key=lambda x: x["price"])  # por precio asc
        poc = self._tick_to_price(s["poc_tick"]) if s["poc_tick"] is not None else None
        return {"poc": poc, "vtotal": float(s["vtotal"]), "bins": bins}

    # -------- API pública --------
    def reset(self, symbol: str, tf: Optional[str] = None):
        self.state.pop(self._key(symbol, tf), None)

    def get_profile(self, symbol: str, tf: Optional[str] = None) -> Dict:
        s = self.state[self._key(symbol, tf)]
        return self._snapshot(s)

    def get_volume_at_price(self, symbol: str, price: float, tf: Optional[str] = None) -> float:
        s = self.state[self._key(symbol, tf)]
        tick = self._to_tick(price)
        return float(s["vol_by_tick"].get(tick, 0.0))

    def on_trade(
            self,
            *,
            symbol: str,
            ts: int,
            price: float,
            size: float,
            tf: Optional[str] = None,
            session_id: Optional[str] = None,
    ) -> Dict:
        if size is None or price is None or float(size) <= 0.0:
            return self.get_profile(symbol, tf)

        key = self._key(symbol, tf)
        s = self.state[key]

        # Reset por día
        if self.reset_daily and ts is not None:
            day = self._day_from_ts(int(ts))
            if s["day"] is not None and day != s["day"]:
                s.update({
                    "last_ts": None, "day": day, "session_id": None,
                    "vol_by_tick": defaultdict(float),
                    "last_price_tick": None,
                    "poc_tick": None,
                    "vtotal": 0.0,
                    "last_update_tick": None,
                })
            else:
                s["day"] = day

        # Reset por session_id
        if self.reset_on_session_id and session_id is not None:
            if s["session_id"] is not None and session_id != s["session_id"]:
                s.update({
                    "last_ts": None, "day": s.get("day"), "session_id": session_id,
                    "vol_by_tick": defaultdict(float),
                    "last_price_tick": None,
                    "poc_tick": None,
                    "vtotal": 0.0,
                    "last_update_tick": None,
                })
            else:
                s["session_id"] = session_id

        # Orden temporal estricta (ignorar retrocesos)
        if s["last_ts"] is not None and ts < s["last_ts"]:
            return self._snapshot(s)
        s["last_ts"] = ts

        # Acumular
        tick = self._to_tick(price)
        s["vol_by_tick"][tick] += float(size)
        s["vtotal"] += float(size)
        s["last_price_tick"] = tick
        s["last_update_tick"] = tick

        # Recalcular POC
        s["poc_tick"] = self._choose_poc_tick(s)

        return self._snapshot(s)
