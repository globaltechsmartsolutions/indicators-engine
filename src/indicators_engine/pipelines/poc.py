from __future__ import annotations
from collections import defaultdict
from typing import Optional, Dict, Tuple

class PocCalc:
    """
    POC (Point of Control) incremental por clave (symbol|tf).

    Acumula volumen por nivel de precio discretizado con `tick_size`.
    Devuelve el precio (nivel) con MAYOR volumen de la sesión/ventana.

    - Bucketing por ticks: tick = round(price / tick_size) -> int
      Guardamos volumen por tick y convertimos a precio: price = tick * tick_size
    - Reset por día (UTC) opcional: reset_daily=True por defecto
    - Reset por session_id opcional (si quieres anclar por RTH/ETH, etc.)
    - Empates (tie-break):
        1) Preferir el nivel más cercano al `last_price_tick`
        2) Si sigue empate, elegir el nivel más ALTO (convención común)
    - Ignora trades con size <= 0.

    Retorno:
      - on_trade(...) -> float | None  (precio POC o None si aún no hay volumen)
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
        })

    # -------- helpers --------
    def _key(self, symbol: str, tf: Optional[str]) -> str:
        return f"{symbol}|{tf or '-'}"

    @staticmethod
    def _day_from_ts(ts_ms: int) -> int:
        return int(ts_ms // 86_400_000)

    def _to_tick(self, price: float) -> int:
        # usamos round para absorber micro errores de float
        return int(round(float(price) / self.tick_size))

    def _tick_to_price(self, tick: int) -> float:
        return float(tick) * self.tick_size

    def _choose_poc_tick(self, s: Dict) -> Optional[int]:
        if not s["vol_by_tick"]:
            return None
        # máximo volumen
        max_vol = max(s["vol_by_tick"].values())
        candidates = [t for t, v in s["vol_by_tick"].items() if v == max_vol]
        if len(candidates) == 1:
            return candidates[0]

        # tie-break 1: más cercano al último precio
        lp = s.get("last_price_tick")
        if lp is not None:
            best = None
            best_dist = None
            for t in candidates:
                dist = abs(t - lp)
                if best is None or dist < best_dist:
                    best, best_dist = t, dist
            # puede seguir habiendo empate de distancia; filtramos y pasamos a regla 2
            closest = [t for t in candidates if abs(t - lp) == best_dist]
        else:
            closest = candidates

        # tie-break 2: el más alto
        return max(closest)

    # -------- API pública --------
    def reset(self, symbol: str, tf: Optional[str] = None):
        self.state.pop(self._key(symbol, tf), None)

    def get_value(self, symbol: str, tf: Optional[str] = None) -> Optional[float]:
        s = self.state[self._key(symbol, tf)]
        return self._tick_to_price(s["poc_tick"]) if s["poc_tick"] is not None else None

    def get_volume_at_poc(self, symbol: str, tf: Optional[str] = None) -> Optional[float]:
        s = self.state[self._key(symbol, tf)]
        pt = s["poc_tick"]
        if pt is None:
            return None
        return float(s["vol_by_tick"].get(pt, 0.0))

    def on_trade(
            self,
            *,
            symbol: str,
            ts: int,
            price: float,
            size: float,
            tf: Optional[str] = None,
            session_id: Optional[str] = None,
    ) -> Optional[float]:
        if size is None or price is None or float(size) <= 0.0:
            return self.get_value(symbol, tf)

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
                })
            else:
                s["session_id"] = session_id

        if s["last_ts"] is not None and ts < s["last_ts"]:
            return self.get_value(symbol, tf)
        s["last_ts"] = ts

        # Acumular
        tick = self._to_tick(price)
        s["vol_by_tick"][tick] += float(size)
        s["last_price_tick"] = tick

        # Elegir POC actualizado
        s["poc_tick"] = self._choose_poc_tick(s)
        return self._tick_to_price(s["poc_tick"]) if s["poc_tick"] is not None else None
