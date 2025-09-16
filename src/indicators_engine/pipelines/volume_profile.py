
from __future__ import annotations
from collections import defaultdict, deque
from typing import Optional, Dict, List

def _tf_to_ms(tf: str) -> int:
    if not tf:
        return 60_000
    s = tf.strip().lower()
    if s.endswith("ms"):
        return int(s[:-2])
    if s.endswith("s"):
        return int(s[:-1]) * 1_000
    if s.endswith("m"):
        return int(s[:-1]) * 60_000
    if s.endswith("h"):
        return int(s[:-1]) * 3_600_000
    if s.endswith("d"):
        return int(s[:-1]) * 86_400_000
    # default 1m
    return 60_000

class VolumeProfileCalc:
    """
    Volume Profile por bucket temporal (tf) a partir de trades.
    - Discretiza precio por `tick_size`.
    - Agrega volumen por nivel de precio dentro de cada bucket temporal (p. ej., 1m).
    - Mantiene varios buckets recientes (max_buckets) para consultas opcionales.
    - Snapshot por bucket:
        {
          "bucket_start": int (ms),
          "vtotal": float,
          "poc": float | None,           # opcional: precio con mayor volumen dentro del bucket
          "bins": [{"price": float, "volume": float}, ...]  # ordenado por precio asc
        }
    """

    def __init__(self, *, tick_size: float, tf: str = "1m", max_buckets: int = 5):
        if tick_size <= 0:
            raise ValueError("tick_size debe ser > 0")
        self.tick_size = float(tick_size)
        self.tf = tf
        self.bucket_ms = _tf_to_ms(tf)
        self.max_buckets = max(1, int(max_buckets))

        # Estado por clave
        # key -> {
        #   "buckets": dict[bucket_ts] -> {"vol_by_tick": defaultdict(float), "vtotal": float}
        #   "order": deque of bucket_ts (FIFO, tamaño <= max_buckets)
        # }
        self.state: Dict[str, Dict] = defaultdict(lambda: {
            "buckets": {},
            "order": deque(),
        })

    # ---- helpers ----
    def _key(self, symbol: str, tf: Optional[str]) -> str:
        return f"{symbol}|{tf or self.tf or '-'}"

    def _to_tick(self, price: float) -> int:
        return int(round(float(price) / self.tick_size))

    def _tick_to_price(self, tick: int) -> float:
        return float(tick) * self.tick_size

    def _bucket_start(self, ts_ms: int) -> int:
        return (int(ts_ms) // self.bucket_ms) * self.bucket_ms

    def _ensure_bucket(self, s: Dict, bts: int):
        if bts not in s["buckets"]:
            s["buckets"][bts] = {
                "vol_by_tick": defaultdict(float),
                "vtotal": 0.0,
            }
            if bts not in s["order"]:
                s["order"].append(bts)
            # Evict antiguos si nos pasamos de capacidad
            while len(s["order"]) > self.max_buckets:
                old = s["order"].popleft()
                s["buckets"].pop(old, None)

    def _snapshot_bucket(self, s: Dict, bts: int) -> Dict:
        bucket = s["buckets"].get(bts)
        if not bucket:
            return {"bucket_start": bts, "vtotal": 0.0, "poc": None, "bins": []}
        vol_by_tick = bucket["vol_by_tick"]
        bins = [{"price": self._tick_to_price(t), "volume": float(v)}
                for t, v in vol_by_tick.items() if v > 0.0]
        bins.sort(key=lambda x: x["price"])
        poc = None
        if vol_by_tick:
            max_vol = max(vol_by_tick.values())
            candidates = [t for t, v in vol_by_tick.items() if v == max_vol]
            poc = self._tick_to_price(max(candidates))  # en empate, precio más alto
        return {"bucket_start": bts, "vtotal": float(bucket["vtotal"]), "poc": poc, "bins": bins}

    # ---- API ----
    def on_trade(self, *, symbol: str, ts: int, price: float, size: float, tf: Optional[str] = None) -> Dict:
        if size is None or price is None or ts is None or float(size) <= 0.0:
            # snapshot del bucket actual si existe
            key = self._key(symbol, tf)
            s = self.state[key]
            bts = self._bucket_start(ts) if ts is not None else None
            if bts is None or bts not in s["buckets"]:
                return {"bucket_start": bts or 0, "vtotal": 0.0, "poc": None, "bins": []}
            return self._snapshot_bucket(s, bts)

        key = self._key(symbol, tf)
        s = self.state[key]
        bts = self._bucket_start(ts)
        self._ensure_bucket(s, bts)

        tick = self._to_tick(price)
        bucket = s["buckets"][bts]
        bucket["vol_by_tick"][tick] += float(size)
        bucket["vtotal"] += float(size)

        return self._snapshot_bucket(s, bts)

    def get_bucket(self, *, symbol: str, bucket_ts: int, tf: Optional[str] = None) -> Dict:
        key = self._key(symbol, tf)
        s = self.state[key]
        return self._snapshot_bucket(s, int(bucket_ts))
