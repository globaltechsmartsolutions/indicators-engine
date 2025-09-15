# src/indicators_engine/pipelines/adx.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, TypedDict, cast, List


class AdxPoint(TypedDict):
    plus_di: float   # +DI
    minus_di: float  # -DI
    dx: float        # 100 * |+DI - -DI| / (+DI + -DI)
    adx: float       # ADX
    value: float     # alias de adx (para homogeneizar)


@dataclass
class _AdxState:
    prev_high: Optional[float] = None
    prev_low: Optional[float] = None
    prev_close: Optional[float] = None
    last_ts: Optional[int] = None

    # --- Wilder ---
    tr_sum: float = 0.0
    plusdm_sum: float = 0.0
    minusdm_sum: float = 0.0
    warmup_count: int = 0

    atr: Optional[float] = None
    plusdm_sm: Optional[float] = None
    minusdm_sm: Optional[float] = None

    dx_buf: List[float] | None = None
    adx: Optional[float] = None

    # --- EMA (modo alternativo) ---
    ema_tr: Optional[float] = None
    ema_plusdm: Optional[float] = None
    ema_minusdm: Optional[float] = None
    ema_adx: Optional[float] = None

    def __post_init__(self):
        if self.dx_buf is None:
            self.dx_buf = []


def _clip01(x: float) -> float:
    """Recorta al rango [0, 100] para evitar flotantes como 100.0000000002."""
    if x < 0.0:
        return 0.0
    if x > 100.0:
        return 100.0
    return x


class AdxCalc:
    """
    ADX incremental con dos métodos:
      - method="wilder" (por defecto): cálculo clásico de Wilder (RMA).
      - method="ema": suaviza TR, DM y DX con EMA (α=2/(n+1)).

    on_bar(symbol, tf, ts, high, low, close) -> AdxPoint | None
    Devuelve None durante el warm-up (más largo en 'wilder').
    """
    def __init__(self, period: int = 14, method: str = "wilder"):
        if period < 1:
            raise ValueError("ADX period must be >= 1")
        if method not in ("wilder", "ema"):
            raise ValueError("method must be 'wilder' or 'ema'")
        self.period = int(period)
        self.method = method
        self.alpha = 2.0 / (self.period + 1.0)  # para modo EMA
        self._state: Dict[str, _AdxState] = {}

    # -------------------- API --------------------
    def on_bar(
            self,
            symbol: str,
            tf: str,
            ts: int,
            high: float,
            low: float,
            close: float,
    ) -> Optional[AdxPoint]:
        key = f"{symbol}|{tf}"
        st = self._state.setdefault(key, _AdxState())

        # Ignora out-of-order
        if st.last_ts is not None and ts <= st.last_ts:
            return None
        st.last_ts = ts

        # Primera barra: sólo inicializa previos
        if st.prev_close is None:
            st.prev_high, st.prev_low, st.prev_close = high, low, close
            return None

        # Cálculo crudo de esta barra
        up_move = high - st.prev_high
        down_move = st.prev_low - low
        plus_dm = up_move if (up_move > 0.0 and up_move > down_move) else 0.0
        minus_dm = down_move if (down_move > 0.0 and down_move > up_move) else 0.0
        tr = max(
            high - low,
            abs(high - st.prev_close),
            abs(low - st.prev_close),
            )

        # Avanza previos
        st.prev_high, st.prev_low, st.prev_close = high, low, close

        if self.method == "wilder":
            return self._on_bar_wilder(st, ts, tr, plus_dm, minus_dm)
        else:
            return self._on_bar_ema(st, ts, tr, plus_dm, minus_dm)

    # -------------------- Wilder (RMA) --------------------
    def _on_bar_wilder(
            self, st: _AdxState, ts: int, tr: float, plus_dm: float, minus_dm: float
    ) -> Optional[AdxPoint]:
        n = self.period

        # Warm-up 1: acumular TR/DM durante 'n' barras
        if st.warmup_count < n:
            st.tr_sum += tr
            st.plusdm_sum += plus_dm
            st.minusdm_sum += minus_dm
            st.warmup_count += 1

            if st.warmup_count == n:
                # Inicializa ATR y DM suavizados (promedios base)
                st.atr = st.tr_sum / n
                st.plusdm_sm = st.plusdm_sum / n
                st.minusdm_sm = st.minusdm_sum / n

                # Primer DX (barra actual) y empezar a llenar dx_buf
                pdi, mdi = self._calc_di(st.atr, st.plusdm_sm, st.minusdm_sm)
                dx = self._calc_dx(pdi, mdi)
                st.dx_buf.append(dx)
                # Aún no hay ADX hasta tener n DX
                return None
            else:
                return None

        # Suavizado Wilder: prev - prev/n + x
        st.atr = st.atr - (st.atr / n) + tr
        st.plusdm_sm = st.plusdm_sm - (st.plusdm_sm / n) + plus_dm
        st.minusdm_sm = st.minusdm_sm - (st.minusdm_sm / n) + minus_dm

        pdi, mdi = self._calc_di(st.atr, st.plusdm_sm, st.minusdm_sm)
        dx = self._calc_dx(pdi, mdi)

        if st.adx is None:
            # Warm-up 2: media de los primeros n DX para ADX inicial
            st.dx_buf.append(dx)
            if len(st.dx_buf) == n:
                st.adx = sum(st.dx_buf) / n
                # clamp antes de devolver
                st.adx = _clip01(st.adx)
                return self._make_point(pdi, mdi, dx, st.adx)
            return None
        else:
            # ADX suavizado Wilder
            st.adx = ((st.adx * (n - 1)) + dx) / n
            st.adx = _clip01(st.adx)
            return self._make_point(pdi, mdi, dx, st.adx)

    # -------------------- EMA --------------------
    def _on_bar_ema(
            self, st: _AdxState, ts: int, tr: float, plus_dm: float, minus_dm: float
    ) -> Optional[AdxPoint]:
        a = self.alpha

        # Inicialización EMA
        if st.ema_tr is None or st.ema_plusdm is None or st.ema_minusdm is None:
            st.ema_tr = tr
            st.ema_plusdm = plus_dm
            st.ema_minusdm = minus_dm
            return None

        # EMA: prev + α*(x - prev)
        st.ema_tr = st.ema_tr + a * (tr - st.ema_tr)
        st.ema_plusdm = st.ema_plusdm + a * (plus_dm - st.ema_plusdm)
        st.ema_minusdm = st.ema_minusdm + a * (minus_dm - st.ema_minusdm)

        pdi, mdi = self._calc_di(st.ema_tr, st.ema_plusdm, st.ema_minusdm)
        dx = self._calc_dx(pdi, mdi)

        if st.ema_adx is None:
            st.ema_adx = dx
            return None  # empieza a emitir en la siguiente
        else:
            st.ema_adx = st.ema_adx + a * (dx - st.ema_adx)
            st.ema_adx = _clip01(st.ema_adx)
            return self._make_point(pdi, mdi, dx, st.ema_adx)

    # -------------------- Helpers --------------------
    @staticmethod
    def _calc_di(tr_sm: float, plusdm_sm: float, minusdm_sm: float) -> tuple[float, float]:
        if tr_sm <= 0.0:
            return 0.0, 0.0
        plus_di = 100.0 * (plusdm_sm / tr_sm)
        minus_di = 100.0 * (minusdm_sm / tr_sm)
        # clamp
        return _clip01(plus_di), _clip01(minus_di)

    @staticmethod
    def _calc_dx(plus_di: float, minus_di: float) -> float:
        denom = plus_di + minus_di
        if denom <= 0.0:
            return 0.0
        dx = 100.0 * abs(plus_di - minus_di) / denom
        return _clip01(dx)

    @staticmethod
    def _make_point(plus_di: float, minus_di: float, dx: float, adx: float) -> AdxPoint:
        # clamp defensivo por si llega algo marginal >100
        plus_di = _clip01(float(plus_di))
        minus_di = _clip01(float(minus_di))
        dx = _clip01(float(dx))
        adx = _clip01(float(adx))
        return cast(AdxPoint, {
            "plus_di": plus_di,
            "minus_di": minus_di,
            "dx": dx,
            "adx": adx,
            "value": adx,
        })
