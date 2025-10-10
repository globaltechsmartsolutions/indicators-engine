
import asyncio
import os
import time
from typing import Any, Dict, Tuple

def _now_ms() -> int:
    return int(time.time() * 1000)

def _parse_min_change(s: str | None):
    if not s:
        return ("abs", 0.0)
    s = str(s).strip().lower()
    if s.startswith("rel:"):
        try:
            return ("rel", float(s.split(":", 1)[1]))
        except Exception:
            return ("rel", 0.01)
    try:
        return ("abs", float(s))
    except Exception:
        return ("abs", 0.0)

class LiveRenderer:
    """
    Muestra 1 línea por (symbol, tf, indicador) y refresca la consola.
    Guarda el último valor de cada (sym, tf, ind) y pinta todo en cada tick de refresco.
    - dbg:* indicadores: rate limit configurable + filtro de cambios pequeños
    """
    def __init__(self, fps: float = 4.0):
        self.latest: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._interval = max(0.05, 1.0 / fps)

        # Silenciar ruido de dbg:*
        self._dbg_every_ms = int(os.getenv("INDICATORS_DEBUG_EVERY_MS", "700"))
        self._dbg_min_change_mode, self._dbg_min_change_val = _parse_min_change(os.getenv("INDICATORS_DEBUG_MIN_CHANGE"))

        # Estado para limitar dbg
        self._dbg_last_emit_ms: Dict[Tuple[str, str, str], int] = {}
        self._dbg_last_value: Dict[Tuple[str, str, str], Any] = {}

        # Redibujar sólo cuando hay cambios
        self._dirty = False
        self._last_frame_hash = 0

    async def stop(self):
        self._stop.set()

    def _changed_enough(self, key, newv) -> bool:
        """Aplica umbral mínimo de cambio para dbg:* (abs o rel)."""
        oldv = self._dbg_last_value.get(key)
        if oldv is None:
            self._dbg_last_value[key] = newv
            return True

        def num(x):
            try:
                return float(x)
            except Exception:
                return None

        # Si dict, compara numéricamente claves comunes
        if isinstance(newv, dict) and isinstance(oldv, dict):
            for k in newv.keys() & oldv.keys():
                a, b = num(newv[k]), num(oldv[k])
                if a is None or b is None:
                    continue
                if self._dbg_min_change_mode == "abs":
                    if abs(a - b) >= self._dbg_min_change_val:
                        self._dbg_last_value[key] = newv
                        return True
                else:  # rel
                    denom = max(abs(b), 1e-12)
                    if abs(a - b) / denom >= self._dbg_min_change_val:
                        self._dbg_last_value[key] = newv
                        return True
            return False
        else:
            a, b = num(newv), num(oldv)
            if a is None or b is None:
                self._dbg_last_value[key] = newv
                return True
            if self._dbg_min_change_mode == "abs":
                ok = abs(a - b) >= self._dbg_min_change_val
            else:
                denom = max(abs(b), 1e-12)
                ok = abs(a - b) / denom >= self._dbg_min_change_val
            if ok:
                self._dbg_last_value[key] = newv
            return ok

    async def update(self, symbol: str, indicator: str, value, ts: int, tf: str | None = None):
        key = (symbol, tf or "-", indicator)

        # Rate limit para dbg:* (por línea)
        if indicator.startswith("dbg:"):
            now = _now_ms()
            last = self._dbg_last_emit_ms.get(key, 0)
            if now - last < self._dbg_every_ms:
                # Sólo acepta si el cambio es suficientemente grande
                if not self._changed_enough(key, value):
                    return
            # Si pasa el filtro, actualiza marcas
            self._dbg_last_emit_ms[key] = now

        async with self._lock:
            self.latest[key] = {"value": value, "ts": ts}
            self._dirty = True

    async def run(self):
        try:
            while not self._stop.is_set():
                await self._draw()
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            pass

    async def _draw(self):
        # Redibujar sólo si hubo cambios desde el último frame
        if not self._dirty:
            return

        async with self._lock:
            items = sorted(self.latest.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2]))
            self._dirty = False

        # Limpia pantalla (Windows = cls, Linux/Mac = clear)
        os.system("cls" if os.name == "nt" else "clear")

        out_lines = []
        out_lines.append("🧮 indicators-engine — vista en vivo (1 línea por indicador)\n")

        if not items:
            out_lines.append("(esperando datos…)\n")
            print("".join(out_lines), end="", flush=True)
            return

        last_sym, last_tf = None, None
        for (sym, tf, ind), payload in items:
            if sym != last_sym:
                out_lines.append(f"\n┌─ {sym} ───────────────────────────────────────────────\n")
                last_sym = sym
                last_tf = None
            if tf != last_tf:
                out_lines.append(f"│  tf: {tf}\n")
                last_tf = tf

            vtxt = self._fmt_value(ind, payload.get("value"))
            out_lines.append(f"│   • {ind:<18} {vtxt}\n")

        print("".join(out_lines), end="", flush=True)

    def _fmt_value(self, ind: str, v):
        """
        Formatea inteligentemente: dicts clave=valor, números con k/M, etc.
        """
        try:
            if isinstance(v, dict):
                # Atajo para MACD {macd, signal, hist}
                if {"macd", "signal", "hist"} <= set(v.keys()):
                    return f"macd={self._n(v['macd'])}  sig={self._n(v['signal'])}  hist={self._n(v['hist'])}"
                parts = []
                for k in sorted(v.keys()):
                    val = v[k]
                    if isinstance(val, bool):
                        parts.append(f"{k}={'true' if val else 'false'}")
                    else:
                        parts.append(f"{k}={self._n(val) if isinstance(val,(int,float)) else val}")
                return "  ".join(parts)
            elif isinstance(v, bool):
                return "true" if v else "false"
            elif isinstance(v, (int, float)):
                return self._n(v)
            else:
                return str(v)
        except Exception:
            return str(v)

    @staticmethod
    def _n(x):
        """
        Formateo numérico compacto: 1234 -> 1.23k, 1_234_567 -> 1.23M, etc.
        """
        try:
            ax = abs(float(x))
            if ax >= 1_000_000: return f"{x/1_000_000:.2f}M"
            if ax >= 1_000:     return f"{x/1_000:.2f}k"
            if ax >= 100:       return f"{x:.2f}"
            if ax >= 1:         return f"{x:.3f}"
            return f"{x:.5f}"
        except Exception:
            return str(x)
