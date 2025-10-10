from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable, Any, Mapping

from .types import Bar


# ========= Interfaces base (para uniformidad y testeo) =========

@runtime_checkable
class StreamIndicator(Protocol):
    """
    Contrato común para indicadores 'streaming' que se alimentan barra a barra.
    - reset(): re-inicializa estado interno
    - on_bar(bar): procesa una barra; devuelve:
        * None mientras está en warm-up o no haya nuevo valor, o
        * float / dict / cualquier resultado serializable cuando haya valor.
    - snapshot(): (opcional) devuelve estado/salida actual como dict serializable
    """
    def reset(self) -> None: ...
    def on_bar(self, bar: Bar) -> Optional[Any]: ...
    def snapshot(self) -> Mapping[str, Any]: ...


@runtime_checkable
class IndicatorBundle(Protocol):
    """
    Agrupa varios indicadores y los procesa con una misma Bar.
    Devuelve dict con salidas por nombre.
    """
    def reset(self) -> None: ...
    def on_bar(self, bar: Bar) -> Optional[dict]: ...
    def snapshot(self) -> Mapping[str, Any]: ...


# ========= Utilidades opcionales comunes =========

@dataclass(slots=True)
class WarmupState:
    """
    Lleva la cuenta de warm-up para indicadores que requieren N barras mínimas.
    """
    need: int            # barras necesarias (p.ej. period + 1)
    seen: int = 0

    def tick(self) -> bool:
        """Incrementa y devuelve True si AÚN está en warm-up."""
        self.seen += 1
        return self.seen < self.need

    @property
    def left(self) -> int:
        """Cuántas barras faltan (0 si ya completó)."""
        return max(0, self.need - self.seen)


class TsOrderedMixin:
    """
    Mixin para indicadores que reciben barras posiblemente repetidas.
    - Descarta solo si ts < last_ts.
    - Si ts == last_ts, permite 'overwrite' de la última muestra.
    """
    __slots__ = ("_last_ts",)

    def __init__(self) -> None:
        self._last_ts: Optional[int] = None

    def allow_ts(self, ts: int) -> bool:
        if self._last_ts is not None and ts < self._last_ts:
            return False
        self._last_ts = ts
        return True
