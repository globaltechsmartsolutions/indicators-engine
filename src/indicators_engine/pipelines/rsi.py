# src/indicators_engine/pipelines/rsi.py
from collections import defaultdict

class RsiCalc:
    """
    RSI incremental de Wilder, sin pandas.
    Mantiene estado por clave (symbol|tf). Requiere 'period' cierres para emitir el primer valor.
    """
    def __init__(self, period: int = 10):
        if period < 1:
            raise ValueError("RSI period must be >= 1")
        self.period = period
        self.state = defaultdict(lambda: {
            "prev_close": None,
            "count": 0,
            "sum_gain": 0.0,
            "sum_loss": 0.0,
            "avg_gain": None,
            "avg_loss": None,
            "last_ts": None,
        })

    def on_bar(self, symbol: str, tf: str, ts: int, close: float):
        key = f"{symbol}|{tf}"
        s = self.state[key]

        # Ignora duplicados o barras fuera de orden
        if s["last_ts"] is not None and ts <= s["last_ts"]:
            return None
        s["last_ts"] = ts

        # Primera barra: solo inicializa
        if s["prev_close"] is None:
            s["prev_close"] = close
            s["count"] = 1
            return None

        change = close - s["prev_close"]
        s["prev_close"] = close

        gain = change if change > 0 else 0.0
        loss = -change if change < 0 else 0.0

        # Construcción de la ventana inicial (media simple)
        if s["avg_gain"] is None or s["avg_loss"] is None:
            s["sum_gain"] += gain
            s["sum_loss"] += loss
            s["count"] += 1

            # Aún no hay suficientes barras
            if s["count"] <= self.period:
                return None

            # Primeras medias (tras acumular 'period' cambios)
            s["avg_gain"] = s["sum_gain"] / self.period
            s["avg_loss"] = s["sum_loss"] / self.period

        else:
            # Suavizado de Wilder
            s["avg_gain"] = ((s["avg_gain"] * (self.period - 1)) + gain) / self.period
            s["avg_loss"] = ((s["avg_loss"] * (self.period - 1)) + loss) / self.period

        ag, al = s["avg_gain"], s["avg_loss"]

        if al == 0.0:
            # Sin pérdidas: RSI 100 si hubo alguna ganancia; si no, 50 neutro
            return 100.0 if ag > 0.0 else 50.0

        rs = ag / al
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return float(rsi)
