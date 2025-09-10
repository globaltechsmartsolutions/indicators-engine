from collections import defaultdict

class MacdCalc:
    """
    MACD incremental: EMA(fast) - EMA(slow); Signal = EMA(signal); Hist = MACD - Signal.
    """
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        if not (1 <= fast < slow):
            raise ValueError("Parámetros inválidos para MACD (fast < slow)")
        self.fast, self.slow, self.signal = fast, slow, signal
        self.state = defaultdict(lambda: {
            "ema_fast": None, "ema_slow": None, "sig": None
        })
        self.kf = 2.0 / (fast + 1.0)
        self.ks = 2.0 / (slow + 1.0)
        self.ksig = 2.0 / (signal + 1.0)

    def on_bar(self, symbol: str, tf: str, ts: int, close: float):
        key = f"{symbol}|{tf}"
        s = self.state[key]
        s["ema_fast"] = close if s["ema_fast"] is None else (self.kf * close + (1 - self.kf) * s["ema_fast"])
        s["ema_slow"] = close if s["ema_slow"] is None else (self.ks * close + (1 - self.ks) * s["ema_slow"])

        if s["ema_fast"] is None or s["ema_slow"] is None:
            return None

        macd = s["ema_fast"] - s["ema_slow"]
        s["sig"] = macd if s["sig"] is None else (self.ksig * macd + (1 - self.ksig) * s["sig"])
        if s["sig"] is None:
            return None
        hist = macd - s["sig"]
        return {"macd": float(macd), "signal": float(s["sig"]), "hist": float(hist)}
