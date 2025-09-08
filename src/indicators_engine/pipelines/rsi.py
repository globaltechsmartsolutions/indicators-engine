import pandas as pd
from ta.momentum import RSIIndicator

class RsiCalc:
    def __init__(self, period: int = 14, max_rows: int = 2000):
        self.period = period
        self.max_rows = max_rows
        # key=(symbol, tf) -> DataFrame[ts, close, rsi]
        self.buffers = {}

    def on_bar(self, symbol: str, tf: str, ts: int, close: float):
        key = (symbol, tf)
        df = self.buffers.get(key)

        if df is None:
            import pandas as pd
            df = pd.DataFrame(columns=["ts", "close"])

        # upsert por ts
        if (df["ts"] == ts).any():
            df.loc[df["ts"] == ts, "close"] = close
        else:
            df.loc[len(df)] = [ts, close]

        # orden temporal y recorte de ventana
        df = df.sort_values("ts")
        if len(df) > self.max_rows:
            df = df.iloc[-(self.max_rows // 2):].copy()

        # calcular RSI
        df["rsi"] = RSIIndicator(df["close"], window=self.period).rsi()
        self.buffers[key] = df

        last = df.iloc[-1]
        if pd.isna(last["rsi"]):
            return None
        return float(last["rsi"])
