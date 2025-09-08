# indicators-engine

Servicio en Python para calcular indicadores técnicos (RSI, CVD, ADX, MACD, VWAP, etc.)
a partir de datos de dxFeed que llegan vía NATS.

## Flujo
1. Suscripción a `market.*` (candles, trades, order book).
2. Cálculo de indicadores.
3. Publicación en `indicators.*` para el motor de trading.

## Ejecución local
```bash
python -m venv .venv
. .venv/Scripts/activate  # en Windows
pip install -e .
python -m indicators_engine.app
