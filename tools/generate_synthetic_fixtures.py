#!/usr/bin/env python3
"""
Genera fixtures sintéticos de ejemplo para testing sin necesidad de NATS corriendo.

Uso:
    python tools/generate_synthetic_fixtures.py --output tests/fixtures/synthetic_session.jsonl
"""
import argparse
import random
from datetime import datetime, timezone
from pathlib import Path

import orjson


def generate_synthetic_fixtures(output_path: Path, num_events: int = 100) -> None:
    """Genera fixtures sintéticos con eventos de trades, books y candles."""
    symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
    events = []

    base_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    base_price = 150.0

    # Generar eventos de trades (vwap frames)
    for i in range(num_events // 3):
        symbol = random.choice(symbols)
        price = base_price + random.uniform(-10, 10)
        vwap = base_price + random.uniform(-5, 5)
        cum_volume = 1000.0 + i * 10.0

        events.append(
            {
                "subject": f"md.trades.vwap",
                "ts_iso": datetime.now(timezone.utc).isoformat(),
                "payload": {
                    "type": "vwap_frame",
                    "ts": base_time + i * 1000,
                    "symbol": symbol,
                    "vwap": round(vwap, 2),
                    "price": round(price, 2),
                    "cumV": round(cum_volume, 2),
                },
            }
        )

    # Generar eventos de book (bids/asks)
    for i in range(num_events // 3):
        symbol = random.choice(symbols)
        mid_price = base_price + random.uniform(-10, 10)
        spread = random.uniform(0.1, 0.5)

        bids = [
            {"p": round(mid_price - spread / 2 - j * 0.1, 2), "v": round(random.uniform(5, 20), 2)}
            for j in range(5)
        ]
        asks = [
            {"p": round(mid_price + spread / 2 + j * 0.1, 2), "v": round(random.uniform(5, 20), 2)}
            for j in range(5)
        ]

        events.append(
            {
                "subject": f"md.book.{symbol.lower()}",
                "ts_iso": datetime.now(timezone.utc).isoformat(),
                "payload": {
                    "ts": base_time + i * 1000 + 500,
                    "symbol": symbol,
                    "bids": bids,
                    "asks": asks,
                },
            }
        )

    # Generar eventos de candles
    for i in range(num_events // 3):
        symbol = random.choice(symbols)
        open_price = base_price + random.uniform(-10, 10)
        close_price = open_price + random.uniform(-2, 2)
        high_price = max(open_price, close_price) + random.uniform(0, 1)
        low_price = min(open_price, close_price) - random.uniform(0, 1)
        volume = random.uniform(100, 1000)

        events.append(
            {
                "subject": f"md.candles.1m.{symbol.lower()}",
                "ts_iso": datetime.now(timezone.utc).isoformat(),
                "payload": {
                    "ts": base_time + i * 60000,  # 1 minuto
                    "symbol": symbol,
                    "tf": "1m",
                    "o": round(open_price, 2),
                    "h": round(high_price, 2),
                    "l": round(low_price, 2),
                    "c": round(close_price, 2),
                    "v": round(volume, 2),
                },
            }
        )

    # Ordenar por timestamp
    events.sort(key=lambda x: x["payload"]["ts"])

    # Guardar
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        for event in events:
            f.write(orjson.dumps(event) + b"\n")

    print(f"Generados {len(events)} eventos sintéticos en {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Genera fixtures sintéticos")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("tests/fixtures/synthetic_session.jsonl"),
        help="Ruta de salida",
    )
    parser.add_argument("--num-events", type=int, default=100, help="Número de eventos a generar")
    args = parser.parse_args()

    generate_synthetic_fixtures(args.output, args.num_events)


if __name__ == "__main__":
    main()

