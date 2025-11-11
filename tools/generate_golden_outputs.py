#!/usr/bin/env python3
"""
Genera outputs esperados (golden snapshots) ejecutando el engine sobre fixtures capturados.

Uso:
    python tools/generate_golden_outputs.py tests/fixtures/live_session.jsonl tests/fixtures/golden_outputs.jsonl
"""
import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List

import orjson

# Asegurar que el path del proyecto está en sys.path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from indicators_engine.engine import IndicatorsEngine


class GoldenPublisher:
    """Publisher que captura todos los outputs para generar golden snapshots."""

    def __init__(self) -> None:
        self.outputs: List[Dict[str, Any]] = []

    async def publish_book(self, name: str, symbol: str, payload: Dict[str, Any]) -> None:
        self.outputs.append(
            {
                "type": "book",
                "indicator": name,
                "symbol": symbol,
                "payload": payload,
            }
        )

    async def publish_trades(self, name: str, symbol: str, payload: Dict[str, Any]) -> None:
        self.outputs.append(
            {
                "type": "trades",
                "indicator": name,
                "symbol": symbol,
                "payload": payload,
            }
        )

    async def publish_candle(
        self, tf: str, name: str, symbol: str, payload: Dict[str, Any]
    ) -> None:
        self.outputs.append(
            {
                "type": "candle",
                "tf": tf,
                "indicator": name,
                "symbol": symbol,
                "payload": payload,
            }
        )


async def process_fixtures(fixtures_path: Path, output_path: Path) -> None:
    """Procesa fixtures y genera outputs esperados."""
    publisher = GoldenPublisher()
    engine = IndicatorsEngine(publisher)

    events_processed = 0
    with open(fixtures_path, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                entry = orjson.loads(line)
                subject = entry.get("subject", "")
                payload = entry.get("payload", {})

                if subject.startswith("md.trades."):
                    await engine.on_trade_dict(payload)
                    events_processed += 1
                elif subject.startswith("md.book.") or subject.startswith("md.bbo."):
                    await engine.on_book_dict(payload)
                    events_processed += 1
                elif subject.startswith("md.candles."):
                    await engine.on_candle_dict(payload)
                    events_processed += 1
            except Exception as e:
                print(f"Error procesando evento: {e}")
                continue

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        for output in publisher.outputs:
            f.write(orjson.dumps(output) + b"\n")

    print(f"Procesados {events_processed} eventos")
    print(f"Generados {len(publisher.outputs)} outputs en {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Genera golden outputs desde fixtures")
    parser.add_argument("fixtures", type=Path, help="Ruta a fixtures JSONL capturados")
    parser.add_argument("output", type=Path, help="Ruta de salida para golden outputs")
    args = parser.parse_args()

    if not args.fixtures.exists():
        print(f"Error: {args.fixtures} no existe")
        return

    asyncio.run(process_fixtures(args.fixtures, args.output))


if __name__ == "__main__":
    main()

