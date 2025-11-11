#!/usr/bin/env python3
"""
Script para capturar eventos reales de NATS y guardarlos como fixtures para tests.

Uso:
    python tools/capture_fixtures.py --seconds 30 --output tests/fixtures/live_session_20241110.jsonl
"""
import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import orjson
from nats.aio.client import Client as NATS


async def capture_events(
    url: str,
    pattern: str,
    seconds: float,
    limit: int,
    output_path: Path,
) -> None:
    """Captura eventos de NATS y los guarda en formato JSONL."""
    nc = NATS()
    await nc.connect(servers=[url])

    captured: list[Dict[str, Any]] = []
    done = asyncio.Event()

    async def handler(msg):
        try:
            payload = orjson.loads(msg.data)
            entry = {
                "subject": msg.subject,
                "ts_iso": datetime.now().isoformat(),
                "payload": payload,
            }
            captured.append(entry)
            print(f"{msg.subject}: {list(payload.keys())}")
            if len(captured) >= limit:
                done.set()
        except Exception as e:
            print(f"Error procesando mensaje: {e}")

    sub = await nc.subscribe(pattern, cb=handler)
    print(f"Escuchando {pattern} en {url} durante {seconds}s (max {limit} mensajes)")

    try:
        await asyncio.wait_for(done.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

    await sub.unsubscribe()
    await nc.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        for entry in captured:
            f.write(orjson.dumps(entry) + b"\n")

    print(f"\nCapturados {len(captured)} eventos en {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Captura eventos de NATS como fixtures")
    parser.add_argument("--url", default="nats://127.0.0.1:4222", help="URL de NATS")
    parser.add_argument(
        "--pattern",
        default="md.>",
        help="Patrón de subjects a capturar (default: md.>)",
    )
    parser.add_argument("--seconds", type=float, default=30.0, help="Segundos a escuchar")
    parser.add_argument("--limit", type=int, default=1000, help="Límite de mensajes")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Ruta de salida (JSONL)",
    )

    args = parser.parse_args()
    asyncio.run(capture_events(args.url, args.pattern, args.seconds, args.limit, args.output))


if __name__ == "__main__":
    main()

