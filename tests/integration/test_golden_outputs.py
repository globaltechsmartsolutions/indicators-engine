"""
Tests de regresión usando fixtures reales capturados y golden outputs.

Para generar fixtures:
    1. Levanta el stack completo (fake extractor + engine)
    2. python tools/capture_fixtures.py --seconds 30 --output tests/fixtures/live_session.jsonl

Para generar golden outputs:
    python tools/generate_golden_outputs.py tests/fixtures/live_session.jsonl tests/fixtures/golden_outputs.jsonl
"""
import asyncio
from pathlib import Path
from typing import Any, Dict, List

import orjson
import pytest

from indicators_engine.engine import IndicatorsEngine


class GoldenPublisher:
    """Publisher que captura outputs para comparar con golden."""

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


def load_fixtures(fixtures_path: Path) -> List[Dict[str, Any]]:
    """Carga fixtures desde JSONL."""
    events = []
    with open(fixtures_path, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            events.append(orjson.loads(line))
    return events


def load_golden_outputs(golden_path: Path) -> List[Dict[str, Any]]:
    """Carga golden outputs desde JSONL."""
    outputs = []
    with open(golden_path, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            outputs.append(orjson.loads(line))
    return outputs


def normalize_for_comparison(value: Any) -> Any:
    """Normaliza tuplas a listas y recursivamente normaliza estructuras anidadas."""
    if isinstance(value, tuple):
        return [normalize_for_comparison(item) for item in value]
    elif isinstance(value, list):
        return [normalize_for_comparison(item) for item in value]
    elif isinstance(value, dict):
        return {k: normalize_for_comparison(v) for k, v in value.items()}
    else:
        return value


@pytest.mark.asyncio
async def test_engine_outputs_match_golden() -> None:
    """Compara outputs del engine con golden snapshots."""
    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    # Buscar primero live_session, luego synthetic_session
    fixtures_path = fixtures_dir / "live_session.jsonl"
    if not fixtures_path.exists():
        fixtures_path = fixtures_dir / "synthetic_session.jsonl"
    golden_path = fixtures_dir / "golden_outputs.jsonl"

    if not fixtures_path.exists():
        pytest.skip(f"Fixtures no encontrados. Ejecuta capture_fixtures.py o generate_synthetic_fixtures.py primero.")

    if not golden_path.exists():
        pytest.skip(f"Golden outputs no encontrados en {golden_path}. Ejecuta generate_golden_outputs.py primero.")

    publisher = GoldenPublisher()
    engine = IndicatorsEngine(publisher)

    # Procesar fixtures
    events = load_fixtures(fixtures_path)
    for entry in events:
        subject = entry.get("subject", "")
        payload = entry.get("payload", {})

        try:
            if subject.startswith("md.trades."):
                await engine.on_trade_dict(payload)
            elif subject.startswith("md.book.") or subject.startswith("md.bbo."):
                await engine.on_book_dict(payload)
            elif subject.startswith("md.candles."):
                await engine.on_candle_dict(payload)
        except Exception as e:
            pytest.fail(f"Error procesando {subject}: {e}")

    # Comparar con golden
    golden_outputs = load_golden_outputs(golden_path)

    assert len(publisher.outputs) == len(golden_outputs), (
        f"Outputs generados ({len(publisher.outputs)}) != golden ({len(golden_outputs)})"
    )

    for actual, expected in zip(publisher.outputs, golden_outputs):
        assert actual["type"] == expected["type"]
        assert actual["indicator"] == expected["indicator"]
        assert actual["symbol"] == expected["symbol"]

        actual_payload = actual["payload"]
        expected_payload = expected["payload"]

        # Normalizar para comparación (tuplas -> listas)
        actual_payload = normalize_for_comparison(actual_payload)
        expected_payload = normalize_for_comparison(expected_payload)

        # Comparar campos críticos con tolerancia para floats
        for key in expected_payload:
            if key == "ts":
                continue  # Timestamps pueden variar
            assert key in actual_payload, f"Campo {key} faltante en output actual"
            if isinstance(expected_payload[key], float):
                assert actual_payload[key] == pytest.approx(
                    expected_payload[key], abs=1e-6
                ), f"{key}: {actual_payload[key]} != {expected_payload[key]}"
            else:
                assert actual_payload[key] == expected_payload[key], (
                    f"{key}: {actual_payload[key]} != {expected_payload[key]}"
                )


@pytest.mark.asyncio
async def test_liquidity_values_within_bounds() -> None:
    """Valida que los valores de liquidity están en rangos esperados."""
    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    fixtures_path = fixtures_dir / "live_session.jsonl"
    if not fixtures_path.exists():
        fixtures_path = fixtures_dir / "synthetic_session.jsonl"
    if not fixtures_path.exists():
        pytest.skip("Fixtures no encontrados")

    publisher = GoldenPublisher()
    engine = IndicatorsEngine(publisher)

    events = load_fixtures(fixtures_path)
    liquidity_outputs = []

    for entry in events:
        subject = entry.get("subject", "")
        payload = entry.get("payload", {})
        if subject.startswith("md.book.") or subject.startswith("md.bbo."):
            await engine.on_book_dict(payload)

    for output in publisher.outputs:
        if output["indicator"] == "liquidity":
            liquidity_outputs.append(output["payload"])

    if not liquidity_outputs:
        pytest.skip("No se generaron outputs de liquidity")

    for liq in liquidity_outputs:
        assert -1.0 <= liq.get("depth_imbalance", 0) <= 1.0, "depth_imbalance fuera de rango"
        assert -1.0 <= liq.get("top_imbalance", 0) <= 1.0, "top_imbalance fuera de rango"
        assert liq.get("spread", 0) >= 0, "spread negativo"
        assert liq.get("mid", 0) > 0, "mid debe ser positivo"
        assert liq.get("best_bid", 0) > 0, "best_bid debe ser positivo"
        assert liq.get("best_ask", 0) > 0, "best_ask debe ser positivo"

