import asyncio
from typing import Any, Dict, List, Tuple

import pytest

from indicators_engine.engine import IndicatorsEngine


class StubPublisher:
    def __init__(self) -> None:
        self.published_books: List[Tuple[str, str, Dict[str, Any]]] = []
        self.published_trades: List[Tuple[str, str, Dict[str, Any]]] = []
        self.published_candles: List[Tuple[str, str, str, Dict[str, Any]]] = []

    async def publish_book(self, name: str, symbol: str, payload: Dict[str, Any]) -> None:
        self.published_books.append((name, symbol, payload))

    async def publish_trades(self, name: str, symbol: str, payload: Dict[str, Any]) -> None:
        self.published_trades.append((name, symbol, payload))

    async def publish_candle(
        self, tf: str, name: str, symbol: str, payload: Dict[str, Any]
    ) -> None:
        self.published_candles.append((tf, name, symbol, payload))


@pytest.mark.asyncio
async def test_liquidity_and_heatmap_payloads_expose_metrics() -> None:
    publisher = StubPublisher()
    engine = IndicatorsEngine(publisher)

    book_event = {
        "ts": 1,
        "symbol": "TEST",
        "bids": [{"p": 100.0, "v": 10.0}, {"p": 99.5, "v": 5.0}],
        "asks": [{"p": 100.5, "v": 8.0}, {"p": 101.0, "v": 6.0}],
    }

    await engine.on_book_dict(book_event)

    assert len(publisher.published_books) == 2, "Expected liquidity and heatmap payloads"

    liq_payload = next(payload for name, _, payload in publisher.published_books if name == "liquidity")
    heatmap_payload = next(payload for name, _, payload in publisher.published_books if name == "heatmap")

    expected_liq_keys = {
        "mid",
        "spread",
        "bids_depth",
        "asks_depth",
        "depth_imbalance",
        "top_imbalance",
        "best_bid",
        "best_ask",
        "bid1_size",
        "ask1_size",
    }
    assert expected_liq_keys.issubset(liq_payload.keys())

    expected_heatmap_keys = {"tiles", "max_sz", "bucket_ts", "bucket_ms", "compression_ratio"}
    assert expected_heatmap_keys.intersection(heatmap_payload.keys()), "Heatmap payload missing expected keys"


@pytest.mark.asyncio
async def test_vwap_frame_includes_price_deviation() -> None:
    publisher = StubPublisher()
    engine = IndicatorsEngine(publisher)

    vwap_frame = {
        "type": "vwap_frame",
        "ts": 2,
        "symbol": "AAPL",
        "vwap": 125.00,
        "price": 126.88,
        "cumV": 1000.0,
    }

    await engine.on_trade_dict(vwap_frame)

    published = next(payload for name, _, payload in publisher.published_trades if name == "vwap")

    assert published["vwap"] == pytest.approx(125.00)
    assert published["last_price"] == pytest.approx(126.88)
    assert "deviation_abs" in published and "deviation_pct" in published
    assert published["deviation_abs"] == pytest.approx(1.88, abs=1e-6)
    assert published["deviation_pct"] == pytest.approx((1.88 / 125.00) * 100.0)

