# tests/unit/test_parse_book_formats.py
"""
Test unitario para validar que parse_book() maneja correctamente
los diferentes formatos de mensajes de book de data-extractor.
"""
import pytest
from indicators_engine.engine import parse_book


def test_parse_book_l2_format():
    """Test formato L2 (bids/asks como arrays)"""
    d = {
        "ts": 1000,
        "symbol": "AAPL",
        "bids": [
            {"p": 149.9, "v": 10.0},
            {"p": 149.8, "v": 5.0}
        ],
        "asks": [
            {"p": 150.1, "v": 10.0},
            {"p": 150.2, "v": 5.0}
        ]
    }
    book = parse_book(d)
    assert book.symbol == "AAPL"
    assert book.ts == 1000
    assert len(book.bids) == 2
    assert len(book.asks) == 2
    assert book.bids[0]["p"] == 149.9
    assert book.bids[0]["v"] == 10.0


def test_parse_book_frame_format():
    """Test formato book_frame (b1/a1 como objetos)"""
    d = {
        "type": "book_frame",
        "ts": 1000,
        "symbol": "AAPL",
        "b1": {"p": 149.9, "v": 10.0},
        "a1": {"p": 150.1, "v": 10.0}
    }
    book = parse_book(d)
    assert book.symbol == "AAPL"
    assert book.ts == 1000
    assert len(book.bids) == 1
    assert len(book.asks) == 1
    assert book.bids[0]["p"] == 149.9
    assert book.asks[0]["p"] == 150.1


def test_parse_bbo_frame_format():
    """Test formato BBO (bid/ask como valores individuales)"""
    d = {
        "type": "bbo_frame",
        "ts": 1000,
        "symbol": "AAPL",
        "bid": 149.9,
        "bidSize": 10.0,
        "ask": 150.1,
        "askSize": 10.0
    }
    book = parse_book(d)
    assert book.symbol == "AAPL"
    assert book.ts == 1000
    assert len(book.bids) == 1
    assert len(book.asks) == 1
    assert book.bids[0]["p"] == 149.9
    assert book.bids[0]["v"] == 10.0
    assert book.asks[0]["p"] == 150.1
    assert book.asks[0]["v"] == 10.0


def test_parse_book_invalid_format():
    """Test que formato inválido lanza error"""
    d = {
        "ts": 1000,
        "symbol": "AAPL"
        # Sin bids, asks, b1, a1, bid, ask
    }
    with pytest.raises(ValueError, match="Formato de book desconocido"):
        parse_book(d)

