import pytest
from indicators_engine.pipelines.book_order import normalize_dxfeed_book_order

def test_normalize_full_depth_snapshot():
    d = {
        "eventSymbol": "ESZ5",
        "time": 1726500000000000000,  # nanos
        "bids": [[4999.50, 5], [4999.25, 2], [4999.00, 1]],
        "asks": [[5000.00, 3], [5000.25, 4], [5000.50, 2]],
    }
    out = normalize_dxfeed_book_order(d)
    assert out is not None
    assert out["symbol"] == "ESZ5"
    assert out["indicator"] == "book_order"
    assert out["ts"] == 1726500000000
    # profundidad completa (no trim)
    assert len(out["bids"]) == 3
    assert len(out["asks"]) == 3
    # orden correcto
    assert out["bids"][0][0] == 4999.5
    assert out["asks"][0][0] == 5000.0
