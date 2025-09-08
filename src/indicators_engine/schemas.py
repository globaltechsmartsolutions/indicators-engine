from pydantic import BaseModel

class CandleIn(BaseModel):
    symbol: str
    tf: str
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
