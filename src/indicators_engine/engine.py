from __future__ import annotations
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from collections import defaultdict
import logging

from indicators_engine.core.types import Bar, Trade, BookSnapshot
from indicators_engine.logs.liveRenderer import LiveRenderer
from indicators_engine.nats.publisher import IndicatorPublisher

# Engine Híbrido (Rust + Python fallback)
from indicators_engine.hybrid_engine import HybridIndicatorEngine

# Indicadores Python (solo los que NO están en Rust)
from indicators_engine.indicators.classic.rsi import RSI
from indicators_engine.indicators.classic.macd import MACD
from indicators_engine.indicators.classic.adx import ADX
from indicators_engine.indicators.volume.svp import SVP, SVPConfig
from indicators_engine.indicators.volume.volume_profile import VolumeProfile


def session_key_utc_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


# Parsers locales (raw dict -> tipos)
def parse_bar(d: Dict[str, Any]) -> Bar:
    return Bar(
        ts=int(d["ts"]),
        open=float(d.get("open", d.get("o"))),
        high=float(d.get("high", d.get("h"))),
        low=float(d.get("low", d.get("l"))),
        close=float(d.get("close", d.get("c"))),
        volume=float(d.get("volume", d.get("v", 0.0))),
        tf=str(d.get("tf", "-")),
        symbol=str(d["symbol"]),
    )

def parse_trade(d: Dict[str, Any]) -> Trade:
    # Timestamps comunes
    ts = d.get("ts") or d.get("t") or d.get("timestamp")
    if ts is None:
        raise KeyError("ts/t/timestamp no encontrado en trade payload")
    ts = int(ts)

    # Precio: admite varios alias
    price = (d.get("price") or d.get("p") or d.get("px") or
             d.get("last") or d.get("mp") or d.get("avg"))
    if price is None:
        raise KeyError("price/p/px/last/mp/avg no encontrado en trade payload")
    price = float(price)

    # Tamaño: admite varios alias
    size = (d.get("size") or d.get("qty") or d.get("q") or
            d.get("volume") or d.get("vol") or d.get("sz") or d.get("amount"))
    if size is None:
        raise KeyError("size/qty/q/volume/vol/sz/amount no encontrado en trade payload")
    size = float(size)

    # Símbolo: alias comunes
    symbol = d.get("symbol") or d.get("sym") or d.get("ticker") or d.get("S")
    if symbol is None:
        raise KeyError("symbol/sym/ticker/S no encontrado en trade payload")
    symbol = str(symbol)

    return Trade(
        ts=ts,
        price=price,
        size=size,
        symbol=symbol,
        exchange=d.get("exchange") or d.get("exch") or d.get("X"),
        side=d.get("side") or d.get("s"),  # si no viene, tu CVD puede inferirlo por mid
    )

def parse_book(d: Dict[str, Any]) -> BookSnapshot:
    return BookSnapshot(
        ts=int(d["ts"]),
        symbol=str(d["symbol"]),
        bids=[{"p": float(x["p"]), "v": float(x["v"])} for x in d.get("bids", [])],
        asks=[{"p": float(x["p"]), "v": float(x["v"])} for x in d.get("asks", [])],
    )


class IndicatorsEngine:
    """
    Recibe datos crudos (dict), los parsea a tipos, calcula indicadores,
    pinta con LiveRenderer y publica con IndicatorPublisher.
    """
    def __init__(self, publisher: IndicatorPublisher, renderer: LiveRenderer):
        self.pub = publisher
        self.ui = renderer

        # Engine Híbrido: usa Rust cuando está disponible, Python como fallback
        self.hybrid = HybridIndicatorEngine()

        # Indicadores Python puros (sin equivalente Rust)
        self.rsi = RSI()
        self.macd = MACD()
        self.adx = ADX()
        self.svp = SVP(SVPConfig(session_key_fn=session_key_utc_day, tick_size=0.01, top_n=10))
        self.volprof = VolumeProfile()

        # Acumulador de CVD a partir de frames agregados (oflow_frame)
        self._cvd_accum = defaultdict(float)  # por símbolo

    # --- Hooks para el subscriber (reciben dicts crudos) ---
    async def on_candle_dict(self, d: Dict[str, Any]) -> None:
        bar = parse_bar(d)

        rsi_val = self.rsi.on_bar(bar)
        if rsi_val is not None:
            name = f"rsi{getattr(self.rsi, 'period', 14)}"
            await self.ui.update(bar.symbol, name, rsi_val, bar.ts, bar.tf)
            await self.pub.publish_candle(bar.tf, name, bar.symbol, {"ts": bar.ts, "value": float(rsi_val)})

        macd_out = self.macd.on_bar(bar)
        if macd_out:
            await self.ui.update(bar.symbol, "macd", macd_out, bar.ts, bar.tf)
            await self.pub.publish_candle(bar.tf, "macd", bar.symbol, {"ts": bar.ts, **macd_out})

        adx_out = self.adx.on_bar(bar)
        if adx_out:
            name = f"adx{getattr(self.adx, 'period', 14)}"
            await self.ui.update(bar.symbol, name, adx_out, bar.ts, bar.tf)
            await self.pub.publish_candle(bar.tf, name, bar.symbol, {"ts": bar.ts, **adx_out})

        _ = self.svp.on_bar(bar)
        svp_top = self.svp.snapshot_top(symbol=bar.symbol)  # ← cambio clave
        if svp_top:
            payload = {"ts": bar.ts, "top": svp_top}
            await self.ui.update(bar.symbol, "svp", payload, bar.ts, bar.tf)
            await self.pub.publish_candle(bar.tf, "svp", bar.symbol, payload)

        vprof_out = self.volprof.on_bar(bar)
        if vprof_out:
            await self.ui.update(bar.symbol, "volprof", vprof_out, bar.ts, bar.tf)
            await self.pub.publish_candle(bar.tf, "volprof", bar.symbol, {"ts": bar.ts, **vprof_out})

    async def on_trade_dict(self, d: Dict[str, Any]) -> None:
        # Si te llega un frame agregado aquí por error, redirige
        if d.get("type") == "oflow_frame":
            await self.on_oflow_frame_dict(d)
            return

        try:
            tr = parse_trade(d)
        except Exception as e:
            logging.getLogger("runner").warning(
                f"Trade malformado ({e}): keys={list(d.keys())} payload={d}"
            )
            return

        # VWAP y CVD: usar engine híbrido (Rust o Python fallback)
        trade_data = {
            "ts": tr.ts,
            "price": tr.price,
            "size": tr.size,
            "symbol": tr.symbol,
            "side": tr.side,
            "exchange": tr.exchange,
        }
        
        # VWAP
        vwap_result = self.hybrid.calculate_vwap(trade_data)
        if vwap_result:
            await self.ui.update(tr.symbol, "vwap", vwap_result.value, tr.ts, "-")
            await self.pub.publish_trades("vwap", tr.symbol, {"ts": tr.ts, "vwap": float(vwap_result.value)})

        # CVD
        cvd_result = self.hybrid.calculate_cvd(trade_data)
        if cvd_result:
            await self.ui.update(tr.symbol, "cvd", {"cvd": cvd_result.value}, tr.ts, "-")
            await self.pub.publish_trades("cvd", tr.symbol, {"ts": tr.ts, "cvd": float(cvd_result.value)})

    # --- NUEVO: frames agregados de order flow ---
    async def on_oflow_frame_dict(self, d: Dict[str, Any]) -> None:
        """
        Payload típico:
          {"symbol":"SPY","windowMs":5000,"buy":8160.0,"sell":9648.0,"delta":-1488.0,"type":"oflow_frame","ts":1760123113372}
        """
        try:
            sym = str(d["symbol"])
            ts  = int(d["ts"])
            buy = float(d.get("buy", 0.0))
            sell = float(d.get("sell", 0.0))
            delta = float(d.get("delta", buy - sell))
            win = int(d.get("windowMs", 0))
        except Exception as e:
            logging.getLogger("runner").warning(f"oflow_frame malformado ({e}): {d}")
            return

        # acumula CVD a partir de delta
        self._cvd_accum[sym] += delta
        cvd_val = self._cvd_accum[sym]

        payload = {
            "ts": ts,
            "window_ms": win,
            "buy": buy,
            "sell": sell,
            "delta": delta,
            "cvd": cvd_val,
        }

        # pintar y publicar (subject: indicators.trades.oflow)
        await self.ui.update(sym, "oflow", payload, ts, "-")
        await self.pub.publish_trades("oflow", sym, payload)

    async def on_book_dict(self, d: Dict[str, Any]) -> None:
        snap = parse_book(d)
        
        # Convertir a formato dict para el hybrid engine
        book_data = {
            "ts": snap.ts,
            "symbol": snap.symbol,
            "bids": [{"price": float(l["p"]), "size": float(l["v"])} for l in snap.bids],
            "asks": [{"price": float(l["p"]), "size": float(l["v"])} for l in snap.asks],
        }

        # Liquidity: usar engine híbrido
        liq_result = self.hybrid.calculate_liquidity(book_data)
        if liq_result:
            await self.ui.update(snap.symbol, "liquidity", liq_result.value, snap.ts, "-")
            await self.pub.publish_book("liquidity", snap.symbol, {"ts": snap.ts, **liq_result.value})

        # Heatmap: usar engine híbrido
        hm_result = self.hybrid.calculate_heatmap(book_data)
        if hm_result:
            await self.ui.update(snap.symbol, "heatmap", hm_result.value, snap.ts, "-")
            await self.pub.publish_book("heatmap", snap.symbol, {"ts": snap.ts, **hm_result.value})
