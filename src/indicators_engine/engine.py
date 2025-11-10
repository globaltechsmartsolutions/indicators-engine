from __future__ import annotations
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from collections import defaultdict
import logging

from indicators_engine.core.types import Bar, Trade, BookSnapshot
from indicators_engine.nats.publisher import IndicatorPublisher
from indicators_engine.logs.logger import get_logger

logger = get_logger(__name__)
# Puedes usar ejemplos:
# logger.info('Motor iniciado')
# logger.debug(f'Parámetros: {params}')
# logger.error('Ocurrió un error', exc_info=True)

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
    """
    Parsea un mensaje de book a BookSnapshot.
    Acepta múltiples formatos:
    - L2 frame: bids=[{"p": float, "v": float}, ...], asks=[...]
    - Book frame: b1={"p": float, "v": float}, a1={...}
    - BBO frame: bid=float, bidSize=float, ask=float, askSize=float
    """
    bids = []
    asks = []
    
    # Formato 1: L2 frame o book completo con arrays bids/asks
    if "bids" in d and "asks" in d:
        bids = [{"p": float(x["p"]), "v": float(x["v"])} for x in d.get("bids", [])]
        asks = [{"p": float(x["p"]), "v": float(x["v"])} for x in d.get("asks", [])]
    # Formato 2: Book frame con b1/a1 (primer nivel como objeto)
    elif "b1" in d or "a1" in d:
        if "b1" in d:
            b1 = d["b1"]
            bids = [{"p": float(b1["p"]), "v": float(b1["v"])}]
        if "a1" in d:
            a1 = d["a1"]
            asks = [{"p": float(a1["p"]), "v": float(a1["v"])}]
    # Formato 3: BBO frame con bid/ask individuales
    elif "bid" in d and "ask" in d:
        bids = [{"p": float(d["bid"]), "v": float(d.get("bidSize", 0.0))}]
        asks = [{"p": float(d["ask"]), "v": float(d.get("askSize", 0.0))}]
    else:
        raise ValueError(
            f"Formato de book desconocido. Campos disponibles: {list(d.keys())}. "
            "Esperado: bids/asks (arrays), b1/a1 (objetos), o bid/ask (valores)"
        )
    
    return BookSnapshot(
        ts=int(d["ts"]),
        symbol=str(d["symbol"]),
        bids=bids,
        asks=asks,
    )


class IndicatorsEngine:
    """
    Recibe datos crudos (dict), los parsea a tipos, calcula indicadores
    y publica con IndicatorPublisher.
    """
    def __init__(self, publisher: IndicatorPublisher):
        self.pub = publisher

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

    @staticmethod
    def _to_dict(value: Any) -> Dict[str, Any]:
        """
        Convierte resultados de PyO3 (que suelen exponerse como objetos con atributos)
        a un dict plano para poder serializar/publicar sin problemas.
        """

        def _convert(obj: Any) -> Any:
            if obj is None or isinstance(obj, (str, float, int, bool)):
                return obj
            if isinstance(obj, dict):
                return {k: _convert(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple, set)):
                return [_convert(v) for v in obj]

            for attr in ("dict", "to_dict", "_asdict", "as_dict"):
                candidate = getattr(obj, attr, None)
                if callable(candidate):
                    try:
                        result = candidate()
                        if result is not None:
                            return _convert(result)
                    except Exception:
                        continue

            if hasattr(obj, "__dict__") and obj.__dict__:
                return {k: _convert(v) for k, v in obj.__dict__.items() if not k.startswith("_")}

            data: Dict[str, Any] = {}
            for attr in dir(obj):
                if attr.startswith("_"):
                    continue
                try:
                    attr_value = getattr(obj, attr)
                except Exception:
                    continue
                if callable(attr_value):
                    continue
                data[attr] = _convert(attr_value)
            if data:
                return data
            try:
                return dict(vars(obj))
            except Exception:
                return str(obj)

        if value is None:
            return {}
        if isinstance(value, dict):
            return value

        converted = _convert(value)
        if isinstance(converted, dict):
            return converted
        return {"value": converted}

    # --- Hooks para el subscriber (reciben dicts crudos) ---
    async def on_candle_dict(self, d: Dict[str, Any]) -> None:
        bar = parse_bar(d)

        rsi_val = self.rsi.on_bar(bar)
        if rsi_val is not None:
            name = f"rsi{getattr(self.rsi, 'period', 14)}"
            await self.pub.publish_candle(bar.tf, name, bar.symbol, {"ts": bar.ts, "value": float(rsi_val)})

        macd_out = self.macd.on_bar(bar)
        if macd_out:
            await self.pub.publish_candle(bar.tf, "macd", bar.symbol, {"ts": bar.ts, **macd_out})

        adx_out = self.adx.on_bar(bar)
        if adx_out:
            name = f"adx{getattr(self.adx, 'period', 14)}"
            await self.pub.publish_candle(bar.tf, name, bar.symbol, {"ts": bar.ts, **adx_out})

        _ = self.svp.on_bar(bar)
        svp_top = self.svp.snapshot_top(symbol=bar.symbol)  # ← cambio clave
        if svp_top:
            payload = {"ts": bar.ts, "top": svp_top}
            await self.pub.publish_candle(bar.tf, "svp", bar.symbol, payload)

        vprof_out = self.volprof.on_bar(bar)
        if vprof_out:
            await self.pub.publish_candle(bar.tf, "volprof", bar.symbol, {"ts": bar.ts, **vprof_out})

    async def on_trade_dict(self, d: Dict[str, Any]) -> None:
        # Si te llega un frame agregado aquí por error, redirige
        if d.get("type") == "oflow_frame":
            await self.on_oflow_frame_dict(d)
            return
        
        # Manejar vwap_frame directamente (ya viene calculado desde data-extractor)
        if d.get("type") == "vwap_frame":
            try:
                symbol = str(d["symbol"])
                ts = int(d["ts"])
                vwap = float(d["vwap"])
                payload = {"ts": ts, "vwap": vwap}
                price = d.get("price") or d.get("last_price")
                if isinstance(price, (int, float)):
                    price = float(price)
                    diff_abs = price - vwap
                    diff_pct = (diff_abs / vwap) * 100 if vwap else 0.0
                    payload.update(
                        {
                            "last_price": price,
                            "deviation_abs": diff_abs,
                            "deviation_pct": diff_pct,
                        }
                    )
                cum_v = d.get("cumV") or d.get("cum_volume")
                if isinstance(cum_v, (int, float)):
                    payload["cum_volume"] = float(cum_v)
                await self.pub.publish_trades("vwap", symbol, payload)
            except Exception as e:
                logging.getLogger("runner").warning(
                    f"vwap_frame malformado ({e}): keys={list(d.keys())} payload={d}"
                )
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
            vwap_metrics = self._to_dict(vwap_result.value)
            vwap_value = vwap_metrics.get("vwap")
            if vwap_value is None:
                try:
                    vwap_value = float(vwap_result.value)
                except Exception:
                    vwap_value = None
            payload_vwap: Dict[str, Any] = {"ts": tr.ts}
            if vwap_value is not None:
                payload_vwap["vwap"] = float(vwap_value)
                price = trade_data.get("price")
                if isinstance(price, (int, float)) and vwap_value:
                    diff_abs = float(price) - float(vwap_value)
                    diff_pct = (diff_abs / float(vwap_value)) * 100.0
                    payload_vwap["last_price"] = float(price)
                    payload_vwap["deviation_abs"] = diff_abs
                    payload_vwap["deviation_pct"] = diff_pct
            for key in ("pv_sum", "v_sum", "session_id"):
                if key in vwap_metrics and key not in payload_vwap:
                    payload_vwap[key] = vwap_metrics[key]
            await self.pub.publish_trades("vwap", tr.symbol, payload_vwap)

        # CVD
        cvd_result = self.hybrid.calculate_cvd(trade_data)
        if cvd_result:
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
            await self.pub.publish_book(
                "liquidity",
                snap.symbol,
                {"ts": snap.ts, **self._to_dict(liq_result.value)},
            )

        # Heatmap: usar engine híbrido
        hm_result = self.hybrid.calculate_heatmap(book_data)
        if hm_result:
            await self.pub.publish_book(
                "heatmap",
                snap.symbol,
                {"ts": snap.ts, **self._to_dict(hm_result.value)},
            )
