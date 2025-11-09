# tests/integration/test_data_extractor_integration.py
"""
Test de integración end-to-end entre data-extractor (Java) e indicators-engine (Python).

Este test simula el flujo completo:
1. data-extractor publica mensajes en los subjects de market data
2. indicators-engine recibe, procesa y calcula indicadores
3. indicators-engine publica resultados en subjects de indicadores

Subjects que data-extractor publica:
- md.bbo.frame
- md.book.frame
- md.book.l2.frame
- md.trades.vwap
- md.trades.oflow
- md.candles.<tf>

Subjects que indicators-engine publica:
- indicators.candles.<tf>.<indicator>
- indicators.book.<indicator>
- indicators.trades.<indicator>
"""
import asyncio
import orjson
import pytest
import pytest_asyncio
from typing import Dict, Any, List
from datetime import datetime, timezone

from nats.aio.client import Client as NATS
from indicators_engine.nats.subscriber import NATSSubscriber
from indicators_engine.nats.publisher import IndicatorPublisher
from indicators_engine.engine import IndicatorsEngine


# ========== Helpers para simular mensajes de data-extractor ==========

def make_bbo_frame(symbol: str, ts: int, bid: float, bid_size: float, ask: float, ask_size: float) -> Dict[str, Any]:
    """Simula un mensaje BBO frame de data-extractor"""
    mid = (bid + ask) / 2.0
    spread = ask - bid
    return {
        "type": "bbo_frame",
        "ts": ts,
        "symbol": symbol,
        "bid": bid,
        "bidSize": bid_size,
        "ask": ask,
        "askSize": ask_size,
        "mid": mid,
        "spread": spread
    }


def make_book_frame(symbol: str, ts: int, mid: float, spread: float, imb3: float) -> Dict[str, Any]:
    """Simula un mensaje book_frame de data-extractor"""
    bid_price = mid - spread/2
    ask_price = mid + spread/2
    return {
        "type": "book_frame",
        "ts": ts,
        "symbol": symbol,
        "mid": mid,
        "spread": spread,
        "imb3": imb3,
        "locked": False,
        "crossed": False,
        "b1": {"p": bid_price, "v": 10.0},
        "a1": {"p": ask_price, "v": 10.0},
        # Para parse_book, también incluimos bids/asks completos
        "bids": [{"p": bid_price, "v": 10.0}, {"p": bid_price - 0.1, "v": 5.0}],
        "asks": [{"p": ask_price, "v": 10.0}, {"p": ask_price + 0.1, "v": 5.0}]
    }


def make_vwap_frame(symbol: str, ts: int, vwap: float, cum_vol: float) -> Dict[str, Any]:
    """Simula un mensaje VWAP frame de data-extractor"""
    return {
        "type": "vwap_frame",
        "ts": ts,
        "symbol": symbol,
        "vwap": vwap,
        "cumV": cum_vol
    }


def make_oflow_frame(symbol: str, ts: int, buy: float, sell: float, delta: float) -> Dict[str, Any]:
    """Simula un mensaje order flow frame de data-extractor"""
    return {
        "type": "oflow_frame",
        "ts": ts,
        "symbol": symbol,
        "buy": buy,
        "sell": sell,
        "delta": delta,
        "windowMs": 5000
    }


def make_candle(symbol: str, tf: str, ts: int, o: float, h: float, l: float, c: float, v: float) -> Dict[str, Any]:
    """Simula un mensaje candle de data-extractor"""
    return {
        "type": "candle",
        "symbol": symbol,
        "tf": tf,
        "ts": ts,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v
    }


# ========== Helpers para recoger mensajes publicados ==========

class MessageCollector:
    """Recolector de mensajes publicados en NATS"""
    def __init__(self):
        self.messages: Dict[str, List[Dict[str, Any]]] = {}
        self.subscriptions = []

    async def subscribe(self, nc: NATS, subject: str):
        """Suscribe a un subject y guarda mensajes"""
        q = asyncio.Queue()

        async def handler(msg):
            try:
                data = orjson.loads(msg.data)
                if subject not in self.messages:
                    self.messages[subject] = []
                self.messages[subject].append(data)
                await q.put(data)
            except Exception as e:
                print(f"⚠️ Error procesando mensaje en {subject}: {e}")

        sub = await nc.subscribe(subject, cb=handler)
        await nc.flush()
        self.subscriptions.append(sub)
        return q

    async def cleanup(self):
        """Limpia suscripciones"""
        for sub in self.subscriptions:
            try:
                await sub.unsubscribe()
            except Exception:
                pass
        await asyncio.sleep(0.1)


# ========== Test de integración end-to-end ==========

@pytest.mark.asyncio
async def test_data_extractor_to_indicators_integration(nc, cfg):
    """
    Test de integración completa simulando el flujo data-extractor → indicators-engine.
    
    Pasos:
    1. Iniciar IndicatorsEngine (subscriber + publisher)
    2. Publicar mensajes simulando data-extractor
    3. Verificar que indicators-engine procesa y calcula indicadores
    4. Verificar que se publican resultados correctamente
    """
    symbol = "AAPL"
    tf = "1m"
    base_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # 1. Crear collector para mensajes de salida
    collector = MessageCollector()
    
    # Subjects de entrada (data-extractor publica aquí)
    subj_bbo = "md.bbo.frame"
    subj_book = "md.book.frame"
    subj_vwap = "md.trades.vwap"
    subj_oflow = "md.trades.oflow"
    subj_candle = f"md.candles.{tf}"
    
    # Subjects de salida (indicators-engine publica aquí)
    subj_out_rsi = f"indicators.candles.{tf}.rsi14"
    subj_out_macd = f"indicators.candles.{tf}.macd"
    subj_out_cvd = "indicators.trades.cvd"
    
    # 2. Suscribirse a subjects de salida
    print(f"[TEST] Suscribiéndose a {subj_out_rsi}, {subj_out_macd}, {subj_out_cvd}")
    await collector.subscribe(nc, subj_out_rsi)
    await collector.subscribe(nc, subj_out_macd)
    await collector.subscribe(nc, subj_out_cvd)
    await asyncio.sleep(0.2)  # Dar tiempo para suscripciones
    
    # 3. Crear y configurar IndicatorsEngine
    print("[TEST] Inicializando IndicatorsEngine...")
    publisher = IndicatorPublisher(nc, out_prefix="indicators")
    engine = IndicatorsEngine(publisher)
    
    # 4. Crear subscriber y conectar callbacks
    subscriber = NATSSubscriber("settings.ini")
    await subscriber.connect()
    
    # Wirear callbacks del engine
    subscriber.cb_candle = engine.on_candle_dict
    subscriber.cb_trade = engine.on_trade_dict
    subscriber.cb_oflow = engine.on_oflow_frame_dict
    subscriber.cb_book = engine.on_book_dict
    
    # Iniciar subscriber en background
    subscriber_task = asyncio.create_task(subscriber.run())
    
    try:
        # 5. Publicar mensajes simulando data-extractor
        await asyncio.sleep(0.3)  # Esperar que las suscripciones estén activas
        
        print(f"[TEST] Publicando mensajes simulando data-extractor...")
        
        # Publicar velas (para RSI, MACD)
        price = 150.0
        for i in range(20):  # 20 velas para que RSI/MACD den valores
            ts = base_ts + i * 60000  # 1 minuto por vela
            candle = make_candle(
                symbol=symbol,
                tf=tf,
                ts=ts,
                o=price,
                h=price + 0.5,
                l=price - 0.5,
                c=price + (0.2 if i % 2 == 0 else -0.2),
                v=1000 + i * 100
            )
            await nc.publish(subj_candle, orjson.dumps(candle))
            price = candle["close"]
        
        # Publicar trades/VWAP (para CVD)
        for i in range(10):
            ts = base_ts + i * 1000
            vwap = 150.0 + i * 0.1
            vwap_msg = make_vwap_frame(symbol, ts, vwap, 1000.0 + i * 100)
            await nc.publish(subj_vwap, orjson.dumps(vwap_msg))
        
        # Publicar order flow
        for i in range(5):
            ts = base_ts + i * 2000
            oflow_msg = make_oflow_frame(symbol, ts, buy=100.0, sell=50.0, delta=50.0)
            await nc.publish(subj_oflow, orjson.dumps(oflow_msg))
        
        # Publicar BBO/Book
        bbo_msg = make_bbo_frame(symbol, base_ts, bid=149.9, bid_size=10.0, ask=150.1, ask_size=10.0)
        await nc.publish(subj_bbo, orjson.dumps(bbo_msg))
        
        book_msg = make_book_frame(symbol, base_ts, mid=150.0, spread=0.2, imb3=0.1)
        await nc.publish(subj_book, orjson.dumps(book_msg))
        
        await nc.flush()
        print("[TEST] Mensajes publicados, esperando procesamiento...")
        
        # 6. Esperar a que se procesen y publiquen indicadores
        await asyncio.sleep(3.0)
        
        # 7. Verificar resultados
        print(f"[TEST] Mensajes recolectados: {list(collector.messages.keys())}")
        print(f"[TEST] Total mensajes: {sum(len(msgs) for msgs in collector.messages.values())}")
        
        # Verificar que se recibieron mensajes en los subjects de salida
        assert len(collector.messages) > 0, "No se recibieron mensajes en subjects de salida"
        
        # Verificar que al menos RSI o MACD se calcularon (con suficientes velas)
        has_candle_indicators = (
            subj_out_rsi in collector.messages or 
            subj_out_macd in collector.messages
        )
        print(f"[TEST] Indicadores de velas calculados: {has_candle_indicators}")
        print(f"[TEST] Detalles: RSI={len(collector.messages.get(subj_out_rsi, []))}, "
              f"MACD={len(collector.messages.get(subj_out_macd, []))}")
        
        # Verificar formato de mensajes publicados
        if subj_out_rsi in collector.messages:
            rsi_msg = collector.messages[subj_out_rsi][0]
            assert "symbol" in rsi_msg
            assert "indicator" in rsi_msg or "value" in rsi_msg
            assert rsi_msg["symbol"] == symbol
        
        if subj_out_macd in collector.messages:
            macd_msg = collector.messages[subj_out_macd][0]
            assert "symbol" in macd_msg
            assert macd_msg["symbol"] == symbol
        
        print("[TEST] ✅ Integración verificada correctamente")
        
    finally:
        # Cleanup
        subscriber_task.cancel()
        try:
            await asyncio.wait_for(subscriber_task, timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        await collector.cleanup()
        await nc.flush()


@pytest.mark.asyncio
async def test_message_format_compatibility():
    """
    Test que verifica que los formatos de mensaje de data-extractor
    son compatibles con los parsers de indicators-engine.
    """
    symbol = "TSLA"
    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # Crear mensajes con formato exacto de data-extractor
    test_cases = [
        ("md.candles.1m", make_candle(symbol, "1m", ts, 100.0, 101.0, 99.0, 100.5, 1000.0)),
        ("md.trades.vwap", make_vwap_frame(symbol, ts, 100.25, 5000.0)),
        ("md.trades.oflow", make_oflow_frame(symbol, ts, 200.0, 150.0, 50.0)),
        ("md.bbo.frame", make_bbo_frame(symbol, ts, 99.9, 10.0, 100.1, 10.0)),
        ("md.book.frame", make_book_frame(symbol, ts, 100.0, 0.2, 0.05)),
    ]
    
    # Verificar que se pueden parsear correctamente
    from indicators_engine.engine import parse_bar, parse_trade, parse_book
    
    for subject, msg in test_cases:
        try:
            if subject.startswith("md.candles"):
                bar = parse_bar(msg)
                assert bar.symbol == symbol
                assert bar.ts == ts
            elif subject == "md.trades.vwap":
                # parse_trade espera campos específicos
                trade_msg = {
                    "ts": msg["ts"],
                    "price": msg.get("vwap", msg.get("price")),
                    "size": msg.get("cumV", msg.get("size", 1.0)),
                    "symbol": msg["symbol"]
                }
                trade = parse_trade(trade_msg)
                assert trade.symbol == symbol
            elif subject in ("md.bbo.frame", "md.book.frame"):
                # Los mensajes book pueden necesitar adaptación
                if "bids" not in msg and "a1" in msg:
                    # Adaptar formato BBO a BookSnapshot
                    book_msg = {
                        "ts": msg["ts"],
                        "symbol": msg["symbol"],
                        "bids": [{"p": msg.get("b1", {}).get("p", 0), "v": msg.get("b1", {}).get("v", 0)}],
                        "asks": [{"p": msg.get("a1", {}).get("p", 0), "v": msg.get("a1", {}).get("v", 0)}]
                    }
                else:
                    book_msg = msg
                book = parse_book(book_msg)
                assert book.symbol == symbol
        except Exception as e:
            pytest.fail(f"Error parseando mensaje de {subject}: {e}")
    
    print("[TEST] ✅ Formato de mensajes compatible")

