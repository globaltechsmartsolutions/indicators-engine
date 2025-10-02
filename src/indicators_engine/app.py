# indicators_engine/app.py
import asyncio
import os
import orjson
from nats.js.errors import NotFoundError

from .config import Config
from .nats_io import connect_nats

# ==== Pipelines (usa tus clases reales) ====
from .pipelines.rsi import RsiCalc
from .pipelines.macd import MacdCalc
from .pipelines.adx import AdxCalc
from .pipelines.vwap import VwapCalc
from .pipelines.poc import PocCalc
from .pipelines.svp import SvpCalc
from .pipelines.volume_profile import VolumeProfileCalc

from .pipelines.cvd import CvdCalc
from .pipelines.orderflow import OrderFlowCalc

DEFAULT_TICK_SIZE = float(os.getenv("DEFAULT_TICK_SIZE", "0.01"))

# ==== Subjects de entrada (de tu extractor) ====
SUB_CANDLES   = "md.candles.>"        # ej. md.candles.1m / 5m / ...
SUB_BBO       = "md.bbo.frame"        # frame con bid/ask/mid/spread
SUB_TVWAP     = "md.trades.vwap"      # frames vwap_frame del extractor
SUB_OFLOW     = "md.trades.oflow"     # frames oflow_frame del extractor
SUB_BOOK      = "md.book.frame"       # frame con mid/spread/imb3 y top1
SUB_BOOK_L2   = "md.book.l2.frame"    # frame L2 (topN)

# ==== Stream/subjects de salida (si publicas indicadores) ====
STREAM_NAME = "INDICATORS"
STREAM_SUBJECTS = ["indicators.>"]


# ====================== RENDER EN CONSOLA ======================
class LiveRenderer:
    """
    Muestra 1 línea por (symbol, tf, indicador).
    Limpia la consola completa en cada refresco para evitar que se acumulen bloques.
    """
    def __init__(self, fps: float = 4.0):
        self.latest: dict[tuple[str, str, str], dict] = {}
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._interval = max(0.05, 1.0 / fps)

    async def update(self, symbol: str, indicator: str, value, ts: int, tf: str | None = None):
        key = (symbol, tf or "-", indicator)
        async with self._lock:
            self.latest[key] = {"value": value, "ts": ts}

    async def run(self):
        try:
            while not self._stop.is_set():
                await self._draw()
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            pass

    async def _draw(self):
        async with self._lock:
            items = sorted(self.latest.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2]))

        # ✅ Limpia pantalla (Windows = cls, Linux/Mac = clear)
        os.system("cls" if os.name == "nt" else "clear")

        out_lines = []
        out_lines.append("🧮 indicators-engine — vista en vivo (1 línea por indicador)\n")

        if not items:
            out_lines.append("(esperando datos…)\n")
            print("".join(out_lines), end="", flush=True)
            return

        last_sym, last_tf = None, None
        for (sym, tf, ind), payload in items:
            if sym != last_sym:
                out_lines.append(f"\n┌─ {sym} ───────────────────────────────────────────────\n")
                last_sym = sym
                last_tf = None
            if tf != last_tf:
                out_lines.append(f"│  tf: {tf}\n")
                last_tf = tf

            vtxt = self._fmt_value(ind, payload.get("value"))
            out_lines.append(f"│   • {ind:<18} {vtxt}\n")

        print("".join(out_lines), end="", flush=True)

    def _fmt_value(self, ind: str, v):
        try:
            if isinstance(v, dict):
                if {"macd", "signal", "hist"} <= set(v.keys()):
                    return f"macd={self._n(v['macd'])}  sig={self._n(v['signal'])}  hist={self._n(v['hist'])}"
                parts = []
                for k in sorted(v.keys()):
                    val = v[k]
                    parts.append(f"{k}={self._n(val) if isinstance(val,(int,float)) else val}")
                return "  ".join(parts)
            elif isinstance(v, (int, float)):
                return self._n(v)
            else:
                return str(v)
        except Exception:
            return str(v)

    @staticmethod
    def _n(x):
        try:
            ax = abs(float(x))
            if ax >= 1_000_000: return f"{x/1_000_000:.2f}M"
            if ax >= 1_000:     return f"{x/1_000:.2f}k"
            if ax >= 100:       return f"{x:.2f}"
            if ax >= 1:         return f"{x:.3f}"
            return f"{x:.5f}"
        except Exception:
            return str(x)


# ====================== HELPERS ======================
def _clean_symbol(sym: str) -> str:
    if not isinstance(sym, str):
        return sym
    i = sym.find("{=")
    return sym[:i] if i != -1 else sym


# ====================== MAIN ======================
async def main():
    cfg = Config()
    nc, js = await connect_nats(cfg.NATS_URL)
    try:
        print(f"✅ Conectado a NATS: {nc.connected_url.geturl()}")
    except Exception:
        print(f"✅ Conectado a NATS: {nc.connected_url}")

    # (Opcional) Asegurar stream de salida si publicas algo
    try:
        await js.stream_info(STREAM_NAME)
    except NotFoundError:
        print(f"ℹ️ Creando stream {STREAM_NAME} con subjects={STREAM_SUBJECTS}")
        await js.add_stream(name=STREAM_NAME, subjects=STREAM_SUBJECTS)

    # ==== Instancias de indicadores sobre velas ====
    rsi10 = RsiCalc(period=10)
    rsi14 = RsiCalc(period=14)
    macd  = MacdCalc()
    adx14 = AdxCalc(period=14)
    vwap_bar = VwapCalc()
    poc   = PocCalc(tick_size=DEFAULT_TICK_SIZE)
    svp   = SvpCalc(tick_size=DEFAULT_TICK_SIZE)
    vp    = VolumeProfileCalc(tick_size=DEFAULT_TICK_SIZE)

    # Trades / orderflow “en vivo”
    cvd   = CvdCalc()
    oflow = OrderFlowCalc()

    # Renderer
    live = LiveRenderer(fps=4.0)
    render_task = asyncio.create_task(live.run())

    # ====================== HANDLERS ======================

    # md.candles.<tf>  -> calculamos todos los indicadores de barra
    async def on_candle(msg):
        try:
            c = orjson.loads(msg.data)
            symbol = _clean_symbol(c.get("symbol") or "")
            ts     = int(c.get("ts") or 0)
            tf     = c.get("tf") or msg.subject.split(".")[2]  # md.candles.<tf>
            o      = float(c.get("open",  c.get("close", 0.0)))
            h      = float(c.get("high",  c.get("close", 0.0)))
            l      = float(c.get("low",   c.get("close", 0.0)))
            close  = float(c.get("close", 0.0))
            vol    = float(c.get("volume", 0.0))

            # RSI(10)
            try:
                v = rsi10.on_bar(symbol, tf, ts, close)
                if v is not None:
                    await live.update(symbol, "rsi10", float(v), ts, tf)
            except Exception as e:
                print("⚠️ rsi10:", e)

            # RSI(14)
            try:
                v = rsi14.on_bar(symbol, tf, ts, close)
                if v is not None:
                    await live.update(symbol, "rsi14", float(v), ts, tf)
            except Exception as e:
                print("⚠️ rsi14:", e)

            # MACD
            try:
                v = macd.on_bar(symbol, tf, ts, close)
                if v:
                    await live.update(symbol, "macd", v, ts, tf)  # dict macd/signal/hist
            except Exception as e:
                print("⚠️ macd:", e)

            # ADX(14)
            try:
                v = adx14.on_bar(symbol, tf, ts, h, l, close)
                if v:
                    await live.update(symbol, "adx14", v, ts, tf)
            except Exception as e:
                print("⚠️ adx14:", e)

            # VWAP sobre barras (si tu clase lo soporta)
            if hasattr(vwap_bar, "on_bar"):
                try:
                    v = vwap_bar.on_bar(symbol, tf, ts, o, h, l, close, vol)
                    if v is not None:
                        await live.update(symbol, "vwap_bar", float(v), ts, tf)
                except Exception as e:
                    print("⚠️ vwap_bar:", e)

            # POC / SVP / VolumeProfile (si devuelven algo)
            if hasattr(poc, "on_bar"):
                try:
                    v = poc.on_bar(symbol, tf, ts, close, vol)
                    if v:
                        await live.update(symbol, "poc", v, ts, tf)
                except Exception as e:
                    print("⚠️ poc:", e)

            if hasattr(svp, "on_bar"):
                try:
                    v = svp.on_bar(symbol, tf, ts, close, vol)
                    if v:
                        await live.update(symbol, "svp", v, ts, tf)
                except Exception as e:
                    print("⚠️ svp:", e)

            if hasattr(vp, "on_bar"):
                try:
                    v = vp.on_bar(symbol, tf, ts, close, vol)
                    if v:
                        await live.update(symbol, "volume_profile", v, ts, tf)
                except Exception as e:
                    print("⚠️ volume_profile:", e)

            # (Opcional) CVD/OrderFlow por trade: suscribirías a md.trades.* si lo necesitas calcular aquí.
        except Exception as e:
            print("❌ [candle] error:", e)

    # md.trades.vwap  -> muestra vwap acumulado por el extractor (frame ligero)
    async def on_trade_vwap(msg):
        try:
            f = orjson.loads(msg.data)
            symbol = _clean_symbol(f.get("symbol") or "")
            ts     = int(f.get("ts") or 0)
            vwap   = float(f.get("vwap", 0.0))
            cumv   = float(f.get("cumV", 0.0))
            await live.update(symbol, "vwap", {"vwap": vwap, "cumV": cumv}, ts, tf="-")
        except Exception as e:
            print("❌ [vwap] error:", e)

    # md.trades.oflow -> muestra orderflow ventana 5s computed-by-extractor (frame)
    async def on_trade_oflow(msg):
        try:
            f = orjson.loads(msg.data)
            symbol = _clean_symbol(f.get("symbol") or "")
            ts     = int(f.get("ts") or 0)
            buy    = float(f.get("buy", 0.0))
            sell   = float(f.get("sell", 0.0))
            delta  = float(f.get("delta", 0.0))
            await live.update(symbol, "oflow(5s)", {"buy": buy, "sell": sell, "Δ": delta}, ts, tf="-")
        except Exception as e:
            print("❌ [oflow] error:", e)

    # md.bbo.frame -> si tienes on_quote en OrderFlowCalc, actualiza “orderflow(bbo)”
    async def on_bbo_frame(msg):
        try:
            q = orjson.loads(msg.data)
            symbol = _clean_symbol(q.get("symbol") or "")
            ts     = int(q.get("ts") or 0)
            bid    = float(q.get("bid", 0.0))
            ask    = float(q.get("ask", 0.0))
            bid_sz = float(q.get("bidSize", 0.0))
            ask_sz = float(q.get("askSize", 0.0))
            if hasattr(oflow, "on_quote"):
                try:
                    v = oflow.on_quote(symbol, ts, bid, bid_sz, ask, ask_sz)
                    if v:
                        await live.update(symbol, "orderflow(bbo)", v, ts, tf="-")
                except Exception as e:
                    print("⚠️ orderflow(bbo):", e)
        except Exception as e:
            print("❌ [bbo_frame] error:", e)

    # md.book.frame -> línea “book” con mid, spread, imb3
    async def on_book_frame(msg):
        try:
            f = orjson.loads(msg.data)
            symbol = _clean_symbol(f.get("symbol") or "")
            ts     = int(f.get("ts") or 0)
            mid    = float(f.get("mid", 0.0))
            spr    = float(f.get("spread", 0.0))
            imb3   = float(f.get("imb3", 0.0))
            await live.update(symbol, "book", {"imb3": imb3, "mid": mid, "spread": spr}, ts, tf="-")
        except Exception as e:
            print("❌ [book_frame] error:", e)

    # md.book.l2.frame -> línea “depth” compacta (niveles totales y top1)
    async def on_book_l2(msg):
        try:
            f = orjson.loads(msg.data)
            symbol = _clean_symbol(f.get("symbol") or "")
            ts     = int(f.get("ts") or 0)
            bids   = f.get("bids") or []
            asks   = f.get("asks") or []
            b1 = bids[0] if bids else {}
            a1 = asks[0] if asks else {}
            val = {
                "levels": f"{len(bids)}/{len(asks)}",
                "b1": f"{b1.get('p','-')}({b1.get('v','-')})" if b1 else "-",
                "a1": f"{a1.get('p','-')}({a1.get('v','-')})" if a1 else "-",
            }
            await live.update(symbol, "depth", val, ts, tf="-")
        except Exception as e:
            print("❌ [book_l2] error:", e)

    # ====================== SUBS ======================
    await nc.subscribe(SUB_CANDLES, cb=on_candle)
    await nc.subscribe(SUB_TVWAP,   cb=on_trade_vwap)
    await nc.subscribe(SUB_OFLOW,   cb=on_trade_oflow)
    await nc.subscribe(SUB_BBO,     cb=on_bbo_frame)
    await nc.subscribe(SUB_BOOK,    cb=on_book_frame)
    await nc.subscribe(SUB_BOOK_L2, cb=on_book_l2)

    await nc.flush()
    print(f"👂 Escuchando: {SUB_CANDLES}, {SUB_TVWAP}, {SUB_OFLOW}, {SUB_BBO}, {SUB_BOOK}, {SUB_BOOK_L2}")

    try:
        await asyncio.Future()
    finally:
        render_task.cancel()
        try:
            await render_task
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
