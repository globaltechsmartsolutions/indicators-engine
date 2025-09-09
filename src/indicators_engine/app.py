import asyncio
import orjson
from nats.js.errors import NotFoundError

from .config import Config
from .nats_io import connect_nats
from .pipelines.rsi import RsiCalc

IN_SUBJECT = "market.candles.1m"
OUT_SUBJECT = "indicators.candles.1m.rsi14"
STREAM_NAME = "INDICATORS"
STREAM_SUBJECTS = ["indicators.candles.1m.*"]

async def main():
    cfg = Config()
    nc, js = await connect_nats(cfg.NATS_URL)
    # connected_url es un ParseResult; imprime algo legible:
    try:
        print(f"✅ Conectado a NATS: {nc.connected_url.geturl()}")
    except Exception:
        print(f"✅ Conectado a NATS: {nc.connected_url}")

    # Asegura que existe el stream para publicar a JetStream
    try:
        await js.stream_info(STREAM_NAME)
    except NotFoundError:
        print(f"ℹ️ Creando stream {STREAM_NAME} con subjects={STREAM_SUBJECTS}")
        await js.add_stream(name=STREAM_NAME, subjects=STREAM_SUBJECTS)

    # RSI a 14 periodos (cuadra con el nombre del indicador)
    rsi = RsiCalc(period=14)

    async def handler(msg):
        try:
            # Log de entrada (binario → repr corta)
            print(f"📥 {msg.subject}: {msg.data!r}")

            c = orjson.loads(msg.data)

            symbol = c["symbol"]
            tf     = c["tf"]
            ts     = int(c["ts"])
            close  = float(c["close"])

            val = rsi.on_bar(symbol, tf, ts, close)
            if val is None:
                return

            out = {
                "v": 1,
                "source": "indicators-engine",
                "symbol": symbol,
                "tf": tf,
                "ts": ts,
                "indicator": "rsi14",
                "value": val,
                "id": f"{symbol}|{tf}|{ts}|rsi14",
            }

            # Publica a JetStream con de-dup por Nats-Msg-Id
            await js.publish(
                OUT_SUBJECT,
                orjson.dumps(out),
                headers={"Nats-Msg-Id": out["id"]},
            )
            print("➡️  RSI publicado:", out)

        except KeyError as e:
            print(f"❌ Falta campo en candle: {e}")
        except Exception as e:
            print("❌ Error procesando candle:", e)

    sub = await nc.subscribe(IN_SUBJECT, cb=handler)
    await nc.flush()  # asegura registro de la sub en el server
    print(f"👂 Escuchando {sub.subject}")

    # Mantén el servicio vivo
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
