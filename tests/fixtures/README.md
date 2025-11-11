# Generación y Uso de Fixtures Reales

Este directorio contiene herramientas para capturar datos reales del sistema y usarlos como referencia en tests de regresión.

## Flujo de Trabajo

### 1. Capturar Fixtures Reales

Con el stack completo corriendo (fake extractor + engine), ejecuta:

```bash
python tools/capture_fixtures.py \
    --seconds 30 \
    --pattern "md.>" \
    --output tests/fixtures/live_session_20241110.jsonl
```

Esto captura todos los eventos de `md.*` durante 30 segundos y los guarda en formato JSONL.

**Parámetros:**
- `--seconds`: Tiempo de captura (default: 30)
- `--pattern`: Patrón de subjects NATS (default: `md.>`)
- `--limit`: Límite máximo de mensajes (default: 1000)
- `--output`: Ruta donde guardar los fixtures

### 2. Generar Golden Outputs

Una vez capturados los fixtures, genera los outputs esperados ejecutando el engine sobre ellos:

```bash
python tools/generate_golden_outputs.py \
    tests/fixtures/live_session_20241110.jsonl \
    tests/fixtures/golden_outputs_20241110.jsonl
```

Esto procesa todos los eventos capturados y genera los indicadores que deberían publicarse, guardándolos como "golden snapshots".

### 3. Ejecutar Tests de Regresión

Los tests en `tests/integration/test_golden_outputs.py` comparan los outputs actuales del engine con los golden snapshots:

```bash
pytest tests/integration/test_golden_outputs.py -v
```

**Nota:** Los tests se saltan automáticamente si no existen los fixtures o golden outputs. Esto permite que el CI funcione sin requerir fixtures pre-generados.

## Estructura de Fixtures

### Fixtures Capturados (`live_session_*.jsonl`)

Cada línea es un JSON con:
```json
{
  "subject": "md.trades.vwap",
  "ts_iso": "2025-11-10T19:00:00+00:00",
  "payload": {
    "type": "vwap_frame",
    "ts": 1762800000000,
    "symbol": "AAPL",
    "vwap": 125.00,
    "price": 126.88,
    "cumV": 1000.0
  }
}
```

### Golden Outputs (`golden_outputs_*.jsonl`)

Cada línea es un output esperado:
```json
{
  "type": "trades",
  "indicator": "vwap",
  "symbol": "AAPL",
  "payload": {
    "ts": 1762800000000,
    "vwap": 125.00,
    "last_price": 126.88,
    "deviation_abs": 1.88,
    "deviation_pct": 1.504
  }
}
```

## Validaciones Automáticas

Los tests verifican:

1. **Integridad de payloads**: Todos los campos esperados están presentes
2. **Rangos válidos**: `depth_imbalance` ∈ [-1, 1], `spread` ≥ 0, etc.
3. **Consistencia**: Los outputs actuales coinciden con los golden (con tolerancia para floats)

## Actualizar Golden Outputs

Si cambias la lógica del engine y los outputs legítimamente cambian:

1. Regenera los golden outputs con el nuevo código
2. Revisa los cambios manualmente
3. Si son correctos, actualiza el golden snapshot en el repo

```bash
python tools/generate_golden_outputs.py \
    tests/fixtures/live_session_20241110.jsonl \
    tests/fixtures/golden_outputs_20241110.jsonl
git add tests/fixtures/golden_outputs_20241110.jsonl
git commit -m "chore: update golden outputs after engine changes"
```

## Mejores Prácticas

- **Captura sesiones representativas**: Incluye diferentes símbolos, condiciones de mercado, etc.
- **Versiona los fixtures**: Usa nombres con fecha (`live_session_20241110.jsonl`)
- **No commitees fixtures grandes**: Usa `.gitignore` para excluir fixtures > 1MB
- **Regenera golden outputs periódicamente**: Especialmente después de cambios importantes

