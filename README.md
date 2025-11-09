# 📊 Indicators Engine - Híbrido Python + Rust

Calculador de indicadores técnicos de alto rendimiento con arquitectura híbrida.

## 🏗️ Arquitectura

```
Python (Orquestación) ← → Rust (Alto Rendimiento)
```

- **Rust Core**: CVD, Liquidity, Heatmap, VWAP (ultra-rápido)
- **Python**: RSI, MACD, ADX, SVP, Volume Profile (I+D)
- **HybridIndicatorEngine**: Usa Rust cuando está disponible, fallback Python

## 🚀 Instalación

### Requisitos
- Python 3.12+
- Rust (cargo)
- Entorno virtual compartido en la raíz (`GLOBALTECH\.venv`)

### Pasos

```powershell
# 1. Preparar entorno compartido (desde la raíz del monorepo)
cd ..
.\setup_common_env.bat

# 2. (Opcional) Compilar e instalar módulo Rust
cd rust-core
maturin develop --release
cd ..

# 3. Verificar
# (activando previamente el entorno compartido si no lo está)
call ..\.venv\Scripts\activate
python -c "import indicators_core; print('✅ Rust instalado')"
```

## 📦 Indicadores

### Rust (Alto Rendimiento)
- **CVD**: Cumulative Volume Delta (+ sliding window optimizado)
- **Liquidity**: Métricas de liquidez del libro
- **Heatmap**: Tiles comprimidos + compresión incremental
- **VWAP**: Volume Weighted Average Price

### Python (I+D)
- **RSI**: Relative Strength Index
- **MACD**: Moving Average Convergence Divergence
- **ADX**: Average Directional Index
- **SVP**: Significant Volume Points
- **Volume Profile**: Perfil de volumen

## 💻 Uso

### Básico

```python
from indicators_engine.hybrid_engine import HybridIndicatorEngine

# Crear engine híbrido
engine = HybridIndicatorEngine()

# Verificar estado
status = engine.get_status()
print(status)
# {'rust_available': True, 'engines': {...}}

# Calcular indicadores
trade_data = {
    'ts': 1234567890,
    'price': 150.0,
    'size': 100.0,
    'symbol': 'AAPL',
    'side': 'BUY'
}

# CVD desde Rust
result = engine.calculate_cvd(trade_data)
print(f'CVD: {result.value}, source: {result.source}')
```

### Directo desde Rust

```python
from indicators_core import CVDEngine, Trade

engine = CVDEngine()
trade = Trade(1234567890, 150.0, 100.0, 'AAPL')
result = engine.on_trade(trade)
print(f'CVD: {result.cvd}')
```

## 🧹 Limpieza de Proyecto

Proyecto limpio sin archivos innecesarios:
- ✅ Un solo `.venv` en la raíz (`GLOBALTECH\.venv`)
- ✅ Sin `.venv` en `rust-core/`
- ✅ Sin archivos `.bat` innecesarios
- ✅ Documentación consolidada

## 📝 Archivos Importantes

```
indicators-engine/
├── rust-core/               # Código Rust
│   ├── src/
│   │   ├── indicators/      # Engines (CVD, Liquidity, Heatmap, VWAP)
│   │   ├── types.rs         # Tipos compartidos
│   │   └── lib.rs          # Módulo PyO3
│   └── target/release/      # Artifactos compilados
├── src/indicators_engine/   # Código Python
│   ├── engine.py           # Motor principal
│   └── hybrid_engine.py    # Orquestador híbrido
└── tests/                   # Tests
```

## ⚡ Optimizaciones Implementadas

- **Heatmap**: Tiles comprimidos (solo >= 1% del max)
- **CVD**: Agregación optimizada con chunks y sliding window
- **VWAP**: Batch processing con `on_trade_batch()`
- **Compression ratio**: Métrica de eficiencia
- **NATS Subscriber**: Integración con async-nats para JetStream
- **Visualización**: Plotly/Dash para dashboards interactivos

### Heatmap con Tiles
```python
from indicators_core import HeatmapEngine, BookSnapshot, Level

engine = HeatmapEngine()
result = engine.on_snapshot(snap)
# result.tiles: Solo tiles significativos
# result.compression_ratio: Eficiencia de compresión
```

### VWAP Batch
```python
from indicators_core import VWAPEngine, Trade

engine = VWAPEngine()
trades = [Trade(1000, 'AAPL', 150.0, 100.0), ...]
batch = engine.on_trade_batch(trades)  # Procesa todos a la vez
```

### NATS Integration (JetStream)
```python
from indicators_core import NATSConfig, NATSSubscriber

config = NATSConfig(
    url="nats://localhost:4222",
    subject="trades",
    stream_name="indicators_stream"
)
subscriber = NATSSubscriber(config)
# Async processing de mensajes NATS
```

### Visualización
```python
from indicators_engine.visualization import plot_heatmap_tiles

# Crear heatmap con tiles
metrics = engine.on_snapshot(snapshot)
fig = plot_heatmap_tiles(metrics)
fig.show()  # Abre en navegador
fig.write_html("heatmap.html")  # Guarda como HTML
```

## 🔧 Compilar vs Instalar

### Compilar Rust
```bash
cd rust-core
cargo build --release  # NO necesita .venv
```

### Instalar en Python
```bash
# Activar entorno compartido
call ..\.venv\Scripts\activate
cd rust-core
maturin develop --release  # SÍ necesita .venv activo
```

### Diferencia
- **Compilar**: `cargo` (Rust, no usa `.venv`)
- **Instalar**: `maturin` (Python, SÍ usa el `.venv` compartido)
- **Razón**: Python solo importa módulos de su path (`GLOBALTECH\.venv`)

## 🎯 Estado Actual

- ✅ **Rust compilado y funcionando**
- ✅ **Instalado en `GLOBALTECH\.venv` con maturin**
- ✅ **Hybrid Indicator Engine operativo**
- ✅ **Todas las integraciones verificadas**

## 📚 Documentación Adicional

- `pyproject.toml` - Configuración del paquete
- `GLOBALTECH\.venv/` - Entorno virtual compartido con módulo Rust instalado

## ⚙️ Configuración

### Variables de Entorno (si Python 3.13+)

```powershell
$env:PYO3_USE_ABI3_FORWARD_COMPATIBILITY='1'
```

### Recompilar Rust

```powershell
cd rust-core
maturin develop --release
```

## 🧪 Tests

```bash
pytest tests/
```

## 📊 Performance

- **Rust**: 10-100x más rápido que Python
- **Thread-safety**: DashMap para concurrencia
- **Zero-cost abstractions**: Sin overhead

## 🔄 Flujo de Datos

```
NATS (md.*) 
  ↓
IndicatorsEngine (Python)
  ↓
HybridIndicatorEngine
  ├─→ Rust (ultra-rápido) ✅
  └─→ Python (fallback)
  ↓
Publicar (indicators.*)
```
