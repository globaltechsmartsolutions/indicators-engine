# 🚀 Guía de Instalación y Ejecución - Indicators Engine

## Instalación Rápida

### 1. Instalar Dependencias

```powershell
# Desde la raíz del monorepo GLOBALTECH
cd ..
.\setup_common_env.bat
```

### 2. Verificar Instalación

```powershell
# Verificar Rust core
python -c "import indicators_core; print('✅ Rust instalado')"

# Verificar Hybrid Engine
python -c "from indicators_engine.hybrid_engine import HybridIndicatorEngine; e = HybridIndicatorEngine(); print(e.get_status())"

# Ejecutar tests
pytest tests/unit/ -v
```

## Ejecución

### Opción 1: Script Batch (Windows)

```powershell
# Doble clic en start.bat o ejecutar:
.\start.bat
```

### Opción 2: Manual

```powershell
# Activar el entorno compartido
cd ..
call .\.venv\Scripts\activate

# Ejecutar engine
python run_engine.py
```

### Opción 3: Dashboard de Visualización

```powershell
# Activar el entorno compartido
cd ..
call .\.venv\Scripts\activate
python run_dashboard.py
```

Abierto en: http://localhost:8501

## Configuración NATS

El engine requiere un servidor NATS corriendo. Configuración en `settings.ini`:

```ini
[NATS]
url = nats://127.0.0.1:4222

[SubjectsIn]
bbo = md.bbo.frame
book = md.book.frame
candles = md.candles.>
trades_vwap = md.trades.vwap
trades_oflow = md.trades.oflow

[IndicatorsOut]
prefix = indicators
```

Para iniciar NATS localmente:

```bash
# Con Docker
docker run -p 4222:4222 nats:latest

# O descargar binario desde https://nats.io/download/
```

## Tests

```powershell
# Tests unitarios (17 tests pasando)
pytest tests/unit/ -v

# Todos los tests
pytest tests/ -v

# Con coverage
pytest tests/ --cov=indicators_engine --cov-report=html
```

## Solución de Problemas

### Rust no compila

```powershell
# Activar entorno compartido (si no lo está)
call ..\.venv\Scripts\activate

# Reinstalar maturin (opcional)
pip install maturin --upgrade

# Recompilar
cd rust-core
maturin develop --release
cd ..
```

### ModuleNotFoundError: indicators_core

```powershell
# Activar entorno compartido
call ..\.venv\Scripts\activate

# Verificar que Rust está compilado
cd rust-core
cargo build --release

# Instalar en Python
maturin develop --release
cd ..
```

### NATS connection error

```powershell
# Verificar que NATS está corriendo
# En otra terminal:
docker run -p 4222:4222 nats:latest

# O modificar settings.ini con URL correcta
```

