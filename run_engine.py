#!/usr/bin/env python3
"""
Script de inicio para el Indicators Engine
"""

import sys
import os
from pathlib import Path

# Añadir src al path
src_dir = Path(__file__).parent / "src"
sys.path.insert(0, str(src_dir))

def main():
    """Ejecuta el Indicators Engine."""
    
    # Verificar que Rust esté instalado
    try:
        import indicators_core
        print("✅ Rust core disponible")
    except ImportError as e:
        print("⚠️ Rust core no disponible. Algunos indicadores no funcionarán.")
        print(f"   Error: {e}")
        print("   Para compilar Rust: cd rust-core && maturin develop --release")
    
    # Verificar que el engine está disponible
    try:
        from indicators_engine.nats.runner import main as run_engine
        print("✅ Indicators Engine cargado")
    except ImportError as e:
        print(f"❌ Error importando engine: {e}")
        sys.exit(1)
    
    print("🚀 Iniciando Indicators Engine...")
    print("📊 Presiona Ctrl+C para detener")
    print("-" * 50)
    
    try:
        import asyncio
        asyncio.run(run_engine())
    except KeyboardInterrupt:
        print("\n🛑 Engine detenido por el usuario")
    except Exception as e:
        print(f"❌ Error ejecutando engine: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

