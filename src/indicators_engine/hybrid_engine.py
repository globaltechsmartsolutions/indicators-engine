"""
# Hybrid Indicator Engine

Engine híbrido que usa Rust cuando está disponible, con fallback automático a Python.
"""

import logging
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass

# Configurar logging
logger = logging.getLogger(__name__)

# Intentar importar el módulo Rust
try:
    import indicators_core
    RUST_AVAILABLE = True
    logger.info("✅ Rust core disponible")
except ImportError as e:
    RUST_AVAILABLE = False
    logger.warning(f"⚠️ Rust core no disponible: {e}")
    logger.info("🔄 Usando implementación Python como fallback")

@dataclass
class IndicatorResult:
    """Resultado de un indicador"""
    value: Any
    timestamp: int
    symbol: str
    indicator_type: str
    source: str  # "rust" o "python"

class HybridIndicatorEngine:
    """
    Engine híbrido que usa Rust cuando está disponible, con fallback automático a Python.
    
    Ventajas:
    - Rendimiento máximo cuando Rust está disponible
    - Funcionalidad garantizada con fallback Python
    - Migración gradual sin romper funcionalidad
    """
    
    def __init__(self):
        self.rust_available = RUST_AVAILABLE
        
        # Inicializar engines Rust si están disponibles
        if self.rust_available:
            try:
                self.rust_cvd = indicators_core.CVDEngine()
                self.rust_liquidity = indicators_core.LiquidityEngine()
                self.rust_heatmap = indicators_core.HeatmapEngine()
                self.rust_vwap = indicators_core.VWAPEngine()
                logger.info("✅ Engines Rust inicializados")
            except Exception as e:
                logger.error(f"❌ Error inicializando engines Rust: {e}")
                self.rust_available = False
        
        # Inicializar engines Python como fallback
        self._init_python_engines()
    
    def _init_python_engines(self):
        """Inicializa los engines Python como fallback"""
        # NOTA: Los indicadores CVD, Liquidity, Heatmap y VWAP ahora son solo Rust
        # Si Rust no está disponible, estos indicadores no funcionarán
        if not self.rust_available:
            logger.warning("⚠️ Indicadores Rust no disponibles. Compila Rust con: cd rust-core && cargo build --release")
            self.python_cvd = None
            self.python_liquidity = None
            self.python_heatmap = None
            self.python_vwap = None
        else:
            # Marcadores placeholders (no se usan cuando Rust está disponible)
            self.python_cvd = None
            self.python_liquidity = None
            self.python_heatmap = None
            self.python_vwap = None
    
    def calculate_cvd(self, trade_data: Dict[str, Any]) -> Optional[IndicatorResult]:
        """
        Calcula CVD usando Rust si está disponible, Python como fallback.
        
        Args:
            trade_data: Diccionario con datos del trade
            
        Returns:
            IndicatorResult o None si hay error
        """
        if self.rust_available:
            try:
                # Convertir dict a objeto Trade de Rust
                trade = indicators_core.Trade(
                    ts=trade_data["ts"],
                    price=trade_data["price"],
                    size=trade_data["size"],
                    symbol=trade_data["symbol"],
                    side=trade_data.get("side"),
                    exchange=trade_data.get("exchange")
                )
                
                result = self.rust_cvd.on_trade(trade)
                if result:
                    return IndicatorResult(
                        value=result,
                        timestamp=trade_data["ts"],
                        symbol=trade_data["symbol"],
                        indicator_type="cvd",
                        source="rust"
                    )
            except Exception as e:
                logger.warning(f"⚠️ Error en CVD Rust: {e}, usando Python")
        
        # Fallback: CVD ahora solo está en Rust
        logger.error("❌ CVD requiere Rust. Compila con: cd rust-core && cargo build --release")
        return None
    
    def calculate_liquidity(self, book_data: Dict[str, Any]) -> Optional[IndicatorResult]:
        """
        Calcula métricas de liquidez usando Rust si está disponible.
        """
        if self.rust_available:
            try:
                # Convertir dict a objeto BookSnapshot de Rust
                snapshot = indicators_core.BookSnapshot(
                    ts=book_data["ts"],
                    symbol=book_data["symbol"],
                    bids=[indicators_core.Level(price=float(b["price"]), size=float(b["size"])) 
                          for b in book_data.get("bids", [])],
                    asks=[indicators_core.Level(price=float(a["price"]), size=float(a["size"])) 
                          for a in book_data.get("asks", [])]
                )
                
                result = self.rust_liquidity.on_snapshot(snapshot)
                if result:
                    return IndicatorResult(
                        value=result,
                        timestamp=book_data["ts"],
                        symbol=book_data["symbol"],
                        indicator_type="liquidity",
                        source="rust"
                    )
            except Exception as e:
                logger.warning(f"⚠️ Error en Liquidity Rust: {e}, usando Python")
        
        # Fallback: Liquidity ahora solo está en Rust
        logger.error("❌ Liquidity requiere Rust. Compila con: cd rust-core && cargo build --release")
        return None
    
    def calculate_heatmap(self, book_data: Dict[str, Any]) -> Optional[IndicatorResult]:
        """
        Calcula heatmap usando Rust si está disponible.
        """
        if self.rust_available:
            try:
                snapshot = indicators_core.BookSnapshot(
                    ts=book_data["ts"],
                    symbol=book_data["symbol"],
                    bids=[indicators_core.Level(price=float(b["price"]), size=float(b["size"])) 
                          for b in book_data.get("bids", [])],
                    asks=[indicators_core.Level(price=float(a["price"]), size=float(a["size"])) 
                          for a in book_data.get("asks", [])]
                )
                
                result = self.rust_heatmap.on_snapshot(snapshot)
                if result:
                    return IndicatorResult(
                        value=result,
                        timestamp=book_data["ts"],
                        symbol=book_data["symbol"],
                        indicator_type="heatmap",
                        source="rust"
                    )
            except Exception as e:
                logger.warning(f"⚠️ Error en Heatmap Rust: {e}, usando Python")
        
        # Fallback: Heatmap ahora solo está en Rust
        logger.error("❌ Heatmap requiere Rust. Compila con: cd rust-core && cargo build --release")
        return None
    
    def calculate_vwap(self, trade_data: Dict[str, Any]) -> Optional[IndicatorResult]:
        """
        Calcula VWAP usando Rust si está disponible.
        """
        if self.rust_available:
            try:
                trade = indicators_core.Trade(
                    ts=trade_data["ts"],
                    price=trade_data["price"],
                    size=trade_data["size"],
                    symbol=trade_data["symbol"],
                    side=trade_data.get("side"),
                    exchange=trade_data.get("exchange")
                )
                
                result = self.rust_vwap.on_trade(trade)
                if result:
                    return IndicatorResult(
                        value=result,
                        timestamp=trade_data["ts"],
                        symbol=trade_data["symbol"],
                        indicator_type="vwap",
                        source="rust"
                    )
            except Exception as e:
                logger.warning(f"⚠️ Error en VWAP Rust: {e}, usando Python")
        
        # Fallback: VWAP ahora solo está en Rust
        logger.error("❌ VWAP requiere Rust. Compila con: cd rust-core && cargo build --release")
        return None
    
    def get_status(self) -> Dict[str, Any]:
        """Obtiene el estado del engine híbrido"""
        return {
            "rust_available": self.rust_available,
            "engines": {
                "cvd": "rust" if self.rust_available else "python",
                "liquidity": "rust" if self.rust_available else "python",
                "heatmap": "rust" if self.rust_available else "python",
                "vwap": "rust" if self.rust_available else "python",
            }
        }
