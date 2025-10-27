//! # CVD Engine
//! 
//! Cumulative Volume Delta calculator with ultra-low latency.

use pyo3::prelude::*;
use dashmap::DashMap;
use std::sync::Arc;
use crate::types::{Trade, CVDMetrics};

/// Engine para calcular CVD (Cumulative Volume Delta)
#[pyclass]
pub struct CVDEngine {
    // Estado por símbolo
    cvd_by_symbol: Arc<DashMap<String, f64>>,
    last_side_by_symbol: Arc<DashMap<String, String>>,
}

#[pymethods]
impl CVDEngine {
    #[new]
    pub fn new() -> Self {
        Self {
            cvd_by_symbol: Arc::new(DashMap::new()),
            last_side_by_symbol: Arc::new(DashMap::new()),
        }
    }
    
    /// Procesa un trade y calcula CVD
    pub fn on_trade(&self, trade: &Trade) -> Option<CVDMetrics> {
        // Validar datos
        if trade.price <= 0.0 || trade.size <= 0.0 {
            return None;
        }
        
        // Determinar lado del trade
        let side = self.determine_side(trade);
        
        // Actualizar CVD acumulado
        let mut cvd = self.cvd_by_symbol.get(&trade.symbol)
            .map(|entry| *entry.value())
            .unwrap_or(0.0);
        
        match side.as_str() {
            "BUY" => cvd += trade.size,
            "SELL" => cvd -= trade.size,
            _ => {} // "NA" - no cambia CVD
        }
        
        // Guardar estado
        self.cvd_by_symbol.insert(trade.symbol.clone(), cvd);
        self.last_side_by_symbol.insert(trade.symbol.clone(), side.clone());
        
        Some(CVDMetrics {
            cvd,
            last_side: side,
            last_size: trade.size,
            timestamp: trade.ts,
        })
    }
    
    /// Obtiene el CVD actual para un símbolo
    pub fn get_cvd(&self, symbol: &str) -> Option<f64> {
        self.cvd_by_symbol.get(symbol).map(|entry| *entry.value())
    }
    
    /// Resetea el CVD para un símbolo
    pub fn reset_symbol(&self, symbol: &str) {
        self.cvd_by_symbol.remove(symbol);
        self.last_side_by_symbol.remove(symbol);
    }
    
    /// Resetea todos los símbolos
    pub fn reset_all(&self) {
        self.cvd_by_symbol.clear();
        self.last_side_by_symbol.clear();
    }
    
    fn __repr__(&self) -> String {
        format!("CVDEngine(symbols={})", self.cvd_by_symbol.len())
    }
}

impl CVDEngine {
    /// Determina el lado del trade basado en el precio y contexto
    pub fn determine_side(&self, trade: &Trade) -> String {
        // Si ya viene especificado el lado, usarlo
        if let Some(side) = &trade.side {
            let side_upper = side.to_uppercase();
            if side_upper == "BUY" || side_upper == "SELL" {
                return side_upper;
            }
        }
        
        // Por ahora, usar lógica simple
        // En una implementación real, aquí usarías datos de quotes
        // para determinar si el trade fue agresivo o pasivo
        
        // Lógica temporal: alternar entre BUY y SELL
        // Esto es solo para testing - en producción usarías quotes reales
        if trade.price as u64 % 2 == 0 {
            "BUY".to_string()
        } else {
            "SELL".to_string()
        }
    }
}

// Nota: on_trade ya es público y accesible desde Python

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Trade;

    #[test]
    fn test_cvd_engine_creation() {
        let engine = CVDEngine::new();
        assert_eq!(engine.get_cvd("AAPL"), None);
    }

    #[test]
    fn test_cvd_single_trade() {
        let engine = CVDEngine::new();
        let trade = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: None,
            exchange: None,
        };
        
        let result = engine.on_trade(&trade);
        assert!(result.is_some());
        let metrics = result.unwrap();
        assert_eq!(metrics.timestamp, 1000);
        assert_eq!(metrics.last_size, 100.0);
    }

    #[test]
    fn test_cvd_accumulation() {
        let engine = CVDEngine::new();
        
        // Primero trade
        let trade1 = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: Some("BUY".to_string()),
            exchange: None,
        };
        
        engine.on_trade(&trade1);
        
        // Segundo trade
        let trade2 = Trade {
            ts: 2000,
            price: 151.0,
            size: 50.0,
            symbol: "AAPL".to_string(),
            side: Some("SELL".to_string()),
            exchange: None,
        };
        
        let result = engine.on_trade(&trade2);
        assert!(result.is_some());
        let metrics = result.unwrap();
        assert_eq!(metrics.last_side, "SELL");
        assert_eq!(metrics.last_size, 50.0);
    }

    #[test]
    fn test_cvd_invalid_trade() {
        let engine = CVDEngine::new();
        
        // Trade con precio inválido
        let trade = Trade {
            ts: 1000,
            price: -150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: None,
            exchange: None,
        };
        
        assert!(engine.on_trade(&trade).is_none());
    }

    #[test]
    fn test_cvd_zero_size() {
        let engine = CVDEngine::new();
        
        let trade = Trade {
            ts: 1000,
            price: 150.0,
            size: 0.0,
            symbol: "AAPL".to_string(),
            side: None,
            exchange: None,
        };
        
        assert!(engine.on_trade(&trade).is_none());
    }

    #[test]
    fn test_cvd_multiple_symbols() {
        let engine = CVDEngine::new();
        
        let trade1 = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: Some("BUY".to_string()),
            exchange: None,
        };
        
        let trade2 = Trade {
            ts: 1000,
            price: 3000.0,
            size: 50.0,
            symbol: "BTCUSDT".to_string(),
            side: Some("BUY".to_string()),
            exchange: None,
        };
        
        engine.on_trade(&trade1);
        engine.on_trade(&trade2);
        
        // Ambos símbolos deben tener estado independiente
        assert!(engine.get_cvd("AAPL").is_some());
        assert!(engine.get_cvd("BTCUSDT").is_some());
    }

    #[test]
    fn test_cvd_reset_symbol() {
        let engine = CVDEngine::new();
        
        let trade = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: Some("BUY".to_string()),
            exchange: None,
        };
        
        engine.on_trade(&trade);
        assert!(engine.get_cvd("AAPL").is_some());
        
        engine.reset_symbol("AAPL");
        assert_eq!(engine.get_cvd("AAPL"), None);
    }

    #[test]
    fn test_cvd_reset_all() {
        let engine = CVDEngine::new();
        
        let trade1 = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: Some("BUY".to_string()),
            exchange: None,
        };
        
        let trade2 = Trade {
            ts: 1000,
            price: 3000.0,
            size: 50.0,
            symbol: "BTCUSDT".to_string(),
            side: Some("BUY".to_string()),
            exchange: None,
        };
        
        engine.on_trade(&trade1);
        engine.on_trade(&trade2);
        
        engine.reset_all();
        
        assert_eq!(engine.get_cvd("AAPL"), None);
        assert_eq!(engine.get_cvd("BTCUSDT"), None);
    }

    #[test]
    fn test_determine_side_with_side() {
        let engine = CVDEngine::new();
        
        let trade = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: Some("BUY".to_string()),
            exchange: None,
        };
        
        let side = engine.determine_side(&trade);
        assert_eq!(side, "BUY");
    }

    #[test]
    fn test_determine_side_without_side() {
        let engine = CVDEngine::new();
        
        let trade1 = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: None,
            exchange: None,
        };
        
        let trade2 = Trade {
            ts: 1000,
            price: 151.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: None,
            exchange: None,
        };
        
        // Side alterna basado en precio
        let side1 = engine.determine_side(&trade1);
        let side2 = engine.determine_side(&trade2);
        
        assert!(side1 == "BUY" || side1 == "SELL");
        assert!(side2 == "BUY" || side2 == "SELL");
    }
}