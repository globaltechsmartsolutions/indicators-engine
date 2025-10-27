//! # Liquidity Engine
//! 
//! Order book liquidity analysis with compact data structures.

use pyo3::prelude::*;
use crate::types::{BookSnapshot, LiquidityMetrics};

/// Engine para calcular métricas de liquidez del libro de órdenes
#[pyclass]
pub struct LiquidityEngine {
    pub depth_levels: usize,
}

#[pymethods]
impl LiquidityEngine {
    #[new]
    pub fn new() -> Self {
        Self {
            depth_levels: 10,
        }
    }
    
    /// Procesa un snapshot del libro y calcula métricas de liquidez
    pub fn on_snapshot(&self, snapshot: &BookSnapshot) -> Option<LiquidityMetrics> {
        // Validar que tenemos datos
        if snapshot.bids.is_empty() || snapshot.asks.is_empty() {
            return None;
        }
        
        // Obtener mejor bid y ask
        let best_bid = snapshot.bids[0].price;
        let best_ask = snapshot.asks[0].price;  // Corregido: usar .price en lugar de .ask
        let bid1_size = snapshot.bids[0].size;
        let ask1_size = snapshot.asks[0].size;
        
        // Calcular métricas básicas
        let mid = (best_bid + best_ask) / 2.0;
        let spread = best_ask - best_bid;
        
        // Calcular profundidad hasta N niveles
        let bids_depth: f64 = snapshot.bids.iter()
            .take(self.depth_levels)
            .map(|level| level.size)
            .sum();
            
        let asks_depth: f64 = snapshot.asks.iter()
            .take(self.depth_levels)
            .map(|level| level.size)
            .sum();
        
        // Calcular imbalance
        let total_depth = bids_depth + asks_depth;
        let depth_imbalance = if total_depth > 0.0 {
            (bids_depth - asks_depth) / total_depth
        } else {
            0.0
        };
        
        // Top imbalance (solo primer nivel)
        let top_imbalance = if (bid1_size + ask1_size) > 0.0 {
            (bid1_size - ask1_size) / (bid1_size + ask1_size)
        } else {
            0.0
        };
        
        Some(LiquidityMetrics {
            mid,
            spread,
            bids_depth,
            asks_depth,
            depth_imbalance,
            top_imbalance,
            best_bid,
            best_ask,
            bid1_size,
            ask1_size,
            levels: format!("{}/{}", snapshot.bids.len(), snapshot.asks.len()),
        })
    }
    
    fn __repr__(&self) -> String {
        format!("LiquidityEngine(depth_levels={})", self.depth_levels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BookSnapshot, Level};

    fn create_test_snapshot() -> BookSnapshot {
        BookSnapshot {
            ts: 1234567890,
            symbol: "AAPL".to_string(),
            bids: vec![
                Level { price: 149.99, size: 100.0 },
                Level { price: 149.98, size: 200.0 },
                Level { price: 149.97, size: 150.0 },
            ],
            asks: vec![
                Level { price: 150.01, size: 100.0 },
                Level { price: 150.02, size: 200.0 },
                Level { price: 150.03, size: 150.0 },
            ],
        }
    }

    #[test]
    fn test_liquidity_engine_creation() {
        let engine = LiquidityEngine::new();
        assert_eq!(engine.depth_levels, 10);
    }

    #[test]
    fn test_liquidity_basic_calculation() {
        let engine = LiquidityEngine::new();
        let snapshot = create_test_snapshot();
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        assert_eq!(metrics.best_bid, 149.99);
        assert_eq!(metrics.best_ask, 150.01);
        assert!((metrics.spread - 0.02).abs() < 0.001);
        assert_eq!(metrics.mid, 150.0);
    }

    #[test]
    fn test_liquidity_empty_snapshot() {
        let engine = LiquidityEngine::new();
        let snapshot = BookSnapshot {
            ts: 1234567890,
            symbol: "AAPL".to_string(),
            bids: vec![],
            asks: vec![],
        };
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_none());
    }

    #[test]
    fn test_liquidity_depth_calculation() {
        let engine = LiquidityEngine::new();
        let snapshot = create_test_snapshot();
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        // bids_depth = 100 + 200 + 150 = 450
        assert_eq!(metrics.bids_depth, 450.0);
        // asks_depth = 100 + 200 + 150 = 450
        assert_eq!(metrics.asks_depth, 450.0);
    }

    #[test]
    fn test_liquidity_imbalance_calculation() {
        let engine = LiquidityEngine::new();
        
        // Snapshot con desbalance
        let snapshot = BookSnapshot {
            ts: 1234567890,
            symbol: "AAPL".to_string(),
            bids: vec![
                Level { price: 149.99, size: 100.0 },
                Level { price: 149.98, size: 200.0 },
            ],
            asks: vec![
                Level { price: 150.01, size: 50.0 },
            ],
        };
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        assert_eq!(metrics.bids_depth, 300.0);
        assert_eq!(metrics.asks_depth, 50.0);
        
        // depth_imbalance = (300 - 50) / 350 = 250/350 ≈ 0.714
        assert!(metrics.depth_imbalance > 0.0);
    }

    #[test]
    fn test_liquidity_top_imbalance() {
        let engine = LiquidityEngine::new();
        
        let snapshot = BookSnapshot {
            ts: 1234567890,
            symbol: "AAPL".to_string(),
            bids: vec![Level { price: 149.99, size: 100.0 }],
            asks: vec![Level { price: 150.01, size: 50.0 }],
        };
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        // top_imbalance = (100 - 50) / 150 = 50/150 = 1/3 ≈ 0.333
        assert!(metrics.top_imbalance > 0.0);
        assert_eq!(metrics.bid1_size, 100.0);
        assert_eq!(metrics.ask1_size, 50.0);
    }

    #[test]
    fn test_liquidity_levels_count() {
        let engine = LiquidityEngine::new();
        let snapshot = create_test_snapshot();
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        assert_eq!(metrics.levels, "3/3");
    }
}