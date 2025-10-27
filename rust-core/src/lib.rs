//! # Indicators Core
//! 
//! High-performance indicators engine core written in Rust.
//! Provides ultra-low latency calculations for critical indicators.

use pyo3::prelude::*;
use std::collections::HashMap;

// Módulos de indicadores
pub mod indicators;
pub mod types;
pub mod utils;
pub mod nats_subscriber;

// Re-exportar tipos principales para Python
pub use types::*;
pub use indicators::*;

/// Inicializar el módulo Python
#[pymodule]
fn indicators_core(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Registrar tipos de datos
    m.add_class::<Trade>()?;
    m.add_class::<Bar>()?;
    m.add_class::<Level>()?;
    m.add_class::<BookSnapshot>()?;
    
    // Registrar métricas
    m.add_class::<CVDMetrics>()?;
    m.add_class::<LiquidityMetrics>()?;
    m.add_class::<Tile>()?;
    m.add_class::<HeatmapMetrics>()?;
    m.add_class::<VWAPMetrics>()?;
    
    // Registrar engines de indicadores
    m.add_class::<CVDEngine>()?;
    m.add_class::<LiquidityEngine>()?;
    m.add_class::<HeatmapEngine>()?;
    m.add_class::<VWAPEngine>()?;
    
    // Registrar NATS
    m.add_class::<crate::nats_subscriber::NATSConfig>()?;
    m.add_class::<crate::nats_subscriber::NATSSubscriber>()?;
    
    // Registrar funciones de utilidad
    let benchmark_func = wrap_pyfunction!(benchmark_indicators, m)?;
    m.add_function(benchmark_func)?;
    
    Ok(())
}

/// Función de benchmark para comparar rendimiento
#[pyfunction]
fn benchmark_indicators(
    trades: Vec<Trade>,
    iterations: usize,
) -> PyResult<HashMap<String, f64>> {
    let mut results = HashMap::new();
    
    // Benchmark CVD
    let cvd_engine = CVDEngine::new();
    let start = std::time::Instant::now();
    
    for _ in 0..iterations {
        for trade in &trades {
            let _ = cvd_engine.on_trade(trade);
        }
    }
    
    let cvd_duration = start.elapsed().as_secs_f64();
    results.insert("cvd".to_string(), cvd_duration);
    
    Ok(results)
}

#[cfg(test)]
mod tests {
    // Tests simples para verificar que el código Rust compila y funciona.
    use super::*;

    #[test]
    fn test_trade_creation() {
        let trade = Trade::new(1234567890, 150.0, 100.0, "AAPL".to_string());
        assert_eq!(trade.ts, 1234567890);
        assert_eq!(trade.price, 150.0);
        assert_eq!(trade.size, 100.0);
        assert_eq!(trade.symbol, "AAPL");
    }

    #[test]
    fn test_cvd_engine_basic() {
        let engine = CVDEngine::new();
        
        let trade = Trade::new(1234567890, 150.0, 100.0, "AAPL".to_string());
        let result = engine.on_trade(&trade);
        
        assert!(result.is_some());
        let metrics = result.unwrap();
        assert_eq!(metrics.timestamp, 1234567890);
        assert_eq!(metrics.last_size, 100.0);
    }

    #[test]
    fn test_level_creation() {
        let level = Level::new(150.0, 100.0);
        assert_eq!(level.price, 150.0);
        assert_eq!(level.size, 100.0);
    }

    #[test]
    fn test_book_snapshot_creation() {
        let bids = vec![Level::new(149.99, 100.0), Level::new(149.98, 200.0)];
        let asks = vec![Level::new(150.01, 100.0), Level::new(150.02, 200.0)];
        
        let snapshot = BookSnapshot::new(1234567890, "AAPL".to_string(), bids, asks);
        assert_eq!(snapshot.ts, 1234567890);
        assert_eq!(snapshot.symbol, "AAPL");
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
    }

    #[test]
    fn test_liquidity_engine_basic() {
        let engine = LiquidityEngine::new();
        
        let bids = vec![Level::new(149.99, 100.0), Level::new(149.98, 200.0)];
        let asks = vec![Level::new(150.01, 100.0), Level::new(150.02, 200.0)];
        let snapshot = BookSnapshot::new(1234567890, "AAPL".to_string(), bids, asks);
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        assert_eq!(metrics.best_bid, 149.99);
        assert_eq!(metrics.best_ask, 150.01);
        assert!((metrics.spread - 0.02).abs() < 0.001);
    }
}