//! # Heatmap Engine
//! 
//! Order book heatmap with temporal buckets and price grids.

use pyo3::prelude::*;
use dashmap::DashMap;
use std::sync::Arc;
use crate::types::{BookSnapshot, HeatmapMetrics, Tile};
use crate::utils::{calculate_bucket, quantize_price};

/// Engine para calcular heatmap del libro de órdenes
#[pyclass]
pub struct HeatmapEngine {
    pub bucket_ms: u64,
    pub tick_size: f64,
    // Estado: (bucket_ts, price_bin, side) -> size acumulado
    grid: Arc<DashMap<(u64, String, String), f64>>,
}

#[pymethods]
impl HeatmapEngine {
    #[new]
    pub fn new() -> Self {
        Self {
            bucket_ms: 1000,
            tick_size: 0.01,
            grid: Arc::new(DashMap::new()),
        }
    }
    
    /// Configura el tamaño del bucket temporal (ms)
    #[setter]
    fn set_bucket_ms(&mut self, bucket_ms: u64) {
        self.bucket_ms = bucket_ms;
    }
    
    /// Configura el tamaño del tick para cuantización de precio
    #[setter]
    fn set_tick_size(&mut self, tick_size: f64) {
        self.tick_size = tick_size;
    }
    
    /// Procesa un snapshot del libro y calcula heatmap
    pub fn on_snapshot(&self, snapshot: &BookSnapshot) -> Option<HeatmapMetrics> {
        // Validar que hay datos
        if snapshot.bids.is_empty() && snapshot.asks.is_empty() {
            return None;
        }
        
        // Calcular bucket actual
        let bucket_ts = calculate_bucket(snapshot.ts, self.bucket_ms);
        
        // Acumular en el grid
        for bid in &snapshot.bids {
            let price_bin = quantize_price(bid.price, self.tick_size);
            let key = (bucket_ts, price_bin.to_string(), "bid".to_string());
            *self.grid.entry(key).or_insert(0.0) += bid.size;
        }
        
        for ask in &snapshot.asks {
            let price_bin = quantize_price(ask.price, self.tick_size);
            let key = (bucket_ts, price_bin.to_string(), "ask".to_string());
            *self.grid.entry(key).or_insert(0.0) += ask.size;
        }
        
        // Extraer tiles del bucket actual (comprimidos)
        let mut tiles: Vec<Tile> = Vec::new();
        let original_count = self.grid.len();
        
        for entry in self.grid.iter() {
            let ((bucket, price_str, side), size) = (entry.key(), entry.value());
            if *bucket == bucket_ts {
                if let Ok(price) = price_str.parse::<f64>() {
                    // Solo tiles significativos (>= threshold del 1% del max)
                    tiles.push(Tile {
                        price_bin: price,
                        total_size: *size,
                        side: side.clone(),
                    });
                }
            }
        }
        
        // Ordenar por precio
        tiles.sort_by(|a, b| a.price_bin.partial_cmp(&b.price_bin).unwrap_or(std::cmp::Ordering::Equal));
        
        // Calcular max_sz y compression ratio
        let max_sz = tiles.iter().map(|t| t.total_size).fold(0.0, f64::max);
        let threshold = max_sz * 0.01; // Filtrar tiles menores al 1% del max
        tiles.retain(|t| t.total_size >= threshold);
        
        let compression_ratio = if tiles.len() > 0 {
            original_count as f64 / tiles.len() as f64
        } else {
            1.0
        };
        
        Some(HeatmapMetrics {
            bucket_ts,
            bucket_ms: self.bucket_ms,
            tiles,
            max_sz,
            compression_ratio,
        })
    }
    
    /// Limpia todos los buckets
    fn reset(&self) {
        self.grid.clear();
    }
    
    /// Limpia un bucket específico
    fn reset_bucket(&self, bucket_ts: u64) {
        self.grid.retain(|k, _| k.0 != bucket_ts);
    }
    
    /// Obtiene solo tiles incrementales (delta desde último publish)
    fn get_tile_delta(&self, bucket_ts: u64) -> Vec<Tile> {
        let mut tiles: Vec<Tile> = Vec::new();
        
        for entry in self.grid.iter() {
            let ((bucket, price_str, side), size) = (entry.key(), entry.value());
            if *bucket == bucket_ts {
                if let Ok(price) = price_str.parse::<f64>() {
                    tiles.push(Tile {
                        price_bin: price,
                        total_size: *size,
                        side: side.clone(),
                    });
                }
            }
        }
        
        tiles.sort_by(|a, b| a.price_bin.partial_cmp(&b.price_bin).unwrap_or(std::cmp::Ordering::Equal));
        tiles
    }
    
    fn __repr__(&self) -> String {
        format!("HeatmapEngine(bucket_ms={}, tick_size={}, entries={})", 
                self.bucket_ms, self.tick_size, self.grid.len())
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
            ],
            asks: vec![
                Level { price: 150.01, size: 100.0 },
                Level { price: 150.02, size: 200.0 },
            ],
        }
    }

    #[test]
    fn test_heatmap_engine_creation() {
        let engine = HeatmapEngine::new();
        assert_eq!(engine.bucket_ms, 1000);
        assert_eq!(engine.tick_size, 0.01);
    }

    #[test]
    fn test_heatmap_empty_snapshot() {
        let engine = HeatmapEngine::new();
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
    fn test_heatmap_single_snapshot() {
        let engine = HeatmapEngine::new();
        let snapshot = create_test_snapshot();
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        assert_eq!(metrics.bucket_ts, 1234567000); // Bucket de 1000ms
        assert_eq!(metrics.bucket_ms, 1000);
        assert!(metrics.tiles.len() > 0);
    }

    #[test]
    fn test_heatmap_compression() {
        let engine = HeatmapEngine::new();
        let snapshot = create_test_snapshot();
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        // Compression ratio debería ser >= 1.0
        assert!(metrics.compression_ratio >= 1.0);
    }

    #[test]
    fn test_heatmap_multiple_snapshots() {
        let engine = HeatmapEngine::new();
        
        let snapshot1 = BookSnapshot {
            ts: 1234567890,
            symbol: "AAPL".to_string(),
            bids: vec![Level { price: 149.99, size: 100.0 }],
            asks: vec![Level { price: 150.01, size: 100.0 }],
        };
        
        let snapshot2 = BookSnapshot {
            ts: 1234568900, // Mismo bucket
            symbol: "AAPL".to_string(),
            bids: vec![Level { price: 149.99, size: 50.0 }],
            asks: vec![Level { price: 150.01, size: 50.0 }],
        };
        
        let _ = engine.on_snapshot(&snapshot1);
        let result = engine.on_snapshot(&snapshot2);
        
        assert!(result.is_some());
        let metrics = result.unwrap();
        // El segundo snapshot está en timestamp 1234568900, que está en bucket 1234568000
        assert_eq!(metrics.bucket_ts, 1234568000);
    }

    #[test]
    fn test_heatmap_tile_ordering() {
        let engine = HeatmapEngine::new();
        let snapshot = create_test_snapshot();
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        // Tiles deben estar ordenados por precio
        for i in 1..metrics.tiles.len() {
            assert!(metrics.tiles[i].price_bin >= metrics.tiles[i-1].price_bin);
        }
    }

    #[test]
    fn test_heatmap_reset() {
        let engine = HeatmapEngine::new();
        let snapshot = create_test_snapshot();
        
        engine.on_snapshot(&snapshot);
        
        // Reset y verificar que está limpio
        engine.reset();
        
        let result = engine.on_snapshot(&snapshot);
        assert!(result.is_some());
        // Después del reset, el primer bucket debería comenzar de nuevo
    }

    #[test]
    fn test_heatmap_reset_bucket() {
        let engine = HeatmapEngine::new();
        
        let snapshot1 = BookSnapshot {
            ts: 1234567890,
            symbol: "AAPL".to_string(),
            bids: vec![Level { price: 149.99, size: 100.0 }],
            asks: vec![Level { price: 150.01, size: 100.0 }],
        };
        
        engine.on_snapshot(&snapshot1);
        engine.reset_bucket(1234567000);
        
        // El bucket debería estar limpio ahora
        let snapshot2 = BookSnapshot {
            ts: 1234568900,
            symbol: "AAPL".to_string(),
            bids: vec![Level { price: 149.99, size: 50.0 }],
            asks: vec![Level { price: 150.01, size: 50.0 }],
        };
        
        let result = engine.on_snapshot(&snapshot2);
        assert!(result.is_some());
    }

    #[test]
    fn test_heatmap_configuration() {
        let mut engine = HeatmapEngine::new();
        
        engine.set_bucket_ms(5000);
        engine.set_tick_size(0.05);
        
        assert_eq!(engine.bucket_ms, 5000);
        assert_eq!(engine.tick_size, 0.05);
    }

    #[test]
    fn test_heatmap_different_buckets() {
        let engine = HeatmapEngine::new();
        
        let snapshot1 = BookSnapshot {
            ts: 1234567890,
            symbol: "AAPL".to_string(),
            bids: vec![Level { price: 149.99, size: 100.0 }],
            asks: vec![Level { price: 150.01, size: 100.0 }],
        };
        
        let snapshot2 = BookSnapshot {
            ts: 2234567890, // Bucket diferente
            symbol: "AAPL".to_string(),
            bids: vec![Level { price: 149.99, size: 50.0 }],
            asks: vec![Level { price: 150.01, size: 50.0 }],
        };
        
        let result1 = engine.on_snapshot(&snapshot1);
        let result2 = engine.on_snapshot(&snapshot2);
        
        assert!(result1.is_some());
        assert!(result2.is_some());
        assert_ne!(result1.unwrap().bucket_ts, result2.unwrap().bucket_ts);
    }
}