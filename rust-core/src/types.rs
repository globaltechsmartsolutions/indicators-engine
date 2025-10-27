//! # Tipos Compartidos
//! 
//! Definiciones de tipos que se comparten entre Python y Rust.

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// Trade individual
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trade {
    #[pyo3(get, set)]
    pub ts: u64,
    #[pyo3(get, set)]
    pub price: f64,
    #[pyo3(get, set)]
    pub size: f64,
    #[pyo3(get, set)]
    pub symbol: String,
    #[pyo3(get, set)]
    pub side: Option<String>,
    #[pyo3(get, set)]
    pub exchange: Option<String>,
}

#[pymethods]
impl Trade {
    #[new]
    pub fn new(ts: u64, price: f64, size: f64, symbol: String) -> Self {
        Self {
            ts,
            price,
            size,
            symbol,
            side: None,
            exchange: None,
        }
    }
    
    fn __repr__(&self) -> String {
        format!("Trade(symbol={}, price={}, size={}, ts={})", 
                self.symbol, self.price, self.size, self.ts)
    }
}

/// Barra OHLCV
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bar {
    #[pyo3(get, set)]
    pub ts: u64,
    #[pyo3(get, set)]
    pub open: f64,
    #[pyo3(get, set)]
    pub high: f64,
    #[pyo3(get, set)]
    pub low: f64,
    #[pyo3(get, set)]
    pub close: f64,
    #[pyo3(get, set)]
    pub volume: f64,
    #[pyo3(get, set)]
    pub tf: String,
    #[pyo3(get, set)]
    pub symbol: String,
}

#[pymethods]
impl Bar {
    #[new]
    pub fn new(ts: u64, open: f64, high: f64, low: f64, close: f64, volume: f64, tf: String, symbol: String) -> Self {
        Self {
            ts,
            open,
            high,
            low,
            close,
            volume,
            tf,
            symbol,
        }
    }
    
    fn __repr__(&self) -> String {
        format!("Bar(symbol={}, tf={}, ohlc=({},{},{},{}), vol={}, ts={})", 
                self.symbol, self.tf, self.open, self.high, self.low, self.close, self.volume, self.ts)
    }
}

/// Nivel del libro de órdenes
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Level {
    #[pyo3(get, set)]
    pub price: f64,
    #[pyo3(get, set)]
    pub size: f64,
}

#[pymethods]
impl Level {
    #[new]
    pub fn new(price: f64, size: f64) -> Self {
        Self { price, size }
    }
    
    fn __repr__(&self) -> String {
        format!("Level(price={}, size={})", self.price, self.size)
    }
}

/// Snapshot del libro de órdenes
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BookSnapshot {
    #[pyo3(get, set)]
    pub ts: u64,
    #[pyo3(get, set)]
    pub symbol: String,
    #[pyo3(get, set)]
    pub bids: Vec<Level>,
    #[pyo3(get, set)]
    pub asks: Vec<Level>,
}

#[pymethods]
impl BookSnapshot {
    #[new]
    pub fn new(ts: u64, symbol: String, bids: Vec<Level>, asks: Vec<Level>) -> Self {
        Self {
            ts,
            symbol,
            bids,
            asks,
        }
    }
    
    fn __repr__(&self) -> String {
        format!("BookSnapshot(symbol={}, bids={}, asks={}, ts={})", 
                self.symbol, self.bids.len(), self.asks.len(), self.ts)
    }
}

/// Métricas de CVD
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CVDMetrics {
    #[pyo3(get, set)]
    pub cvd: f64,
    #[pyo3(get, set)]
    pub last_side: String,
    #[pyo3(get, set)]
    pub last_size: f64,
    #[pyo3(get, set)]
    pub timestamp: u64,
}

#[pymethods]
impl CVDMetrics {
    #[new]
    pub fn new(cvd: f64, last_side: String, last_size: f64, timestamp: u64) -> Self {
        Self { cvd, last_side, last_size, timestamp }
    }
    
    fn __repr__(&self) -> String {
        format!("CVDMetrics(cvd={}, side={}, size={}, ts={})",
                self.cvd, self.last_side, self.last_size, self.timestamp)
    }
}

/// Métricas de Liquidity
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiquidityMetrics {
    #[pyo3(get, set)]
    pub mid: f64,
    #[pyo3(get, set)]
    pub spread: f64,
    #[pyo3(get, set)]
    pub bids_depth: f64,
    #[pyo3(get, set)]
    pub asks_depth: f64,
    #[pyo3(get, set)]
    pub depth_imbalance: f64,
    #[pyo3(get, set)]
    pub top_imbalance: f64,
    #[pyo3(get, set)]
    pub best_bid: f64,
    #[pyo3(get, set)]
    pub best_ask: f64,
    #[pyo3(get, set)]
    pub bid1_size: f64,
    #[pyo3(get, set)]
    pub ask1_size: f64,
    #[pyo3(get, set)]
    pub levels: String,
}

#[pymethods]
impl LiquidityMetrics {
    #[new]
    pub fn new(mid: f64, spread: f64, bids_depth: f64, asks_depth: f64, depth_imbalance: f64, top_imbalance: f64,
           best_bid: f64, best_ask: f64, bid1_size: f64, ask1_size: f64, levels: String) -> Self {
        Self { mid, spread, bids_depth, asks_depth, depth_imbalance, top_imbalance,
               best_bid, best_ask, bid1_size, ask1_size, levels }
    }
    
    fn __repr__(&self) -> String {
        format!("LiquidityMetrics(mid={}, spread={}, imbalance={})",
                self.mid, self.spread, self.depth_imbalance)
    }
}

/// Tile individual (precio + tamaño comprimido)
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tile {
    #[pyo3(get, set)]
    pub price_bin: f64,
    #[pyo3(get, set)]
    pub total_size: f64,
    #[pyo3(get, set)]
    pub side: String,
}

#[pymethods]
impl Tile {
    #[new]
    pub fn new(price_bin: f64, total_size: f64, side: String) -> Self {
        Self { price_bin, total_size, side }
    }
    
    fn __repr__(&self) -> String {
        format!("Tile(price={}, size={}, side={})", self.price_bin, self.total_size, self.side)
    }
}

/// Métricas de Heatmap con tiles comprimidos
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeatmapMetrics {
    #[pyo3(get, set)]
    pub bucket_ts: u64,
    #[pyo3(get, set)]
    pub bucket_ms: u64,
    #[pyo3(get, set)]
    pub tiles: Vec<Tile>,  // ← Comprimido, NO todas las rows
    #[pyo3(get, set)]
    pub max_sz: f64,
    #[pyo3(get, set)]
    pub compression_ratio: f64,
}

#[pymethods]
impl HeatmapMetrics {
    #[new]
    fn new(bucket_ts: u64, bucket_ms: u64, tiles: Vec<Tile>, max_sz: f64, compression_ratio: f64) -> Self {
        Self { bucket_ts, bucket_ms, tiles, max_sz, compression_ratio }
    }
    
    fn __repr__(&self) -> String {
        format!("HeatmapMetrics(bucket_ts={}, bucket_ms={}, tiles={}, max_sz={}, comp={})",
                self.bucket_ts, self.bucket_ms, self.tiles.len(), self.max_sz, self.compression_ratio)
    }
}

/// Métricas de VWAP
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VWAPMetrics {
    #[pyo3(get, set)]
    pub vwap: f64,
    #[pyo3(get, set)]
    pub pv_sum: f64,
    #[pyo3(get, set)]
    pub v_sum: f64,
    #[pyo3(get, set)]
    pub session_id: Option<String>,
}

#[pymethods]
impl VWAPMetrics {
    #[new]
    pub fn new(vwap: f64, pv_sum: f64, v_sum: f64, session_id: Option<String>) -> Self {
        Self { vwap, pv_sum, v_sum, session_id }
    }
    
    fn __repr__(&self) -> String {
        format!("VWAPMetrics(vwap={}, pv_sum={}, v_sum={})",
                self.vwap, self.pv_sum, self.v_sum)
    }
}
