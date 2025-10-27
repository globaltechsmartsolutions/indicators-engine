//! # VWAP Engine
//! 
//! Volume Weighted Average Price calculator with session management.

use pyo3::prelude::*;
use dashmap::DashMap;
use std::sync::Arc;
use crate::types::{Trade, Bar, VWAPMetrics};
use crate::utils::safe_div;

/// Engine para calcular VWAP por símbolo
#[pyclass]
pub struct VWAPEngine {
    // Estado por símbolo: (symbol, session_id) -> (pv_sum, v_sum)
    state: Arc<DashMap<(String, Option<String>), (f64, f64)>>,
}

#[pymethods]
impl VWAPEngine {
    #[new]
    pub fn new() -> Self {
        Self {
            state: Arc::new(DashMap::new()),
        }
    }
    
    /// Procesa un trade y actualiza VWAP
    pub fn on_trade(&self, trade: &Trade) -> Option<VWAPMetrics> {
        // Validar datos
        if trade.price <= 0.0 || trade.size <= 0.0 {
            return None;
        }
        
        let key = (trade.symbol.clone(), None);
        
        // Actualizar estado usando entry API
        let entry = self.state.entry(key);
        let (pv_sum, v_sum) = match entry {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                let (pv, v) = *e.get();
                let new_pv = pv + (trade.price * trade.size);
                let new_v = v + trade.size;
                e.insert((new_pv, new_v));
                (new_pv, new_v)
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let pv = trade.price * trade.size;
                let v = trade.size;
                e.insert((pv, v));
                (pv, v)
            }
        };
        
        let vwap = safe_div(pv_sum, v_sum);
        
        Some(VWAPMetrics {
            vwap,
            pv_sum,
            v_sum,
            session_id: None,
        })
    }
    
    /// Procesa una barra y actualiza VWAP usando typical price
    fn on_bar(&self, bar: &Bar) -> Option<VWAPMetrics> {
        // Validar datos
        if bar.volume <= 0.0 {
            return None;
        }
        
        // Typical price = (high + low + close) / 3
        let tp = (bar.high + bar.low + bar.close) / 3.0;
        
        let key = (bar.symbol.clone(), None);
        
        // Actualizar estado usando entry API
        let entry = self.state.entry(key);
        let (pv_sum, v_sum) = match entry {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                let (pv, v) = *e.get();
                let new_pv = pv + (tp * bar.volume);
                let new_v = v + bar.volume;
                e.insert((new_pv, new_v));
                (new_pv, new_v)
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let pv = tp * bar.volume;
                let v = bar.volume;
                e.insert((pv, v));
                (pv, v)
            }
        };
        
        let vwap = safe_div(pv_sum, v_sum);
        
        Some(VWAPMetrics {
            vwap,
            pv_sum,
            v_sum,
            session_id: None,
        })
    }
    
    /// Obtiene el VWAP actual para un símbolo
    pub fn get_vwap(&self, symbol: &str) -> Option<f64> {
        let key = (symbol.to_string(), None);
        self.state.get(&key).map(|entry| {
            let (pv_sum, v_sum) = *entry.value();
            safe_div(pv_sum, v_sum)
        })
    }
    
    /// Resetea el VWAP para un símbolo
    pub fn reset_symbol(&self, symbol: &str) {
        let key = (symbol.to_string(), None);
        self.state.remove(&key);
    }
    
    /// Resetea todos los símbolos
    pub fn reset_all(&self) {
        self.state.clear();
    }
    
    /// Calcula VWAP en batch usando Polars (mucho más rápido)
    pub fn on_trade_batch(&self, trades: Vec<Trade>) -> Vec<VWAPMetrics> {
        if trades.is_empty() {
            return Vec::new();
        }
        
        // Calcular PV y V acumulado (implementación manual por ahora)
        // TODO: Usar cumsum cuando esté disponible en la versión de Polars
        let mut pv_cumsum = 0.0;
        let mut v_cumsum = 0.0;
        let mut results = Vec::new();
        
        for trade in trades {
            pv_cumsum += trade.price * trade.size;
            v_cumsum += trade.size;
            let vwap = safe_div(pv_cumsum, v_cumsum);
            
            results.push(VWAPMetrics {
                vwap,
                pv_sum: pv_cumsum,
                v_sum: v_cumsum,
                session_id: None,
            });
        }
        
        return results;
    }
    
    fn __repr__(&self) -> String {
        format!("VWAPEngine(symbols={})", self.state.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Trade, Bar};

    #[test]
    fn test_vwap_engine_creation() {
        let engine = VWAPEngine::new();
        assert_eq!(engine.get_vwap("AAPL"), None);
    }

    #[test]
    fn test_vwap_single_trade() {
        let engine = VWAPEngine::new();
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
        assert_eq!(metrics.vwap, 150.0);
        assert_eq!(metrics.pv_sum, 15000.0); // 150 * 100
        assert_eq!(metrics.v_sum, 100.0);
    }

    #[test]
    fn test_vwap_accumulation() {
        let engine = VWAPEngine::new();
        
        let trade1 = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: None,
            exchange: None,
        };
        
        let trade2 = Trade {
            ts: 2000,
            price: 151.0,
            size: 50.0,
            symbol: "AAPL".to_string(),
            side: None,
            exchange: None,
        };
        
        engine.on_trade(&trade1);
        let result = engine.on_trade(&trade2);
        
        assert!(result.is_some());
        let metrics = result.unwrap();
        
        // VWAP = (150*100 + 151*50) / (100 + 50) = (15000 + 7550) / 150 = 150.33...
        let expected_vwap = (150.0 * 100.0 + 151.0 * 50.0) / 150.0;
        assert!((metrics.vwap - expected_vwap).abs() < 0.01);
    }

    #[test]
    fn test_vwap_multiple_symbols() {
        let engine = VWAPEngine::new();
        
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
            price: 3000.0,
            size: 1.0,
            symbol: "BTCUSDT".to_string(),
            side: None,
            exchange: None,
        };
        
        engine.on_trade(&trade1);
        engine.on_trade(&trade2);
        
        let vwap_aapl = engine.get_vwap("AAPL");
        let vwap_btc = engine.get_vwap("BTCUSDT");
        
        assert_eq!(vwap_aapl, Some(150.0));
        assert_eq!(vwap_btc, Some(3000.0));
    }

    #[test]
    fn test_vwap_invalid_trade() {
        let engine = VWAPEngine::new();
        
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
    fn test_vwap_on_bar() {
        let engine = VWAPEngine::new();
        
        let bar = Bar {
            ts: 1000,
            open: 149.0,
            high: 151.0,
            low: 148.0,
            close: 150.0,
            volume: 1000.0,
            tf: "1m".to_string(),
            symbol: "AAPL".to_string(),
        };
        
        let result = engine.on_bar(&bar);
        assert!(result.is_some());
        
        let metrics = result.unwrap();
        // Typical price = (151 + 148 + 150) / 3 = 149.67
        let expected_tp = (151.0 + 148.0 + 150.0) / 3.0;
        assert!((metrics.vwap - expected_tp).abs() < 0.01);
    }

    #[test]
    fn test_vwap_batch_processing() {
        let engine = VWAPEngine::new();
        
        let trades = vec![
            Trade { ts: 1000, price: 150.0, size: 100.0, symbol: "AAPL".to_string(), side: None, exchange: None },
            Trade { ts: 2000, price: 151.0, size: 50.0, symbol: "AAPL".to_string(), side: None, exchange: None },
            Trade { ts: 3000, price: 152.0, size: 75.0, symbol: "AAPL".to_string(), side: None, exchange: None },
        ];
        
        let results = engine.on_trade_batch(trades);
        
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].vwap, 150.0);
        
        // Segundo resultado
        let expected2 = (150.0 * 100.0 + 151.0 * 50.0) / 150.0;
        assert!((results[1].vwap - expected2).abs() < 0.01);
    }

    #[test]
    fn test_vwap_reset_symbol() {
        let engine = VWAPEngine::new();
        
        let trade = Trade {
            ts: 1000,
            price: 150.0,
            size: 100.0,
            symbol: "AAPL".to_string(),
            side: None,
            exchange: None,
        };
        
        engine.on_trade(&trade);
        assert!(engine.get_vwap("AAPL").is_some());
        
        engine.reset_symbol("AAPL");
        assert_eq!(engine.get_vwap("AAPL"), None);
    }

    #[test]
    fn test_vwap_reset_all() {
        let engine = VWAPEngine::new();
        
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
            price: 3000.0,
            size: 1.0,
            symbol: "BTCUSDT".to_string(),
            side: None,
            exchange: None,
        };
        
        engine.on_trade(&trade1);
        engine.on_trade(&trade2);
        
        engine.reset_all();
        
        assert_eq!(engine.get_vwap("AAPL"), None);
        assert_eq!(engine.get_vwap("BTCUSDT"), None);
    }

    #[test]
    fn test_vwap_empty_batch() {
        let engine = VWAPEngine::new();
        let trades = Vec::new();
        
        let results = engine.on_trade_batch(trades);
        assert!(results.is_empty());
    }

    #[test]
    fn test_vwap_zero_volume() {
        let engine = VWAPEngine::new();
        
        let bar = Bar {
            ts: 1000,
            open: 150.0,
            high: 151.0,
            low: 149.0,
            close: 150.0,
            volume: 0.0,
            tf: "1m".to_string(),
            symbol: "AAPL".to_string(),
        };
        
        let result = engine.on_bar(&bar);
        assert!(result.is_none());
    }
}

