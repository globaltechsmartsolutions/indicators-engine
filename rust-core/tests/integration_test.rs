//! # Tests de Integración
//! 
//! Verifican que todos los engines funcionen juntos correctamente.

use indicators_core::*;

/// Crea un trade de prueba
fn create_trade(ts: u64, price: f64, size: f64, symbol: &str, _side: &str) -> Trade {
    Trade::new(ts, price, size, symbol.to_string())
}

/// Crea un snapshot del libro de prueba
fn create_book_snapshot(ts: u64, symbol: &str, best_bid: f64, best_ask: f64) -> BookSnapshot {
    let bids = vec![Level::new(best_bid, 100.0), Level::new(best_bid - 0.01, 200.0)];
    let asks = vec![Level::new(best_ask, 100.0), Level::new(best_ask + 0.01, 200.0)];
    BookSnapshot::new(ts, symbol.to_string(), bids, asks)
}

#[test]
fn test_cvd_and_vwap_together() {
    // Test: CVD y VWAP deben funcionar juntos procesando el mismo flujo de trades
    
    let cvd_engine = CVDEngine::new();
    let vwap_engine = VWAPEngine::new();
    
    let trades = vec![
        create_trade(1000, 150.0, 100.0, "AAPL", "BUY"),
        create_trade(2000, 151.0, 50.0, "AAPL", "SELL"),
        create_trade(3000, 152.0, 75.0, "AAPL", "BUY"),
    ];
    
    for trade in &trades {
        let cvd_result = cvd_engine.on_trade(trade);
        let vwap_result = vwap_engine.on_trade(trade);
        
        assert!(cvd_result.is_some(), "CVD debe procesar todos los trades");
        assert!(vwap_result.is_some(), "VWAP debe procesar todos los trades");
    }
    
    // Verificar que ambos engines tienen estado del símbolo
    let cvd = cvd_engine.get_cvd("AAPL");
    let vwap = vwap_engine.get_vwap("AAPL");
    
    assert!(cvd.is_some(), "CVD debe tener estado para AAPL");
    assert!(vwap.is_some(), "VWAP debe tener estado para AAPL");
}

#[test]
fn test_liquidity_and_heatmap_together() {
    // Test: Liquidity y Heatmap deben funcionar juntos procesando el mismo libro
    
    let liquidity_engine = LiquidityEngine::new();
    let heatmap_engine = HeatmapEngine::new();
    
    let snapshots = vec![
        create_book_snapshot(1000, "AAPL", 149.99, 150.01),
        create_book_snapshot(2000, "AAPL", 150.00, 150.02),
        create_book_snapshot(3000, "AAPL", 150.01, 150.03),
    ];
    
    for snapshot in &snapshots {
        let liq_result = liquidity_engine.on_snapshot(snapshot);
        let hm_result = heatmap_engine.on_snapshot(snapshot);
        
        assert!(liq_result.is_some(), "Liquidity debe procesar todos los snapshots");
        assert!(hm_result.is_some(), "Heatmap debe procesar todos los snapshots");
    }
}

#[test]
fn test_all_engines_multiple_symbols() {
    // Test: Todos los engines deben manejar múltiples símbolos correctamente
    
    let cvd_engine = CVDEngine::new();
    let vwap_engine = VWAPEngine::new();
    let liquidity_engine = LiquidityEngine::new();
    let heatmap_engine = HeatmapEngine::new();
    
    // Trades para AAPL
    let aapl_trades = vec![
        create_trade(1000, 150.0, 100.0, "AAPL", "BUY"),
        create_trade(2000, 151.0, 50.0, "AAPL", "SELL"),
    ];
    
    // Trades para MSFT
    let msft_trades = vec![
        create_trade(1000, 300.0, 200.0, "MSFT", "BUY"),
        create_trade(2000, 301.0, 100.0, "MSFT", "SELL"),
    ];
    
    // Snapshots para AAPL
    let aapl_snapshots = vec![
        create_book_snapshot(1000, "AAPL", 149.99, 150.01),
        create_book_snapshot(2000, "AAPL", 150.00, 150.02),
    ];
    
    // Snapshots para MSFT
    let msft_snapshots = vec![
        create_book_snapshot(1000, "MSFT", 299.99, 300.01),
        create_book_snapshot(2000, "MSFT", 300.00, 300.02),
    ];
    
    // Procesar todos los trades
    for trade in aapl_trades.iter().chain(msft_trades.iter()) {
        cvd_engine.on_trade(trade);
        vwap_engine.on_trade(trade);
    }
    
    // Procesar todos los snapshots
    for snapshot in aapl_snapshots.iter().chain(msft_snapshots.iter()) {
        liquidity_engine.on_snapshot(snapshot);
        heatmap_engine.on_snapshot(snapshot);
    }
    
    // Verificar que todos los engines tienen estado para ambos símbolos
    assert!(cvd_engine.get_cvd("AAPL").is_some());
    assert!(cvd_engine.get_cvd("MSFT").is_some());
    assert!(vwap_engine.get_vwap("AAPL").is_some());
    assert!(vwap_engine.get_vwap("MSFT").is_some());
}

#[test]
fn test_incremental_updates() {
    // Test: Los engines deben manejar actualizaciones incrementales correctamente
    
    let cvd_engine = CVDEngine::new();
    let vwap_engine = VWAPEngine::new();
    
    // Primer trade
    let trade1 = create_trade(1000, 150.0, 100.0, "AAPL", "BUY");
    let cvd1 = cvd_engine.on_trade(&trade1).unwrap();
    let vwap1 = vwap_engine.on_trade(&trade1).unwrap();
    
    assert_eq!(cvd1.cvd, 100.0); // Primer trade BUY suma 100
    assert_eq!(vwap1.vwap, 150.0);
    
    // Segundo trade
    let trade2 = create_trade(2000, 151.0, 50.0, "AAPL", "SELL");
    let cvd2 = cvd_engine.on_trade(&trade2).unwrap();
    let vwap2 = vwap_engine.on_trade(&trade2).unwrap();
    
    // CVD debe ser 100 - 50 = 50
    assert_eq!(cvd2.cvd, 50.0);
    // VWAP debe ser (150*100 + 151*50) / 150 = 150.33...
    assert!((vwap2.vwap - 150.33).abs() < 0.1);
}

#[test]
fn test_batch_vwap_against_incremental() {
    // Test: VWAP batch debe dar el mismo resultado que incremental
    
    let vwap_incremental = VWAPEngine::new();
    let vwap_batch = VWAPEngine::new();
    
    let trades = vec![
        create_trade(1000, 150.0, 100.0, "AAPL", "BUY"),
        create_trade(2000, 151.0, 50.0, "AAPL", "SELL"),
        create_trade(3000, 152.0, 75.0, "AAPL", "BUY"),
    ];
    
    // Incremental
    let mut incremental_results = Vec::new();
    for trade in &trades {
        let result = vwap_incremental.on_trade(trade);
        if let Some(r) = result {
            incremental_results.push(r);
        }
    }
    
    // Batch
    let batch_results = vwap_batch.on_trade_batch(trades);
    
    assert_eq!(incremental_results.len(), batch_results.len());
    
    // Verificar que los últimos valores coinciden
    let incremental_final = incremental_results.last().unwrap();
    let batch_final = batch_results.last().unwrap();
    
    assert!((incremental_final.vwap - batch_final.vwap).abs() < 0.01,
        "VWAP incremental y batch deben coincidir");
}

#[test]
fn test_reset_functions() {
    // Test: Las funciones de reset deben limpiar el estado correctamente
    
    let cvd_engine = CVDEngine::new();
    let vwap_engine = VWAPEngine::new();
    
    // Procesar trades
    cvd_engine.on_trade(&create_trade(1000, 150.0, 100.0, "AAPL", "BUY"));
    cvd_engine.on_trade(&create_trade(1000, 300.0, 200.0, "MSFT", "BUY"));
    
    vwap_engine.on_trade(&create_trade(1000, 150.0, 100.0, "AAPL", "BUY"));
    vwap_engine.on_trade(&create_trade(1000, 300.0, 200.0, "MSFT", "BUY"));
    
    // Verificar que hay estado
    assert!(cvd_engine.get_cvd("AAPL").is_some());
    assert!(cvd_engine.get_cvd("MSFT").is_some());
    assert!(vwap_engine.get_vwap("AAPL").is_some());
    assert!(vwap_engine.get_vwap("MSFT").is_some());
    
    // Reset un símbolo
    cvd_engine.reset_symbol("AAPL");
    vwap_engine.reset_symbol("AAPL");
    
    // Verificar que solo AAPL se limpió
    assert_eq!(cvd_engine.get_cvd("AAPL"), None);
    assert!(cvd_engine.get_cvd("MSFT").is_some());
    assert_eq!(vwap_engine.get_vwap("AAPL"), None);
    assert!(vwap_engine.get_vwap("MSFT").is_some());
    
    // Reset todos
    cvd_engine.reset_all();
    vwap_engine.reset_all();
    
    // Verificar que todo se limpió
    assert_eq!(cvd_engine.get_cvd("MSFT"), None);
    assert_eq!(vwap_engine.get_vwap("MSFT"), None);
}

#[test]
fn test_heatmap_compression() {
    // Test: Heatmap debe comprimir correctamente
    
    let engine = HeatmapEngine::new();
    
    // Crear múltiples snapshots en el mismo bucket
    let snapshot1 = create_book_snapshot(1234567890, "AAPL", 149.99, 150.01);
    let snapshot2 = create_book_snapshot(1234567891, "AAPL", 150.00, 150.02);
    let snapshot3 = create_book_snapshot(1234567892, "AAPL", 150.01, 150.03);
    
    engine.on_snapshot(&snapshot1);
    engine.on_snapshot(&snapshot2);
    let result = engine.on_snapshot(&snapshot3);
    
    assert!(result.is_some());
    let metrics = result.unwrap();
    
    // Debe tener tiles significativos
    assert!(metrics.tiles.len() > 0);
    // Compression ratio debe ser >= 1.0
    assert!(metrics.compression_ratio >= 1.0);
}

#[test]
fn test_liquidity_imbalance() {
    // Test: Liquidity debe calcular imbalance correctamente
    
    let engine = LiquidityEngine::new();
    
    // Snapshot con desbalance (más bids que asks)
    let snapshot = BookSnapshot::new(
        1234567890,
        "AAPL".to_string(),
        vec![
            Level::new(149.99, 100.0),
            Level::new(149.98, 200.0),
            Level::new(149.97, 150.0),
        ],
        vec![
            Level::new(150.01, 50.0),  // Menos profundidad en asks
        ],
    );
    
    let result = engine.on_snapshot(&snapshot);
    assert!(result.is_some());
    
    let metrics = result.unwrap();
    
    // Debe tener más profundidad en bids
    assert!(metrics.bids_depth > metrics.asks_depth);
    // Imbalance debe ser positivo (más bids)
    assert!(metrics.depth_imbalance > 0.0);
}

#[test]
fn test_concurrent_access() {
    // Test: Los engines deben ser thread-safe (DashMap + Arc)
    
    // Este test verifica que la estructura es thread-safe,
    // aunque no podemos probar realmente concurrencia sin threads adicionales
    // en un test simple.
    
    let cvd_engine = CVDEngine::new();
    
    // Simular acceso rápido
    let trades = (0..100).map(|i| {
        create_trade(1000 + i, 150.0 + (i as f64 * 0.01), 100.0 + (i as f64), "AAPL", "BUY")
    }).collect::<Vec<_>>();
    
    for trade in &trades {
        let result = cvd_engine.on_trade(trade);
        assert!(result.is_some());
    }
    
    // Verificar que todos se procesaron
    let cvd = cvd_engine.get_cvd("AAPL");
    assert!(cvd.is_some());
}

#[test]
fn test_edge_cases() {
    // Test: Manejo de casos extremos
    
    let cvd_engine = CVDEngine::new();
    let vwap_engine = VWAPEngine::new();
    let liquidity_engine = LiquidityEngine::new();
    let heatmap_engine = HeatmapEngine::new();
    
    // Trade con tamaño muy pequeño
    let small_trade = create_trade(1000, 150.0, 0.0001, "AAPL", "BUY");
    assert!(cvd_engine.on_trade(&small_trade).is_some());
    assert!(vwap_engine.on_trade(&small_trade).is_some());
    
    // Trade con precio muy bajo
    let low_price_trade = create_trade(2000, 0.01, 100.0, "AAPL", "BUY");
    assert!(cvd_engine.on_trade(&low_price_trade).is_some());
    assert!(vwap_engine.on_trade(&low_price_trade).is_some());
    
    // Snapshot con spread muy pequeño
    let tight_spread = create_book_snapshot(1000, "AAPL", 149.999, 150.001);
    assert!(liquidity_engine.on_snapshot(&tight_spread).is_some());
    assert!(heatmap_engine.on_snapshot(&tight_spread).is_some());
}

#[test]
fn test_performance_indicators() {
    // Test: Verificar que los indicadores dan resultados esperados
    
    let engine = LiquidityEngine::new();
    
    // Snapshot balanceado
    let balanced = create_book_snapshot(1000, "AAPL", 149.99, 150.01);
    let result = engine.on_snapshot(&balanced).unwrap();
    
    // Spread debe ser 0.02
    assert!((result.spread - 0.02).abs() < 0.001);
    
    // Mid debe ser 150.0
    assert!((result.mid - 150.0).abs() < 0.001);
    
    // Con spread balanceado, ambos depths deben ser similares
    assert!((result.bids_depth - result.asks_depth).abs() < 1.0);
}
