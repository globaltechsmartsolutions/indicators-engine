//! # Utilidades
//! 
//! Funciones de utilidad para el núcleo de indicadores.

/// División segura (evita NaNs e Infinitos)
pub fn safe_div(num: f64, den: f64) -> f64 {
    if den.is_finite() && den > 0.0 && num.is_finite() {
        num / den
    } else {
        0.0
    }
}

/// Valida que un valor sea finito
pub fn is_finite(value: f64) -> bool {
    value.is_finite()
}

/// Calcula el precio medio entre bid y ask
pub fn calculate_mid(bid: f64, ask: f64) -> f64 {
    (bid + ask) / 2.0
}

/// Calcula el spread entre bid y ask
pub fn calculate_spread(bid: f64, ask: f64) -> f64 {
    ask - bid
}

/// Cuantiza un precio al tick más cercano
pub fn quantize_price(price: f64, tick_size: f64) -> f64 {
    (price / tick_size).round() * tick_size
}

/// Calcula el bucket temporal
pub fn calculate_bucket(ts: u64, bucket_ms: u64) -> u64 {
    (ts / bucket_ms) * bucket_ms
}

/// Agregación SIMD de volumen (optimizada con chunks)
/// 
/// Para arrays grandes, usa procesamiento por chunks para mejor caché locality
pub fn aggregate_volume_simd(volumes: &[f64]) -> f64 {
    // Procesar en chunks de 4 para mejor caché
    let chunk_size = 4;
    let mut sum = 0.0;
    
    // Procesar chunks completos
    for chunk in volumes.chunks(chunk_size) {
        sum += chunk.iter().sum::<f64>();
    }
    
    sum
}

/// Suma incremental optimizada para slides de ventana deslizante
pub fn sliding_window_sum(values: &[f64], window_size: usize) -> Vec<f64> {
    if window_size >= values.len() {
        return vec![values.iter().sum()];
    }
    
    let mut result = Vec::with_capacity(values.len() - window_size + 1);
    let mut current_sum: f64 = values[..window_size].iter().sum();
    result.push(current_sum);
    
    // Slide: añadir nuevo, quitar antiguo
    for i in 0..values.len() - window_size {
        current_sum += values[i + window_size] - values[i];
        result.push(current_sum);
    }
    
    result
}

/// Binning de precios con SIMD (placeholder)
pub fn price_binning_simd(prices: &[f64], tick_size: f64) -> Vec<u64> {
    prices.iter()
        .map(|&p| quantize_price(p, tick_size) as u64)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_div_normal() {
        assert_eq!(safe_div(10.0, 2.0), 5.0);
        assert_eq!(safe_div(15.0, 3.0), 5.0);
    }

    #[test]
    fn test_safe_div_zero_denominator() {
        assert_eq!(safe_div(10.0, 0.0), 0.0);
    }

    #[test]
    fn test_safe_div_infinity() {
        assert_eq!(safe_div(1.0, f64::INFINITY), 0.0);
        assert_eq!(safe_div(f64::INFINITY, 1.0), 0.0);
    }

    #[test]
    fn test_is_finite() {
        assert!(is_finite(10.0));
        assert!(!is_finite(f64::INFINITY));
        assert!(!is_finite(f64::NAN));
    }

    #[test]
    fn test_calculate_mid() {
        assert_eq!(calculate_mid(99.0, 101.0), 100.0);
        assert_eq!(calculate_mid(149.99, 150.01), 150.0);
    }

    #[test]
    fn test_calculate_spread() {
        assert_eq!(calculate_spread(99.0, 101.0), 2.0);
        assert!((calculate_spread(149.99, 150.01) - 0.02).abs() < 0.001);
    }

    #[test]
    fn test_quantize_price() {
        assert_eq!(quantize_price(150.23, 0.01), 150.23);
        assert_eq!(quantize_price(150.227, 0.01), 150.23);
        assert!((quantize_price(150.225, 0.1) - 150.2).abs() < 0.001);
        assert!((quantize_price(150.26, 0.1) - 150.3).abs() < 0.001);
    }

    #[test]
    fn test_calculate_bucket() {
        assert_eq!(calculate_bucket(1234567890, 1000), 1234567000);
        assert_eq!(calculate_bucket(1234567989, 1000), 1234567000);
        assert_eq!(calculate_bucket(1234569000, 1000), 1234569000);
    }

    #[test]
    fn test_aggregate_volume_simd() {
        let volumes = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        assert_eq!(aggregate_volume_simd(&volumes), 21.0);
        
        let single = vec![100.0];
        assert_eq!(aggregate_volume_simd(&single), 100.0);
        
        let empty = vec![];
        assert_eq!(aggregate_volume_simd(&empty), 0.0);
    }

    #[test]
    fn test_sliding_window_sum() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = sliding_window_sum(&values, 3);
        
        assert_eq!(result, vec![6.0, 9.0, 12.0]); // [1+2+3, 2+3+4, 3+4+5]
    }

    #[test]
    fn test_sliding_window_sum_large_window() {
        let values = vec![1.0, 2.0, 3.0];
        let result = sliding_window_sum(&values, 5); // Window bigger than data
        
        assert_eq!(result, vec![6.0]); // Sum of all
    }

    #[test]
    fn test_price_binning_simd() {
        let prices = vec![150.23, 150.27, 150.25];
        let result = price_binning_simd(&prices, 0.01);
        
        // quantize_price redondea y luego se castea a u64, truncando decimales
        // Así que 150.23 -> 150 (como u64)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], 150);
        assert_eq!(result[1], 150);
        assert_eq!(result[2], 150);
    }
}