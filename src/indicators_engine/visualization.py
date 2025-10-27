"""
Visualization module para indicadores con Plotly
"""

import plotly.graph_objects as go
from typing import List, Optional
import numpy as np

from indicators_core import Tile, HeatmapMetrics


def plot_heatmap_tiles(metrics: HeatmapMetrics, title: str = "Order Book Heatmap") -> go.Figure:
    """
    Visualiza los tiles del heatmap como un mapa de calor.
    
    Args:
        metrics: HeatmapMetrics con tiles comprimidos
        title: Título del gráfico
        
    Returns:
        Figura de Plotly
    """
    # Separar por lado (bid/ask)
    bid_tiles = [t for t in metrics.tiles if t.side == "bid"]
    ask_tiles = [t for t in metrics.tiles if t.side == "ask"]
    
    # Preparar datos para el gráfico
    prices = [t.price_bin for t in metrics.tiles]
    sizes = [t.total_size for t in metrics.tiles]
    sides = [t.side for t in metrics.tiles]
    
    fig = go.Figure()
    
    # Barras para bids (verde)
    bid_prices = [t.price_bin for t in bid_tiles]
    bid_sizes = [t.total_size for t in bid_tiles]
    if bid_prices and bid_sizes:
        fig.add_trace(go.Bar(
            x=bid_prices,
            y=bid_sizes,
            name="Bid",
            marker_color="green",
            opacity=0.7
        ))
    
    # Barras para asks (rojo)
    ask_prices = [t.price_bin for t in ask_tiles]
    ask_sizes = [t.total_size for t in ask_tiles]
    if ask_prices and ask_sizes:
        fig.add_trace(go.Bar(
            x=ask_prices,
            y=ask_sizes,
            name="Ask",
            marker_color="red",
            opacity=0.7
        ))
    
    fig.update_layout(
        title=f"{title}<br><sub>Compression: {metrics.compression_ratio:.2f}x | Tiles: {len(metrics.tiles)}</sub>",
        xaxis_title="Price",
        yaxis_title="Size",
        barmode="overlay",
        height=500,
        hovermode="closest"
    )
    
    return fig


def plot_heatmap_matrix(metrics: HeatmapMetrics, num_bins: int = 50) -> go.Figure:
    """
    Visualiza heatmap como una matriz 2D (price bins vs lado).
    
    Args:
        metrics: HeatmapMetrics con tiles
        num_bins: Número de bins de precio
        
    Returns:
        Figura de Plotly
    """
    # Crear bins de precio
    all_prices = [t.price_bin for t in metrics.tiles]
    if not all_prices:
        # Si no hay datos, crear gráfico vacío
        fig = go.Figure()
        fig.add_annotation(text="No data available", xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    min_price = min(all_prices)
    max_price = max(all_prices)
    bin_size = (max_price - min_price) / num_bins if max_price > min_price else 1.0
    
    # Crear matriz de tamaño
    matrix = np.zeros((num_bins, 2))  # 2 columnas: bid, ask
    
    for tile in metrics.tiles:
        bin_idx = min(int((tile.price_bin - min_price) / bin_size), num_bins - 1)
        side_idx = 0 if tile.side == "bid" else 1
        matrix[bin_idx, side_idx] += tile.total_size
    
    # Crear etiquetas de precio
    price_labels = [min_price + i * bin_size for i in range(num_bins)]
    
    fig = go.Figure()
    
    # Side A (Bids)
    fig.add_trace(go.Bar(
        y=price_labels,
        x=matrix[:, 0],
        name="Bid",
        orientation="h",
        marker_color="green",
        opacity=0.7
    ))
    
    # Side B (Asks)
    fig.add_trace(go.Bar(
        y=price_labels,
        x=matrix[:, 1],
        name="Ask",
        orientation="h",
        marker_color="red",
        opacity=0.7
    ))
    
    fig.update_layout(
        title=f"Heatmap Matrix (Bucket: {metrics.bucket_ts})",
        xaxis_title="Size",
        yaxis_title="Price",
        height=800,
        barmode="overlay"
    )
    
    return fig


def plot_tiles_scatter(metrics: HeatmapMetrics, title: str = "Heatmap Tiles") -> go.Figure:
    """
    Visualiza tiles como scatter plot (precio vs tamaño).
    
    Args:
        metrics: HeatmapMetrics
        title: Título
        
    Returns:
        Figura de Plotly
    """
    bid_tiles = [t for t in metrics.tiles if t.side == "bid"]
    ask_tiles = [t for t in metrics.tiles if t.side == "ask"]
    
    fig = go.Figure()
    
    if bid_tiles:
        fig.add_trace(go.Scatter(
            x=[t.price_bin for t in bid_tiles],
            y=[t.total_size for t in bid_tiles],
            mode="markers",
            name="Bid",
            marker=dict(color="green", size=10),
            hovertemplate="<b>Bid</b><br>Price: %{x}<br>Size: %{y}<extra></extra>"
        ))
    
    if ask_tiles:
        fig.add_trace(go.Scatter(
            x=[t.price_bin for t in ask_tiles],
            y=[t.total_size for t in ask_tiles],
            mode="markers",
            name="Ask",
            marker=dict(color="red", size=10),
            hovertemplate="<b>Ask</b><br>Price: %{x}<br>Size: %{y}<extra></extra>"
        ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Price",
        yaxis_title="Size",
        hovermode="closest",
        height=500
    )
    
    return fig


def create_dashboard(metrics: HeatmapMetrics) -> go.Figure:
    """
    Crea un dashboard completo con múltiples visualizaciones.
    
    Args:
        metrics: HeatmapMetrics
        
    Returns:
        Figura con subplots
    """
    from plotly.subplots import make_subplots
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Heatmap Bars", "Heatmap Matrix", "Tile Scatter", "Compression Stats"),
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "scatter"}, {"type": "bar"}]]
    )
    
    # Preparar datos
    bid_tiles = [t for t in metrics.tiles if t.side == "bid"]
    ask_tiles = [t for t in metrics.tiles if t.side == "ask"]
    
    # 1. Heatmap Bars (row 1, col 1)
    if bid_tiles:
        fig.add_trace(go.Bar(x=[t.price_bin for t in bid_tiles], 
                           y=[t.total_size for t in bid_tiles],
                           name="Bid", marker_color="green", opacity=0.7),
                     row=1, col=1)
    if ask_tiles:
        fig.add_trace(go.Bar(x=[t.price_bin for t in ask_tiles],
                           y=[t.total_size for t in ask_tiles],
                           name="Ask", marker_color="red", opacity=0.7),
                     row=1, col=1)
    
    # 2. Heatmap Matrix simplificado (row 1, col 2)
    if metrics.tiles:
        prices = [t.price_bin for t in metrics.tiles]
        fig.add_trace(go.Histogram(x=prices, name="Price Distribution"),
                     row=1, col=2)
    
    # 3. Scatter (row 2, col 1)
    if bid_tiles:
        fig.add_trace(go.Scatter(x=[t.price_bin for t in bid_tiles],
                               y=[t.total_size for t in bid_tiles],
                               mode="markers", name="Bid",
                               marker_color="green"),
                     row=2, col=1)
    if ask_tiles:
        fig.add_trace(go.Scatter(x=[t.price_bin for t in ask_tiles],
                               y=[t.total_size for t in ask_tiles],
                               mode="markers", name="Ask",
                               marker_color="red"),
                     row=2, col=1)
    
    # 4. Stats (row 2, col 2)
    stats_names = ["Total Tiles", "Max Size", "Compression"]
    stats_values = [len(metrics.tiles), metrics.max_sz, metrics.compression_ratio]
    fig.add_trace(go.Bar(x=stats_names, y=stats_values, name="Stats"),
                 row=2, col=2)
    
    fig.update_xaxes(title_text="Price", row=2, col=1)
    fig.update_yaxes(title_text="Size", row=2, col=1)
    
    fig.update_layout(
        title_text=f"Heatmap Dashboard (Bucket {metrics.bucket_ts})",
        height=1000,
        showlegend=True
    )
    
    return fig


def save_html(fig: go.Figure, filename: str) -> None:
    """Guarda la figura como HTML."""
    fig.write_html(filename)


if __name__ == "__main__":
    # Test básico
    print("✅ Módulo de visualización importado")
    print("Funciones disponibles:")
    print("  - plot_heatmap_tiles()")
    print("  - plot_heatmap_matrix()")
    print("  - plot_tiles_scatter()")
    print("  - create_dashboard()")

