"""
Dashboard principal para visualización en tiempo real
"""

import dash
from dash import dcc, html
import plotly.graph_objects as go
from typing import Optional
from indicators_core import HeatmapEngine, BookSnapshot, Level, HeatmapMetrics


def create_live_dashboard() -> dash.Dash:
    """
    Crea un dashboard Dash interactivo.
    
    Returns:
        App de Dash configurada
    """
    app = dash.Dash(__name__)
    
    app.layout = html.Div([
        html.H1("Indicators Engine Dashboard", style={'textAlign': 'center'}),
        
        html.Div([
            html.H3("Order Book Heatmap"),
            dcc.Graph(id='heatmap-plot'),
            dcc.Interval(id='interval-component', interval=1000, n_intervals=0)
        ]),
        
        html.Div([
            html.H3("Stats"),
            html.Div(id='stats-display')
        ])
    ])
    
    @app.callback(
        dash.dependencies.Output('heatmap-plot', 'figure'),
        dash.dependencies.Input('interval-component', 'n_intervals')
    )
    def update_heatmap(n):
        # Placeholder para datos reales
        fig = go.Figure()
        fig.add_annotation(text="Waiting for data...", 
                         xref="paper", yref="paper", x=0.5, y=0.5)
        return fig
    
    @app.callback(
        dash.dependencies.Output('stats-display', 'children'),
        dash.dependencies.Input('interval-component', 'n_intervals')
    )
    def update_stats(n):
        return html.Div([
            html.P(f"Updates: {n}"),
            html.P("Waiting for book data...")
        ])
    
    return app


def show_example_heatmap():
    """Muestra un ejemplo de heatmap con datos sintéticos."""
    from .visualization import plot_heatmap_tiles
    
    # Crear engine y procesar snapshot de ejemplo
    engine = HeatmapEngine()
    
    # Datos sintéticos
    bids = [Level(price=150.0 - i * 0.01, size=100.0 + i * 50.0) for i in range(10)]
    asks = [Level(price=150.0 + i * 0.01, size=100.0 + i * 50.0) for i in range(10)]
    
    snapshot = BookSnapshot(
        ts=1234567890,
        symbol="AAPL",
        bids=bids,
        asks=asks
    )
    
    # Procesar
    metrics = engine.on_snapshot(snapshot)
    
    if metrics:
        # Visualizar
        fig = plot_heatmap_tiles(metrics, title="Example Heatmap")
        fig.show()


if __name__ == "__main__":
    print("Iniciando dashboard...")
    show_example_heatmap()

