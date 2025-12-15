import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = "sqlite:///analytics.db"
engine = create_engine(DATABASE_URL)

class BIDashboard:
    """Business Intelligence Dashboard with Dash and Plotly"""
    
    def __init__(self, debug=True):
        self.app = dash.Dash(__name__)
        self.debug = debug
        self.setup_layout()
        self.setup_callbacks()
    
    def fetch_sales_data(self):
        """Fetch sales data from database"""
        try:
            query = "SELECT region, amount, quantity FROM sales_data"
            df = pd.read_sql(query, engine)
            logger.info(f"Fetched {len(df)} records from database")
            return df
        except Exception as e:
            logger.error(f"Database query error: {e}")
            return None
    
    def get_kpis(self, df):
        """Calculate Key Performance Indicators"""
        if df is None or df.empty:
            return {"total": 0, "avg": 0, "max": 0}
        
        return {
            "total": f"${df['amount'].sum():.2f}",
            "avg": f"${df['amount'].mean():.2f}",
            "max": f"${df['amount'].max():.2f}",
            "count": len(df)
        }
    
    def setup_layout(self):
        """Setup dashboard layout with KPIs and charts"""
        df = self.fetch_sales_data()
        kpis = self.get_kpis(df)
        
        self.app.layout = html.Div([
            html.Div([
                html.H1("ETL SQL BI Analytics Dashboard", 
                       style={'textAlign': 'center', 'marginBottom': 30}),
            ]),
            
            html.Div([
                html.Div([
                    html.Div([
                        html.H3("Total Sales"),
                        html.P(kpis["total"], style={'fontSize': '24px', 'color': '#1f77b4'})
                    ], className='kpi-card'),
                    
                    html.Div([
                        html.H3("Average Sale"),
                        html.P(kpis["avg"], style={'fontSize': '24px', 'color': '#ff7f0e'})
                    ], className='kpi-card'),
                    
                    html.Div([
                        html.H3("Max Sale"),
                        html.P(kpis["max"], style={'fontSize': '24px', 'color': '#2ca02c'})
                    ], className='kpi-card'),
                ], style={'display': 'flex', 'justifyContent': 'space-around', 'marginBottom': 30}),
            ]),
            
            html.Div([
                html.Div([
                    dcc.Graph(id='sales-by-region')
                ], style={'width': '48%', 'display': 'inline-block'}),
                
                html.Div([
                    dcc.Graph(id='amount-distribution')
                ], style={'width': '48%', 'display': 'inline-block', 'float': 'right'}),
            ]),
        ], style={'padding': '20px'})
    
    def setup_callbacks(self):
        """Setup Dash callbacks for interactive charts"""
        @self.app.callback(
            [Output('sales-by-region', 'figure'),
             Output('amount-distribution', 'figure')],
            [Input('sales-by-region', 'id')]
        )
        def update_graphs(_):
            df = self.fetch_sales_data()
            
            if df is None or df.empty:
                return {}, {}
            
            region_sales = df.groupby('region')['amount'].sum().reset_index()
            fig1 = px.bar(region_sales, x='region', y='amount',
                         title='Sales by Region')
            
            fig2 = px.histogram(df, x='amount',
                              title='Sales Amount Distribution',
                              nbins=20)
            
            return fig1, fig2
    
    def run(self, host='127.0.0.1', port=8050):
        """Run the dashboard server"""
        logger.info(f"Starting BI Dashboard on {host}:{port}")
        self.app.run_server(debug=self.debug, host=host, port=port)

if __name__ == '__main__':
    dashboard = BIDashboard(debug=True)
    dashboard.run()
    print("BI Dashboard module loaded successfully")
