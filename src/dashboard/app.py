import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import time
import os

# --- Configuration ---
st.set_page_config(
    page_title="Real-Time Stock Insight",
    page_icon="📈",
    layout="wide"
)

# Constants
GOLD_PATH = "/app/data/lake/gold"
REFRESH_SECONDS = 2

# --- Data Loading ---
def get_duckdb_connection():
    return duckdb.connect(database=':memory:')

def load_data():
    """Reads Parquet files from Gold Layer using DuckDB"""
    con = get_duckdb_connection()
    try:
        # Check if path exists
        if not os.path.exists(GOLD_PATH):
            return None
            

        # DuckDB generic glob pattern for recursive parquet search
        # explicit glob to avoid reading _delta_log files
        query = f"""
            SELECT * 
            FROM read_parquet('{GOLD_PATH}/symbol=*/*.parquet', hive_partitioning=true)
        """
        df = con.execute(query).df()
        
        if df.empty:
            return None
            
        return df
    except Exception as e:
        # Gracefully handle "No files found" or other errors during startup
        # st.toast(f"Waiting for data: {e}") # Optional debug to UI
        return None

# --- UI Components ---
def render_kpis(df_symbol):
    """Render Key Performance Indicators"""
    latest = df_symbol.sort_values("window_end", ascending=False).iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Latest Price (Avg)", 
            value=f"${latest['avg_price']:.2f}",
            delta=f"{latest['price_change_pct']:.2f}%" if 'price_change_pct' in latest else None
        )
    
    with col2:
        st.metric(
            label="Volatility", 
            value=f"{latest['volatility']:.4f}"
        )
        
    with col3:
        st.metric(
            label="Volume", 
            value=f"{latest['total_volume']:,}"
        )
        
    with col4:
        # Simple Logic for "Signal" based on Price vs 5-min Avg (if we had it, otherwise distinct metric)
        # Using volatility as a proxy for "Risk" here
        risk_level = "HIGH" if latest['volatility'] > 1.0 else "LOW"
        st.metric(label="Risk Level", value=risk_level)

def render_charts(df_symbol):
    """Render Main Charts"""
    
    # 1. Candlestick-like Chart (Min/Max/Avg)
    fig_candle = go.Figure(data=[
        go.Candlestick(
            x=df_symbol['window_end'],
            open=df_symbol['avg_price'], # Using avg as open proxy for aggregated view
            high=df_symbol['max_price'],
            low=df_symbol['min_price'],
            close=df_symbol['avg_price'] # Close is avg for this window
        )
    ])
    
    fig_candle.update_layout(
        title=f"Price Movement (Aggregated Windows)",
        xaxis_title="Time",
        yaxis_title="Price",
        template="plotly_dark",
        height=500
    )
    
    st.plotly_chart(fig_candle, use_container_width=True)
    
    # 2. Volume Bar Chart
    fig_vol = px.bar(
        df_symbol, 
        x="window_end", 
        y="total_volume", 
        title="Trading Volume",
        template="plotly_dark"
    )
    st.plotly_chart(fig_vol, use_container_width=True)

# --- Main App Loop ---
def main():
    st.title("⚡ Real-Time Stock Insight - Lakehouse Dashboard")
    
    # Placeholders for dynamic content
    status_container = st.empty()
    content_container = st.container()
    
    # --- Sidebar ---
    st.sidebar.header("Configuration")
    auto_refresh = st.sidebar.checkbox("Auto-Refresh", value=True)
    
    # Load Data
    df = load_data()
    
    if df is None or df.empty:
        status_container.warning("⚠️ No data available in Gold Layer yet. Waiting for Spark Pipeline...")
        time.sleep(REFRESH_SECONDS)
        if auto_refresh:
            st.rerun()
        return

    # Convert timestamps if needed
    if 'window_end' in df.columns:
        df['window_end'] = pd.to_datetime(df['window_end'])

    # Get available symbols
    symbols = df['symbol'].unique()
    selected_symbol = st.sidebar.selectbox("Select Symbol", symbols)
    
    # Filter for selected symbol
    df_symbol = df[df['symbol'] == selected_symbol].sort_values("window_end")
    
    # --- Render Content ---
    with content_container:
        if not df_symbol.empty:
            st.subheader(f"Analysis for {selected_symbol}")
            render_kpis(df_symbol)
            render_charts(df_symbol)
        else:
            st.info(f"No data for {selected_symbol}")

    # Re-run
    if auto_refresh:
        time.sleep(REFRESH_SECONDS)
        st.rerun()

if __name__ == "__main__":
    main()
