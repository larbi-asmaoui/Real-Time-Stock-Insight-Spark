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

# SENIOR CONFIG: Point to S3, not local disk
# Note: Spark uses 's3a://', DuckDB uses 's3://'
GOLD_PATH = "s3://finance-lake/gold"
REFRESH_SECONDS = 2

# --- Data Loading ---
def get_duckdb_connection():
    con = duckdb.connect(database=':memory:')
    
    # INSTALL S3 SUPPORT
    # This is critical for MinIO/AWS access
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # CONFIGURE S3 (Read from Docker Env Vars)
    # These vars are set in your docker-compose.yml
    s3_endpoint = os.getenv("S3_ENDPOINT", "minio:9000").replace("http://", "")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    
    con.execute(f"""
        SET s3_endpoint='{s3_endpoint}';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

def load_data():
    """Reads Parquet files from MinIO Gold Layer using DuckDB"""
    try:
        con = get_duckdb_connection()
        
        # DuckDB glob pattern for S3
        # We look for ANY parquet file recursively under gold
        query = f"""
            SELECT * 
            FROM read_parquet('{GOLD_PATH}/symbol=*/*.parquet', hive_partitioning=true)
        """
        df = con.execute(query).df()
        
        if df.empty:
            return None
            
        return df
    except Exception as e:
        # This often happens during startup if the bucket is empty
        # st.warning(f"Waiting for data... ({e})")
        return None

# --- UI Components ---
def render_kpis(df_symbol):
    """Render Key Performance Indicators"""
    # Sort by window_end to get latest
    latest = df_symbol.sort_values("window_end", ascending=False).iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Latest Price (Avg)", 
            value=f"${latest['avg_price']:.2f}",
            delta=f"{latest['price_change_pct']:.2f}%" if 'price_change_pct' in latest else None
        )
    
    with col2:
        vol = latest.get('volatility', 0.0)
        st.metric(
            label="Volatility", 
            value=f"{vol:.4f}"
        )
        
    with col3:
        st.metric(
            label="Volume", 
            value=f"{latest['total_volume']:,}"
        )
        
    with col4:
        # Simple Logic for "Risk"
        vol = latest.get('volatility', 0.0)
        risk_level = "HIGH" if vol > 1.0 else "LOW"
        st.metric(label="Risk Level", value=risk_level)

def render_charts(df_symbol):
    """Render Main Charts"""
    
    # 1. Candlestick-like Chart
    fig_candle = go.Figure(data=[
        go.Candlestick(
            x=df_symbol['window_end'],
            open=df_symbol['avg_price'], 
            high=df_symbol['max_price'],
            low=df_symbol['min_price'],
            close=df_symbol['avg_price']
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
    
    status_container = st.empty()
    content_container = st.container()
    
    st.sidebar.header("Configuration")
    auto_refresh = st.sidebar.checkbox("Auto-Refresh", value=True)
    
    df = load_data()
    
    if df is None or df.empty:
        status_container.info("⏳ Connecting to S3 Data Lake... Waiting for Spark to write files.")
        time.sleep(REFRESH_SECONDS)
        if auto_refresh:
            st.rerun()
        return

    # Convert timestamps
    if 'window_end' in df.columns:
        df['window_end'] = pd.to_datetime(df['window_end'])

    # Get available symbols
    symbols = df['symbol'].unique()
    selected_symbol = st.sidebar.selectbox("Select Symbol", symbols)
    
    # Filter
    df_symbol = df[df['symbol'] == selected_symbol].sort_values("window_end")
    
    with content_container:
        if not df_symbol.empty:
            st.subheader(f"Analysis for {selected_symbol}")
            render_kpis(df_symbol)
            render_charts(df_symbol)
        else:
            st.info(f"No data for {selected_symbol}")

    if auto_refresh:
        time.sleep(REFRESH_SECONDS)
        st.rerun()

if __name__ == "__main__":
    main()