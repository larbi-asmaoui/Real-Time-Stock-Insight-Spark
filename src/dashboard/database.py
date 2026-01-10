import duckdb
import streamlit as st
from dashboard import config

@st.cache_resource
def get_db_connection():
    """
    Creates a persistent DuckDB connection acting as a SQL Engine over S3.
    """
    try:
        con = duckdb.connect(database=':memory:')
        
        # Install S3 Extension
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Configure MinIO Access
        con.execute(f"""
            SET s3_endpoint='{config.S3_ENDPOINT}';
            SET s3_access_key_id='{config.ACCESS_KEY}';
            SET s3_secret_access_key='{config.SECRET_KEY}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        
        # Initialize Views (Virtual Tables)
        # We use try/except in case files don't exist yet
        try:
            con.execute(f"CREATE OR REPLACE VIEW gold AS SELECT * FROM read_parquet('{config.GOLD_PATH}/symbol=*/*.parquet', hive_partitioning=true)")
            con.execute(f"CREATE OR REPLACE VIEW predictions AS SELECT * FROM read_parquet('{config.PRED_PATH}/*.parquet')")
        except Exception:
            pass
            
        return con
    except Exception as e:
        st.error(f"Failed to connect to Data Lake: {e}")
        return None