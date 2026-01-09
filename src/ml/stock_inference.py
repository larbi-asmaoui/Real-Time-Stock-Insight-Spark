
import time
import json
import logging
import os
import shutil
import pandas as pd
import numpy as np
import torch
import mlflow.pytorch
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, collect_list
from pyspark.sql.window import Window
from feature_engineering import transform_data


logging.basicConfig(
    filename='logs/stock_inference.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w'
)

# --- CONFIGURATION ---
POLL_INTERVAL_SECONDS = 10 

def get_spark_session():
    """Builds a persistent Spark Session"""
    return SparkSession.builder \
        .appName("StockInferenceService") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def load_model_resources():
    """Loads Model and Stats once to save time"""
    mlflow.set_tracking_uri("file:///app/mlruns")
    try:
        # Find best run
        runs = mlflow.search_runs(experiment_names=["Stock_Prediction_LSTM"], order_by=["start_time DESC"], max_results=1)
        if runs.empty:
            return None, None
            
        run_id = runs.iloc[0].run_id
        logging.info(f"Loading Model from Run ID: {run_id}")
        
        # Load Model
        model_uri = f"runs:/{run_id}/model"
        model = mlflow.pytorch.load_model(model_uri)
        
        # Load Stats
        stats_path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="metrics.json")
        with open(stats_path) as f:
            stats = json.load(f)
            
        return model, stats
    except Exception as e:
        logging.error(f"Error loading model: {e}")
        return None, None

def run_prediction_cycle(spark, model, stats):
    """Single execution of the prediction logic"""
    try:
        # 1. READ LATEST DATA (Optimization: Read only recent files if possible, here we read all Gold)
        df = spark.read.format("delta").load("s3a://finance-lake/lake/gold")
        
        # 2. FEATURE ENGINEERING
        df = transform_data(df)
        
        # 3. SCALE
        feature_cols = ["log_returns", "volatility", "log_volume"]
        for c in feature_cols:
            m = stats[c]["mean"]
            s = stats[c]["std"]
            df = df.withColumn(c, (col(c) - m) / s)

        # 4. GET LATEST SEQUENCE
        SEQ_LEN = 5
        w = Window.partitionBy("symbol").orderBy("window_start")
        
        # Collect last 5 rows per symbol
        df_seq = df \
            .withColumn("feature_vec", array(feature_cols)) \
            .withColumn("sequence", collect_list("feature_vec").over(w.rowsBetween(-SEQ_LEN + 1, 0))) \
            .withColumn("seq_len", F.size(col("sequence"))) \
            .filter(col("seq_len") == SEQ_LEN)

        # Filter to only the absolute latest timestamp per symbol
        latest_windows = df_seq.groupBy("symbol").agg(F.max("window_start").alias("window_start"))
        df_inference = df_seq.join(latest_windows, on=["symbol", "window_start"])

        # 5. PREDICT (Local Driver Loop)
        rows = df_inference.select("symbol", "window_start", "sequence").collect()
        
        if not rows:
            logging.info("No valid sequences found (Waiting for more data history...")
            return

        results = []
        logging.info(f"Generating predictions for {len(rows)} symbols...")
        
        for row in rows:
            # Convert [ [f1,f2,f3], [f1,f2,f3] ... ] to Tensor
            seq_array = np.array(row.sequence) 
            tensor_in = torch.tensor(seq_array, dtype=torch.float32).unsqueeze(0)
            
            with torch.no_grad():
                prob = model(tensor_in).item()
                
            # Logic: > 0.55 BUY, < 0.45 SELL, Else HOLD
            if prob > 0.55: signal = "BUY"
            elif prob < 0.45: signal = "SELL"
            else: signal = "HOLD"
            
            results.append((row.symbol, row.window_start, float(prob), signal))
            # print(f"   {row.symbol}: {prob:.4f} ({signal})")

        # 6. WRITE TO DELTA
        if results:
            res_df = spark.createDataFrame(results, ["symbol", "window_end", "probability", "signal"])
            res_df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://finance-lake/lake/predictions")
            logging.info(f"Saved {len(results)} predictions to Lake.")
            
    except Exception as e:
        logging.error(f"Inference cycle failed: {e}")

if __name__ == "__main__":
    logging.info("Starting AI Inference Service...")
    
    # Initialize Spark Once
    spark_session = get_spark_session()
    
    # Initialize Model Once
    loaded_model, loaded_stats = load_model_resources()
    
    if loaded_model is None:
        logging.error("FATAL: Could not load model. Train it first!")
        exit(1)

    # Infinite Loop (The "Real-Time" part)
    while True:
        start_ts = time.time()
        
        run_prediction_cycle(spark_session, loaded_model, loaded_stats)
        
        # Sleep until next cycle
        process_time = time.time() - start_ts
        sleep_time = max(0, POLL_INTERVAL_SECONDS - process_time)
        
        if sleep_time > 0:
            time.sleep(sleep_time)