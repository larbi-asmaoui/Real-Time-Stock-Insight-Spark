import argparse
import os
import json
import pandas as pd
import numpy as np
import mlflow.pytorch
import torch
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.window import Window

from feature_engineering import transform_data

# --- Global Cache for Model/Stats on Executors ---
class ModelCache:
    model = None
    stats = None

def load_resources(model_path, stats_path):
    if ModelCache.model is None:
        # Load Model
        # If path is local to driver but not executors, this fails. 
        # In a real cluster, use SparkFiles or shared storage (DFS/NFS).
        # We assume shared storage "/app/data" per PROMPT context.
        device = torch.device("cpu") # Inference on CPU usually for spark workers unless GPU configured
        ModelCache.model = mlflow.pytorch.load_model(f"file://{model_path}", map_location=device)
        ModelCache.model.eval()
        
    if ModelCache.stats is None:
        with open(stats_path, 'r') as f:
            ModelCache.stats = json.load(f)

def inference_function(key, pdf: pd.DataFrame, model_path, stats_path, seq_len):
    """
    Pandas UDF function to run for each symbol.
    """
    load_resources(model_path, stats_path)
    
    # 1. Sort by time (crucial for LSTM)
    pdf = pdf.sort_values("window_start")
    
    # 2. Preprocessing
    # Features are: log_returns, volatility, log_volume
    cols = ["log_returns", "volatility", "log_volume"]
    
    # Scale
    for c in cols:
        m = ModelCache.stats[c]["mean"]
        s = ModelCache.stats[c]["std"]
        if s == 0: s = 1.0
        pdf[c] = (pdf[c] - m) / s
        
    # 3. Create Sequence
    # We need the LAST valid sequence
    # Drop NaNs (from lags)
    pdf = pdf.dropna(subset=cols)
    
    if len(pdf) < seq_len:
        # Not enough data to predict
        return pd.DataFrame()
        
    # Take last SEQ_LEN rows
    seq = pdf.tail(seq_len)[cols].values
    
    # 4. Predict
    X = torch.tensor(seq, dtype=torch.float32).unsqueeze(0) # (1, seq_len, 3)
    
    with torch.no_grad():
        prob = ModelCache.model(X).item()
        
    # 5. Return Result
    return pd.DataFrame({
        "symbol": [key[0]], # key is tuple
        "app_prediction": [prob],
        "prediction_timestamp": [pd.Timestamp.now()]
    })


def run_inference(spark, config):
    # 1. Configuration
    gold_path = config["GOLD_PATH"]
    predictions_path = "/app/data/lake/predictions"
    
    # 2. Read Gold Data
    df = spark.read.format("delta").load(gold_path)
    
    # 3. Filter Latest Window
    # Strategy: Rank by time desc, take top (SEQ_LEN + Buffer)
    w = Window.partitionBy("symbol").orderBy(F.col("window_start").desc())
    rows_needed = config["SEQ_LEN"] + 20 # buffer for lags
    
    df_filtered = df.withColumn("rank", F.row_number().over(w)) \
                    .filter(F.col("rank") <= rows_needed) \
                    .drop("rank")
                    
    # 4. Apply Feature Engineering
    df_features = transform_data(df_filtered)
    
    # 5. Execute Inference
    # We pass the paths via closure or config. 
    # applyInPandas takes a function. We can use functools.partial or a wrapper.
    
    result_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("app_prediction", FloatType(), True),
        StructField("prediction_timestamp", TimestampType(), True)
    ])
    
    # Wrapper to unpack arguments
    def inference_wrapper(key, pdf):
        return inference_function(key, pdf, 
                                  config["MODEL_PATH"], 
                                  config["STATS_PATH"], 
                                  config["SEQ_LEN"])

    predictions = df_features.groupBy("symbol").applyInPandas(inference_wrapper, schema=result_schema)
    
    # 6. Write to Delta
    predictions.write.format("delta").mode("append").save(predictions_path)
    print(f"Predictions written to {predictions_path}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("StockInference") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    # Typically these come from CLI args or persistent config
    config = {
        "GOLD_PATH": "/app/data/lake/gold",
        "SEQ_LEN": 10,
        "MODEL_PATH": "/app/data/mlruns/0/latest/artifacts/model", # Adjust as needed
        "STATS_PATH": "/app/data/mlruns/0/latest/artifacts/metrics.json" # Adjust as needed
    }
    
    # Ensure paths are valid (In a real scenario, we might resolve 'latest' dynamically)
    # For now, we assume the user/airflow sets these correctly.
    
    run_inference(spark, config)
