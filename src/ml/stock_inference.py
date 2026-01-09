import mlflow.pytorch
import torch
import json
import pandas as pd 
import numpy as np 
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, collect_list
from pyspark.sql.window import Window
from feature_engineering import transform_data

def get_spark_session():
    return SparkSession.builder \
        .appName("StockInference") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def run_inference():
    spark = get_spark_session()
    
    # 1. LOAD MODEL & METRICS FROM MLFLOW
    mlflow.set_tracking_uri("file:///app/mlruns")
    try:
        # Get latest successful run
        runs = mlflow.search_runs(experiment_names=["Stock_Prediction_LSTM"], order_by=["start_time DESC"], max_results=1)
        if runs.empty:
            print("❌ No training runs found.")
            return
            
        run_id = runs.iloc[0].run_id
        print(f"📥 Loading Model from Run ID: {run_id}")
        
        model_uri = f"runs:/{run_id}/model"
        model = mlflow.pytorch.load_model(model_uri)
        
        # Load the metrics.json artifact to get scaling params
        local_path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="metrics.json")
        with open(local_path) as f:
            stats = json.load(f)
            
    except Exception as e:
        print(f"❌ Error loading model: {e}")
        return

    # 2. READ LIVE DATA (Gold Layer)
    print("Reading Gold data...")
    try:
        # Optimization: Only read enough data for the sequence
        # In a real batch job, you'd filter by time. For now, read all.
        df = spark.read.format("delta").load("s3a://finance-lake/lake/gold")
    except:
        print("Gold table not found.")
        return

    # 3. FEATURE ENGINEERING (Apply same logic)
    df = transform_data(df)
    
    # Apply Scaling
    feature_cols = ["log_returns", "volatility", "log_volume"]
    for c in feature_cols:
        m = stats[c]["mean"]
        s = stats[c]["std"]
        df = df.withColumn(c, (col(c) - m) / s)

    # 4. PREPARE LATEST SEQUENCES
    SEQ_LEN = 5
    w = Window.partitionBy("symbol").orderBy("window_start")
    
    # Collect history for every row
    df_seq = df \
        .withColumn("feature_vec", array(feature_cols)) \
        .withColumn("sequence", collect_list("feature_vec").over(w.rowsBetween(-SEQ_LEN + 1, 0))) \
        .withColumn("seq_len", F.size(col("sequence"))) \
        .filter(col("seq_len") == SEQ_LEN)

    # Get ONLY the latest window for each symbol to predict "What happens next?"
    latest_windows = df_seq.groupBy("symbol").agg(F.max("window_start").alias("window_start"))
    df_inference = df_seq.join(latest_windows, on=["symbol", "window_start"])

    # 5. PREDICT (Driver-side Loop for Simplicity)
    # Since we only have ~10 symbols, we collect to driver and infer.
    # For 1M symbols, use a Pandas UDF.
    
    rows = df_inference.select("symbol", "window_start", "sequence").collect()
    results = []
    
    print(f"🔮 Generating predictions for {len(rows)} symbols...")
    for row in rows:
        # Prepare Tensor (1, Seq_Len, Features)
        seq_array = np.array(row.sequence)
        tensor_in = torch.tensor(seq_array, dtype=torch.float32).unsqueeze(0)
        
        with torch.no_grad():
            prob = model(tensor_in).item()
            
        # Decision: > 0.5 = BUY, < 0.5 = SELL
        signal = "BUY" if prob > 0.5 else "SELL"
        results.append((row.symbol, row.window_start, float(prob), signal))
        print(f"   {row.symbol}: {prob:.4f} ({signal})")

    # 6. WRITE PREDICTIONS TO LAKE
    if results:
        res_df = spark.createDataFrame(results, ["symbol", "window_end", "probability", "signal"])
        res_df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://finance-lake/lake/predictions")
        print("✅ Predictions saved to s3a://finance-lake/lake/predictions")

if __name__ == "__main__":
    run_inference()