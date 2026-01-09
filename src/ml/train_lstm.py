import os
import json
import shutil
import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, array, lit, lead
from pyspark.sql.window import Window

from feature_engineering import transform_data
from dataset import StockDataset

# --- HYPERPARAMETERS ---
SEQ_LEN = 5
HIDDEN_SIZE = 64
EPOCHS = 10
BATCH_SIZE = 32
LR = 0.001

class LSTMModel(nn.Module):
    def __init__(self, input_size):
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_size, HIDDEN_SIZE, batch_first=True)
        self.fc = nn.Linear(HIDDEN_SIZE, 1)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :]) # Last time step
        return self.sigmoid(out)

def get_spark_session():
    """Initializes Spark with S3 Support for Training"""
    return SparkSession.builder \
        .appName("StockPredictionTrain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def train():
    spark = get_spark_session()
    
    # 1. READ GOLD FROM S3
    print("⏳ Reading Gold Data from MinIO...")
    try:
        df = spark.read.format("delta").load("s3a://finance-lake/lake/gold")
    except Exception as e:
        print(f"❌ Could not read Gold data. Is the pipeline running? Error: {e}")
        return

    # 2. FEATURE ENGINEERING
    df = transform_data(df)
    
    # 3. COMPUTE SCALING STATS (Global)
    # We calculate mean/std on the whole dataset for this MVP
    feature_cols = ["log_returns", "volatility", "log_volume"]
    stats = {}
    
    print("📊 Computing Stats...")
    for c in feature_cols:
        row = df.selectExpr(f"avg({c}) as mean", f"stddev({c}) as std").collect()[0]
        # Handle cases where std might be null or 0
        mean_val = row["mean"] or 0.0
        std_val = row["std"] or 1.0
        if std_val == 0: std_val = 1.0
        stats[c] = {"mean": mean_val, "std": std_val}

    # Save stats locally (to be logged to MLflow)
    with open("metrics.json", "w") as f:
        json.dump(stats, f)

    # 4. PREPARE VECTORS & SEQUENCES
    # Scale Data
    for c in feature_cols:
        m = stats[c]["mean"]
        s = stats[c]["std"]
        df = df.withColumn(c, (col(c) - m) / s)

    w_seq = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-SEQ_LEN + 1, 0)
    w_target = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(1, 1)

    # Generate Sequence Arrays and Target
    # Target = 1.0 if Next Price > Current Price, else 0.0
    w_lead = Window.partitionBy("symbol").orderBy("window_start")
    df_seq = df \
        .withColumn("feature_vec", array(feature_cols)) \
        .withColumn("features", collect_list("feature_vec").over(w_seq)) \
        .withColumn("next_price", lead("avg_price", 1).over(w_lead)) \
        .withColumn("target", (col("next_price") > col("avg_price")).cast("float")) \
        .filter(col("target").isNotNull()) \
        .filter(f"size(features) = {SEQ_LEN}")

    # 5. MATERIALIZE TO LOCAL DISK (Speed Optimization)
    # We save to the container's local disk so PyTorch reads fast.
    # We do NOT save training data back to S3.
    local_train_dir = "/app/data/temp_train"
    if os.path.exists(local_train_dir): shutil.rmtree(local_train_dir)
    
    print(f"💾 Materializing {df_seq.count()} samples to {local_train_dir}...")
    df_seq.select("features", "target").coalesce(1).write.parquet(local_train_dir)

    # 6. PYTORCH TRAINING LOOP
    print("🚀 Starting Training...")
    
    # Configure MLflow to use the S3 bucket we created
    mlflow.set_tracking_uri("file:///app/mlruns")
    mlflow.set_experiment("Stock_Prediction_LSTM")

    with mlflow.start_run():
        mlflow.log_params({"lr": LR, "epochs": EPOCHS, "seq_len": SEQ_LEN})
        mlflow.log_artifact("metrics.json") # CRITICAL: Save scaling logic with model

        dataset = StockDataset(local_train_dir)
        if len(dataset.file_paths) == 0:
            print("❌ No training data found.")
            return

        loader = DataLoader(dataset, batch_size=BATCH_SIZE)
        model = LSTMModel(input_size=len(feature_cols))
        optimizer = torch.optim.Adam(model.parameters(), lr=LR)
        criterion = nn.BCELoss()

        for epoch in range(EPOCHS):
            total_loss = 0
            steps = 0
            for X, y in loader:
                optimizer.zero_grad()
                pred = model(X)
                loss = criterion(pred, y)
                loss.backward()
                optimizer.step()
                total_loss += loss.item()
                steps += 1
            
            avg_loss = total_loss / steps if steps > 0 else 0
            print(f"Epoch {epoch+1}: Loss = {avg_loss:.4f}")
            mlflow.log_metric("loss", avg_loss, step=epoch)

        # Log Model
        mlflow.pytorch.log_model(model, "model")
        print("✅ Model trained and saved to MLflow.")

if __name__ == "__main__":
    train()