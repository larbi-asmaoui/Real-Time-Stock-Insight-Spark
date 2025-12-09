import os
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel
from delta import configure_spark_with_delta_pip

# Allow access to config/utils if needed
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ml.utils import setup_ml_logging

# --- 1. Spark Setup ---
builder = (
    SparkSession.builder
    .appName("StockInference")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
logger = setup_ml_logging()

# --- 2. Load Model and Data ---
logger.info("📥 Loading Model and Data...")
MODEL_PATH = "/app/data/ml/model"
GOLD_PATH = "/app/data/lake/gold"
PREDICTIONS_PATH = "/app/data/lake/predictions"

# Load the trained PIPELINE model
# This model now contains the Assembler + RandomForest combined
model = PipelineModel.load(MODEL_PATH)

# Load new data (from Gold layer)
df = spark.read.format("delta").load(GOLD_PATH)

# --- 3. Feature Engineering ---
# We only prepare calculated columns (MA, Momentum...)
# The VectorAssembler is already included inside the pipeline model
logger.info("🛠 Preparing features...")
w = Window.partitionBy("symbol").orderBy("window_end")

df_features = (
    df.withColumn("prev_price", F.lag("avg_price").over(w))
      .withColumn("momentum", F.col("avg_price") - F.col("prev_price"))
      .withColumn("ma_4", F.avg("avg_price").over(w.rowsBetween(-3, 0)))
      # Fill null values to prevent errors
      .fillna({"momentum": 0.0, "ma_4": 0.0, "volatility": 0.0, "avg_price_change_pct": 0.0})
)

# --- 4. Prediction ---
logger.info("🔮 Running Predictions...")
# The model takes df_features, passes it through its internal VectorAssembler, then through RandomForest
predictions = model.transform(df_features)

# --- 5. Select and Save Results ---
final_results = predictions.select(
    "symbol",
    "window_end",
    "avg_price",
    "prediction",     # 1 = Up, 0 = Down
    "probability"     # Prediction confidence
)

# Save to a new Delta table
logger.info(f"💾 Saving predictions to {PREDICTIONS_PATH} ...")
(final_results.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PREDICTIONS_PATH)
)

logger.info("✅ Inference Job Completed Successfully!")