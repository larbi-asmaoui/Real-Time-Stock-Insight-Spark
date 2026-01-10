
import time
import json
import logging
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, max
from feature_engineering import transform_data
import pyspark.sql.functions as F



logging.basicConfig(
    filename='logs/stock_inference.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w'
)


def run_inference():
    spark = SparkSession.builder.appName("StockInferenceRF").getOrCreate()
    
    # 1. Load Model
    model_path = "s3a://finance-lake/models/stock_rf"
    try:
        model = PipelineModel.load(model_path)
    except Exception as e:
        logging.error(f"Could not load model: {e}")
        return

    # 2. Read Latest Gold
    df = spark.read.format("delta").load("s3a://finance-lake/lake/gold")
    
    # 3. Features
    df_feat = transform_data(df)
    
    # 4. Get Latest Row per Symbol
    latest_times = df_feat.groupBy("symbol").agg(max("window_start").alias("window_start"))
    df_latest = df_feat.join(latest_times, on=["symbol", "window_start"])
    
    # 5. Predict (Native Spark!)
    # The model adds a "probability" vector column
    predictions = model.transform(df_latest)
    
    # 6. Extract Probability
    # Spark outputs [prob_down, prob_up]. We want prob_up (index 1).
    # We use a Vector UDF to extract it.
    from pyspark.ml.functions import vector_to_array
    
    final_df = predictions \
        .withColumn("probs", vector_to_array("probability")) \
        .select(
            col("symbol"), 
            col("window_start").alias("window_end"),
            col("probs")[1].alias("probability")
        ) \
        .withColumn("signal", when(col("probability") > 0.55, "BUY")
                             .when(col("probability") < 0.45, "SELL")
                             .otherwise("HOLD"))

    # 7. Write
    final_df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://finance-lake/lake/predictions")
    logging.info("Predictions written.")

if __name__ == "__main__":
    while True:
        logging.info("Running Inference...")
        run_inference()
        time.sleep(10)