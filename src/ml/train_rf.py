from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, when
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from feature_engineering import transform_data
import shutil
import os
import logging

logging.basicConfig(
    filename='logs/train_rf.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w'
)

def train():
    spark = SparkSession.builder.appName("StockTrainRF").getOrCreate()
    
    logging.info("⏳ Reading Gold Data...")
    try:
        df = spark.read.format("delta").load("s3a://finance-lake/lake/gold")
    except:
        logging.error("No Gold data found. Skipping.")
        return

    # 1. Features
    df = transform_data(df)
    
    # 2. Target Generation (1 = Up, 0 = Down)
    w = Window.partitionBy("symbol").orderBy("window_start")
    df = df.withColumn("next_price", lead("avg_price", 1).over(w)) \
           .withColumn("label", when(col("next_price") > col("avg_price"), 1.0).otherwise(0.0)) \
           .na.drop()

    # 3. Assemble Vector
    # MLlib needs all features in one "vector" column
    feature_cols = ["log_returns", "volatility", "log_volume"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # 4. Define Random Forest
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=50)

    # 5. Pipeline
    pipeline = Pipeline(stages=[assembler, rf])

    # 6. Train
    logging.info("Training Random Forest...")
    # Split time-based (approx)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train_df)

    # 7. Evaluate (Optional)
    predictions = model.transform(test_df)
    accuracy = predictions.filter(col("prediction") == col("label")).count() / predictions.count()
    logging.info(f"Model Accuracy: {accuracy:.2%}")

    # 8. Save Native Spark Model
    model_path = "s3a://finance-lake/models/stock_rf"
    # We can write directly to S3 now because it's native Spark!
    logging.info(f"Saving model to {model_path}...")
    model.write().overwrite().save(model_path)
    logging.info("Done.")

if __name__ == "__main__":
    train()