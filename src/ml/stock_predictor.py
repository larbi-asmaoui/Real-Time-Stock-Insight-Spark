import os
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from delta import configure_spark_with_delta_pip

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ml.config import MLConfig
from ml.utils import setup_ml_logging


class StockPredictor:

        self.spark = spark
        self.config = config
        self.logger = setup_ml_logging()

    def load_gold(self):
        return df

    def build_features(self, df):


        w = Window.partitionBy("symbol").orderBy("window_end")


        df = df.fillna({
            "volatility": 0.0,
            "momentum": 0.0,
            "ma_4": 0.0,
            "avg_price_change_pct": 0.0
        })

        df_ml = df.select(
            "symbol",
            "avg_price",
            "volatility",
            "total_volume",
            "momentum",
            "ma_4",
            "avg_price_change_pct",
            "label"
        )

        df_ml = df_ml.na.drop(subset=["label"])
        
        self.logger.info(f"📐 Final ML dataset: {df_ml.count()} rows")
        return df_ml

    # ---------------------------------------------------------
    # 3) Train model
    # ---------------------------------------------------------
    def train_model(self, df_ml):

        self.logger.info("🤖 Training RandomForest...")

        assembler = VectorAssembler(
            inputCols=[
                "avg_price",
                "volatility",
                "total_volume",
                "momentum",
                "ma_4",
                "avg_price_change_pct"
            ],
            outputCol="features"
        )

        rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=5)

        pipeline = Pipeline(stages=[assembler, rf])

        train, test = df_ml.randomSplit([0.7, 0.3], seed=42)

        model = pipeline.fit(train)
        predictions = model.transform(test)

        evaluator = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction"
        )

        auc = evaluator.evaluate(predictions)
        self.logger.info(f"📊 AUC = {auc:.4f}")

        model.write().overwrite().save(self.config.MODEL_PATH)
        self.logger.info(f"💾 Model saved to {self.config.MODEL_PATH}")

        return model, auc

    # ---------------------------------------------------------
    # 4) Full pipeline
    # ---------------------------------------------------------
    def run(self):
        df_gold = self.load_gold()
        df_ml = self.build_features(df_gold)
        return self.train_model(df_ml)


if __name__ == "__main__":
    print("🚀 Starting ML Pipeline...")

    builder = (
        SparkSession.builder
            .appName("StockPredictor")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    config = MLConfig()
    predictor = StockPredictor(spark, config)

    predictor.run()