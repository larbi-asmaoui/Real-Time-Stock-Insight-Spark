from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

class StockPredictor:

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    # ==========================================================
    # 1️⃣ Lecture données GOLD
    # ==========================================================
    def load_gold(self):
        print("📥 Loading GOLD data...")
        df = (
            self.spark.read
            .format("delta")
            .load(self.config.GOLD_PATH)
        )
        return df

    # ==========================================================
    # 2️⃣ Feature engineering (ML)
    # ==========================================================
    def build_features(self, df):

        print("🛠 Building ML features...")

        w = Window.partitionBy("symbol").orderBy("window_end")

        # Label: 1 si next_price > avg_price
        df = df.withColumn("next_price", F.lead("avg_price", 1).over(w))
        df = df.withColumn("label",
                           (F.col("next_price") > F.col("avg_price")).cast("int"))

        # Momentum
        df = df.withColumn("prev_price", F.lag("avg_price", 1).over(w))
        df = df.withColumn("momentum",
                           F.col("avg_price") - F.col("prev_price"))

        # Moving average MA(4)
        w4 = w.rowsBetween(-3, 0)
        df = df.withColumn("ma_4", F.avg("avg_price").over(w4))

        # Nettoyage valeurs nulles
        df = df.fillna({
            "volatility": 0.0,
            "momentum": 0.0,
            "ma_4": 0.0,
            "avg_price_change_pct": 0.0
        })

        # Dataset final ML
        df_ml = df.select(
            "symbol",
            "avg_price",
            "volatility",
            "total_volume",
            "momentum",
            "ma_4",
            "avg_price_change_pct",
            "label"
        ).dropna()

        return df_ml

    # ==========================================================
    # 3️⃣ Training RandomForest
    # ==========================================================
    def train_model(self, df_ml):

        print("🤖 Training RandomForest model...")

        # Assemble features into vector
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

        rf = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            numTrees=50
        )

        pipeline = Pipeline(stages=[assembler, rf])

        train, test = df_ml.randomSplit([0.7, 0.3], seed=42)

        model = pipeline.fit(train)

        predictions = model.transform(test)

        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )

        auc = evaluator.evaluate(predictions)

        print(f"📊 Model AUC = {auc:.4f}")

        # Sauvegarde modèle en Delta Lake / MLlib format
        model.save(self.config.ML_MODEL_PATH)

        print(f"💾 Model saved to {self.config.ML_MODEL_PATH}")

        return model, auc

    # ==========================================================
    # 4️⃣ Execute full ML pipeline
    # ==========================================================
    def run(self):
        df_gold = self.load_gold()
        df_ml = self.build_features(df_gold)
        return self.train_model(df_ml)
