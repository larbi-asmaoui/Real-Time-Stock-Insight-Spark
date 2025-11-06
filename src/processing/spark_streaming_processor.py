# spark_streaming_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from processing.config import SparkConfig
from processing.schemas import StockSchemas
from processing.spark_streaming_utils import setup_logging, get_sql_queries

logger = setup_logging()

class StockStreamingProcessor:
    def __init__(self, config=None):
        self.config = config or SparkConfig()
        self.spark = None
        self.streaming_query = None
        self.schemas = StockSchemas()

    def create_spark_session(self):
        logger.info("🚀 Initialisation de la session Spark...")
        builder = SparkSession.builder.appName(self.config.APP_NAME)
        for k, v in self.config.get_spark_configs().items():
            builder = builder.config(k, v)
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(self.config.LOG_LEVEL)
        logger.info(f"✅ Session Spark créée: {self.spark.version}")
        return self.spark

    def run_spark_sql_analysis(self, view_name="stock_analytics"):
        """Exécute les requêtes SQL d'analyse"""
        queries = get_sql_queries(view_name)
        return queries
    
    def start_streaming(self, write_mode="console"):
            
        spark = self.create_spark_session()
        
        logger.info("🚦 Lancement du streaming depuis Kafka...")

        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.KAFKA_BROKERS)
            .option("subscribe", self.config.KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )

        df_parsed = df.selectExpr("CAST(value AS STRING) as json_data")

        df_clean = df_parsed.withColumn("timestamp", current_timestamp())

        df_clean.createOrReplaceTempView("stock_analytics")

        result_df = spark.sql("SELECT COUNT(*) AS total_rows FROM stock_analytics")

        if write_mode == "console":
            self.streaming_query = (
                result_df.writeStream
                .outputMode("complete")
                .format("console")
                .start()
            )
        elif write_mode == "memory":
            self.streaming_query = (
                result_df.writeStream
                .outputMode("complete")
                .format("memory")
                .queryName("stock_stats")
                .start()
            )
        else:
            logger.warning(f"❌ Mode d’écriture inconnu: {write_mode}")
            return

        logger.info("✅ Streaming démarré ! Consultez http://localhost:4040 pour Spark UI.")
        self.streaming_query.awaitTermination()
