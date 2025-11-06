from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from processing.config import SparkConfig
from processing.schemas import StockSchemas
from processing.spark_streaming_utils import setup_logging, get_sql_queries

from pyspark.sql.types import ArrayType

logger = setup_logging()

class StockStreamingProcessor:
    def __init__(self, config=None):
        self.config = config or SparkConfig()
        self.spark = None
        self.streaming_query = None
        self.schemas = StockSchemas()
        
    

    def create_spark_session(self):
        logger.info("Initialisation of session Spark...")
        builder = SparkSession.builder.appName(self.config.APP_NAME)
        for k, v in self.config.get_spark_configs().items():
            builder = builder.config(k, v)
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(self.config.LOG_LEVEL)
        logger.info(f"Session Spark created: {self.spark.version}")
        return self.spark

    def run_spark_sql_analysis(self, view_name="stock_analytics"):
        """Exécute les requêtes SQL d'analyse"""
        queries = get_sql_queries(view_name)
        return queries
    
    def start_streaming(self, write_mode="console"):
        spark = self.create_spark_session()
        
        logger.info("🚦 Lancement du streaming depuis Kafka...")
        
        input_schema = self.schemas.get_input_schema()  # StructType for one record

        # Read From Kafka
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.KAFKA_BROKERS)
            .option("subscribe", self.config.KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )
        
        df_parsed = (
                df.selectExpr("CAST(value AS STRING) as json_data")
                .select(from_json(col("json_data"), ArrayType(input_schema)).alias("data"))
                .withColumn("data", explode(col("data")))
                .select("data.*")
)

        # Créer une vue temporaire
        df_parsed.createOrReplaceTempView("stock_analytics")

        # Agrégation simple
        result_df = spark.sql("""
            SELECT 
                symbol,
                COUNT(*) as total_records,
                ROUND(AVG(price), 2) AS avg_price,
                MAX(price) as max_price,
                MIN(price) as min_price
            FROM stock_analytics
            GROUP BY symbol
        """)

        # Écriture avec gestion d'erreurs
        query = (
            result_df.writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .trigger(processingTime="10 seconds")
            .start()
        )

        logger.info("Streaming is started ! Spark UI: http://localhost:4040")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stop streaming...")
            query.stop()