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
    
    # def start_streaming(self, write_mode="medallion"):
    #     spark = self.create_spark_session(conf=SparkConfig.get_spark_configs())

    #     # read raw Kafka stream
    #     raw = (
    #         spark.readStream
    #         .format("kafka")
    #         .option("kafka.bootstrap.servers", self.config.KAFKA_BROKERS)
    #         .option("subscribe", self.config.KAFKA_TOPIC)
    #         .option("startingOffsets", self.config.KAFKA_STARTING_OFFSETS)
    #         .option("failOnDataLoss", "false")
    #         .load()
    #     )

    #     # parse value (assumes JSON string payload)
    #     parsed = (
    #         raw.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
    #            .withColumn("data", from_json(col("json_str"), self.schemas.get_input_schema()))
    #            .select("data.*", "kafka_timestamp")
    #            .withColumn("timestamp", to_timestamp(col("timestamp")))
    #            .withColumn("date_partition", date_format(col("kafka_timestamp"), "yyyy-MM-dd"))
    #     )

    #     # Bronze: raw JSON persisted as delta (append)
    #     bronze_path = f"{self.config.OUTPUT_PATH}/bronze"
    #     bronze_checkpoint = f"{self.config.CHECKPOINT_LOCATION}/bronze"
    #     bronze_q = (
    #         parsed.writeStream
    #         .format("delta")
    #         .option("checkpointLocation", bronze_checkpoint)
    #         .partitionBy("date_partition")
    #         .outputMode("append")
    #         .start(bronze_path)
    #     )

    #     # Silver: cleaned, typed, deduped
    #     silver_df = (
    #         parsed
    #         .dropDuplicates(["symbol", "timestamp"])
    #         .filter(col("price").isNotNull())
    #     )
    #     silver_path = f"{self.config.OUTPUT_PATH}/silver"
    #     silver_checkpoint = f"{self.config.CHECKPOINT_LOCATION}/silver"
    #     silver_q = (
    #         silver_df.writeStream
    #         .format("delta")
    #         .option("checkpointLocation", silver_checkpoint)
    #         .partitionBy("date_partition")
    #         .outputMode("append")
    #         .start(silver_path)
    #     )

    #     # Gold: aggregated business metrics (hourly example)
    #     gold_df = (
    #         silver_df.withWatermark("timestamp", "1 minute")
    #         .groupBy(window(col("timestamp"), "1 hour"), col("symbol"))
    #         .agg(
    #             {"price": "avg", "price": "max", "price": "min"}
    #         )
    #     )
    #     gold_path = f"{self.config.OUTPUT_PATH}/gold"
    #     gold_checkpoint = f"{self.config.CHECKPOINT_LOCATION}/gold"
    #     gold_q = (
    #         gold_df.writeStream
    #         .format("delta")
    #         .option("checkpointLocation", gold_checkpoint)
    #         .outputMode("complete")
    #         .start(gold_path)
    #     )

    #     # keep streaming until stopped
    #     try:
    #         spark.streams.awaitAnyTermination()
    #     except KeyboardInterrupt:
    #         for q in (bronze_q, silver_q, gold_q):
    #             if q and q.isActive:
    #                 q.stop()
    
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
        # query = (
        #     result_df.writeStream
        #     .outputMode("complete")
        #     .format("console")
        #     .option("truncate", "false")
        #     .trigger(processingTime="10 seconds")
        #     .start()
        # )
        
        # write also in delta lake
        query = (
            result_df.writeStream
            .outputMode("complete")
            .format("delta")
            .option("checkpointLocation", f"{self.config.CHECKPOINT_LOCATION}/gold")
            .trigger(processingTime="10 seconds")
            .start(f"{self.config.OUTPUT_PATH}/gold")
        )

        logger.info("Streaming is started ! Spark UI: http://localhost:4040")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stop streaming...")
            query.stop()