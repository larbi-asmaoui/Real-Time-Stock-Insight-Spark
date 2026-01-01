from pyspark.sql.functions import (
    col, from_json, explode, current_timestamp
)
from pyspark.sql.types import ArrayType
from processing.spark_streaming_utils import setup_logging

logger = setup_logging()

class BronzeLayer:
    def __init__(self, spark, config, schemas):
        self.spark = spark
        self.config = config
        self.schemas = schemas
    
    def create_stream(self):
        logger.info("🥉 BRONZE - Démarrage ingestion Kafka...")
        
        input_schema = self.schemas.get_input_schema()
        # Lecture Kafka
        raw_df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.KAFKA_BROKERS)
            .option("subscribe", self.config.KAFKA_TOPIC)
            .option("startingOffsets", self.config.KAFKA_STARTING_OFFSETS)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", self.config.MAX_OFFSETS_PER_TRIGGER)
            .load()
        )
        
        
        parsed_df = (
            raw_df
            .selectExpr(
                "CAST(value AS STRING) as json_data",
                "timestamp as kafka_timestamp"
            )
            .select(
                from_json(col("json_data"), input_schema).alias("record"),
                col("kafka_timestamp")
            )
            .select(
                "record.*",
                col("kafka_timestamp").alias("ingestion_timestamp")
            )
            .withColumn("bronze_inserted_at", current_timestamp())
        )
        
        # Native Delta Sink (Optimized)
        query = (
            parsed_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("mergeSchema", "true")
            .partitionBy("symbol")
            .option("checkpointLocation", f"{self.config.BASE_PATH}/checkpoints/bronze")
            .trigger(processingTime=self.config.PROCESSING_TIME)
            .queryName("bronze_ingestion")
            .start(self.config.BRONZE_PATH)
        )
        
        logger.info(f"✅ Bronze actif → {self.config.BRONZE_PATH}")
        return query