from pyspark.sql.functions import (
    col, current_timestamp, to_timestamp, when
)
from processing.spark_streaming_utils import setup_logging

logger = setup_logging()

class SilverLayer:
    def __init__(self, spark, config, schemas):
        self.spark = spark
        self.config = config
        self.schemas = schemas
    
    def create_stream(self):
        logger.info("🥈 SILVER - Enrichissement données...")
        
        # Delta infère automatiquement depuis les métadonnées
        bronze_df = (
            self.spark.readStream
            .format("delta")
            .option("startingVersion", "0")
            .option("ignoreChanges", "true")  # Pour les updates
            .option("ignoreDeletes", "true")  # Pour les deletes
            .load(self.config.BRONZE_PATH)
        )
        
        logger.info(f"✅ Lecture Bronze → {self.config.BRONZE_PATH}")
        
        # Transformations Silver
        silver_df = (
            bronze_df
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            
            # Métriques calculées
            .withColumn("spread", col("high") - col("low"))
            .withColumn("price_change", col("price") - col("open"))
            .withColumn(
                "price_change_pct",
                when(col("open") > 0, (col("price_change") / col("open") * 100))
                .otherwise(0.0)
            )
            
            # Détection anomalies
            .withColumn(
                "is_anomaly",
                when(
                    (col("volume") == 0) |
                    (col("price") <= 0) |
                    (col("open") <= 0) |
                    (col("high") < col("low")),
                    True
                ).otherwise(False)
            )
            
            .withColumn("silver_processed_at", current_timestamp())
            # FIX: Added watermark to limit state size for dropDuplicates
            .withWatermark("timestamp", "10 minutes")
            .dropDuplicates(["symbol", "timestamp"])
        )
        
        # Native Delta Sink (Optimized)
        query = (
            silver_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("mergeSchema", "true")
            .partitionBy("symbol")
            .option("checkpointLocation", f"{self.config.BASE_PATH}/checkpoints/silver")
            .trigger(processingTime=self.config.PROCESSING_TIME)
            .queryName("silver_enrichment")
            .start(self.config.SILVER_PATH)
        )
        
        logger.info(f"✅ Silver actif → {self.config.SILVER_PATH}")
        return query