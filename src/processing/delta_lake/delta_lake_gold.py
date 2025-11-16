from pyspark.sql.functions import (
    col, current_timestamp, window, count, avg, 
    min as spark_min, max as spark_max, sum as spark_sum, 
    stddev, round as spark_round
)
from processing.spark_streaming_utils import setup_logging

logger = setup_logging()

class GoldLayer:
    def __init__(self, spark, config, schemas):
        self.spark = spark
        self.config = config
        self.schemas = schemas
    
    def create_stream(self):
        logger.info("🥇 GOLD - Agrégations business...")
        
        # 🔑 FIX: Delta infère le schéma automatiquement
        silver_df = (
            self.spark.readStream
            .format("delta")
            # NE PAS FAIRE: .schema(silver_schema)
            .option("startingVersion", "0")
            .option("ignoreChanges", "true")
            .option("ignoreDeletes", "true")
            .load(self.config.SILVER_PATH)
        )
        
        logger.info(f"✅ Lecture Silver → {self.config.SILVER_PATH}")
        
        # Agrégations avec fenêtres
        gold_df = (
            silver_df
            .filter(col("is_anomaly") == False)
            .withWatermark("timestamp", self.config.WATERMARK_DELAY)
            
            .groupBy(
                window(col("timestamp"), self.config.WINDOW_DURATION),
                col("symbol")
            )
            .agg(
                count("*").alias("trade_count"),
                spark_round(avg("price"), 2).alias("avg_price"),
                spark_round(spark_min("price"), 2).alias("min_price"),
                spark_round(spark_max("price"), 2).alias("max_price"),
                spark_sum("volume").alias("total_volume"),
                spark_round(stddev("price"), 4).alias("volatility"),
                spark_round(avg("price_change_pct"), 2).alias("avg_price_change_pct")
            )
            
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("symbol"),
                col("avg_price"),
                col("min_price"),
                col("max_price"),
                col("volatility"),
                col("total_volume"),
                col("trade_count"),
                col("avg_price_change_pct")
            )
            .withColumn("gold_computed_at", current_timestamp())
        )
        
        # Écriture Gold
        def write_gold(batch_df, batch_id):
            if batch_df.isEmpty():
                logger.info(f"📦 Gold batch {batch_id}: vide")
                return
                
            count = batch_df.count()
            symbols = batch_df.select("symbol").distinct().count()
            
            logger.info(f"📦 Gold batch {batch_id}: {count} fenêtres, {symbols} symboles")
            
            try:
                (
                    batch_df.write
                    .format("delta")
                    .mode("append")
                    .partitionBy("symbol")
                    .option("mergeSchema", "true")
                    .save(self.config.GOLD_PATH)
                )
                logger.info(f"✅ Gold batch {batch_id} écrit avec succès")
            except Exception as e:
                logger.error(f"❌ Erreur Gold batch {batch_id}: {e}")
                raise
        
        query = (
            gold_df.writeStream
            .foreachBatch(write_gold)
            .trigger(processingTime=self.config.PROCESSING_TIME)
            .option("checkpointLocation", f"{self.config.BASE_PATH}/checkpoints/gold")
            .queryName("gold_aggregations")
            .start()
        )
        
        logger.info(f"✅ Gold actif → {self.config.GOLD_PATH}")
        return query