"""
Central Configuration for Spark Streaming
BEST PRACTICE: Separate configuration from business logic
"""

class SparkConfig:
    """Optimized Spark Configuration for Financial Streaming"""
    
    # Kafka Configuration
    KAFKA_BROKERS = "kafka:29092"
    KAFKA_TOPIC = "stock_prices"
    KAFKA_STARTING_OFFSETS = "earliest"  # or "earliest" for history
    
    # Spark Configuration
    APP_NAME = "StockInsightStreaming"
    LOG_LEVEL = "WARN"
    
    # Performance Tuning
    MAX_OFFSETS_PER_TRIGGER = 10000
    PROCESSING_TIME = "0 seconds" # Minimize latency
    
    # Storage paths for medallion architecture
    BRONZE_INIT_DELAY = 45
    SILVER_INIT_DELAY = 45
    BASE_PATH = "/app/data"
    BRONZE_PATH = f"{BASE_PATH}/lake/bronze"
    SILVER_PATH = f"{BASE_PATH}/lake/silver"
    GOLD_PATH = f"{BASE_PATH}/lake/gold"
    
    # Windowing Configuration
    WINDOW_DURATION = "10 seconds"
    SLIDE_DURATION = "5 seconds"  # Overlapping windows
    WATERMARK_DELAY = "1 minute"
    
    # Spark SQL Optimization
    SHUFFLE_PARTITIONS = 4
    ADAPTIVE_ENABLED = True
    
    # Delta Lake Configuration
    DELTA_MERGE_SCHEMA = True
    DELTA_AUTO_OPTIMIZE = True
    
    @staticmethod
    def get_spark_configs():
        """Returns a dict of Spark configurations"""
        return {
           
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0",
            
            # --- DELTA LAKE CONFIGURATION ---
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.schema.autoMerge.enabled": str(SparkConfig.DELTA_MERGE_SCHEMA).lower(),
            
            "spark.sql.streaming.checkpointLocation.root": "/app/data/checkpoints",
            "spark.sql.streaming.minBatchesToRetain": "10",
            
            "spark.sql.streaming.schemaInference": "false",
            "spark.sql.adaptive.enabled": "true",
            
            "spark.sql.streaming.schemaInference": "false",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.streaming.kafka.consumer.poll.ms": "512",
            "spark.sql.shuffle.partitions": str(SparkConfig.SHUFFLE_PARTITIONS),
            "spark.default.parallelism": "200",
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "false"
        }