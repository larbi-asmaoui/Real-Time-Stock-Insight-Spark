"""
Configuration centrale pour Spark Streaming
BEST PRACTICE: Séparer la configuration du code métier
"""

class SparkConfig:
    """Configuration Spark optimisée pour streaming financier"""
    
    # Kafka Configuration
    KAFKA_BROKERS = "kafka:29092"
    KAFKA_TOPIC = "stock_prices"
    KAFKA_STARTING_OFFSETS = "latest"  # ou "earliest" pour historique
    
    # Spark Configuration
    APP_NAME = "StockInsightStreaming"
    LOG_LEVEL = "WARN"
    
    # Performance Tuning
    MAX_OFFSETS_PER_TRIGGER = 10000
    PROCESSING_TIME = "10 seconds"
    
    # Checkpoint & Storage
    CHECKPOINT_LOCATION = "/app/checkpoints/stock_streaming"
    OUTPUT_PATH = "/app/data/processed_stocks"
    
    # Windowing Configuration
    WINDOW_DURATION = "10 seconds"
    SLIDE_DURATION = "5 seconds"  # Overlapping windows
    WATERMARK_DELAY = "1 minute"
    
    # Spark SQL Optimization
    SHUFFLE_PARTITIONS = 200
    ADAPTIVE_ENABLED = True
    
    @staticmethod
    def get_spark_configs():
        """Retourne un dict de configurations Spark"""
        return {
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
            "spark.sql.streaming.schemaInference": "false",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.streaming.kafka.consumer.poll.ms": "512",
            "spark.sql.shuffle.partitions": str(SparkConfig.SHUFFLE_PARTITIONS),
            "spark.default.parallelism": "200",
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "false"
        }