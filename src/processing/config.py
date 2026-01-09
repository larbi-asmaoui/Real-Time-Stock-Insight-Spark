class SparkConfig:
    
    # Kafka Configuration
    # KAFKA_BROKERS = "kafka:29092"
    KAFKA_BROKERS = "redpanda:9094"
    KAFKA_TOPIC = "stock_prices"
    KAFKA_STARTING_OFFSETS = "earliest" 
    
    # Spark Configuration
    APP_NAME = "StockInsightStreaming"
    LOG_LEVEL = "WARN"
    
    # Performance Tuning
    MAX_OFFSETS_PER_TRIGGER = 10000
    PROCESSING_TIME = "0 seconds" 
    
    # Storage paths for medallion architecture
    BRONZE_INIT_DELAY = 45
    SILVER_INIT_DELAY = 45
    # BASE_PATH = "/app/data"
    BASE_PATH = "s3a://finance-lake"
    BRONZE_PATH = f"{BASE_PATH}/lake/bronze"
    SILVER_PATH = f"{BASE_PATH}/lake/silver"
    GOLD_PATH = f"{BASE_PATH}/lake/gold"
    CHECKPOINT_ROOT = f"{BASE_PATH}/checkpoints"
    
    # Windowing Configuration
    WINDOW_DURATION = "10 seconds"
    SLIDE_DURATION = "5 seconds" 
    WATERMARK_DELAY = "5 seconds"
    
    # Spark SQL Optimization
    SHUFFLE_PARTITIONS = 4
    ADAPTIVE_ENABLED = True
    
    # Delta Lake Configuration
    DELTA_MERGE_SCHEMA = True
    DELTA_AUTO_OPTIMIZE = True
    
    @staticmethod
    def get_spark_configs():
        """Returns a dict of Spark configurations"""
        configs = {
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0",
            
            # --- DELTA ---
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.schema.autoMerge.enabled": str(SparkConfig.DELTA_MERGE_SCHEMA).lower(),
            
            # --- STREAMING ---
            "spark.sql.streaming.minBatchesToRetain": "10",
            "spark.sql.streaming.schemaInference": "false",
            "spark.sql.adaptive.enabled": "true",
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.streaming.kafka.consumer.poll.ms": "512",
            
            # --- LOCAL MODE TUNING (Critical) ---
            "spark.master": "local[*]", 
            "spark.driver.memory": "4g",
            "spark.sql.shuffle.partitions": "2", # Reduced from 4
            "spark.default.parallelism": "2",
            "spark.memory.fraction": "0.6",
            "spark.memory.storageFraction": "0.1",
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "false",

            # --- S3 / MINIO CONFIG ---
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.committer.name": "directory", 
            "spark.sql.sources.commitProtocolClass": "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",

            # --- OOM FIX (Missing in your file) ---
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.fast.upload.buffer": "disk", 
            "spark.hadoop.fs.s3a.buffer.dir": "/tmp/spark-s3-buffer",
            "spark.hadoop.fs.s3a.multipart.size": "5M",
            "spark.hadoop.fs.s3a.block.size": "10M"
        }

        configs["spark.sql.streaming.checkpointLocation.root"] = SparkConfig.CHECKPOINT_ROOT
        return configs