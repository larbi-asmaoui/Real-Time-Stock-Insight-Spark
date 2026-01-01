from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from processing.spark_streaming_utils import setup_logging
from typing import Any

logger = setup_logging()

class StreamLayer(ABC):
    """
    Abstract Base Class for Spark Streaming Layers trying to follow Template Method Pattern.
    """
    
    def __init__(self, spark: SparkSession, config: Any):
        self.spark = spark
        self.config = config

    @property
    @abstractmethod
    def layer_name(self) -> str:
        """Name of the layer for logging and query naming."""
        pass

    @property
    @abstractmethod
    def output_path(self) -> str:
        """Destination path for the Delta Table."""
        pass
    
    @property
    @abstractmethod
    def checkpoint_path(self) -> str:
        """Checkpoint path for the stream."""
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        """Define the source of the stream."""
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply business logic and transformations."""
        pass

    def write(self, df: DataFrame) -> StreamingQuery:
        """
        Generic write logic for Delta Lake.
        Follows DRY principle.
        """
        logger.info(f"Configuring usage of Delta sink for {self.layer_name} -> {self.output_path}")
        
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("mergeSchema", "true")
            .partitionBy("symbol")
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(processingTime=self.config.PROCESSING_TIME)
            .queryName(f"{self.layer_name}_ingestion")
            .start(self.output_path)
        )

    def bootstrap(self) -> bool:
        """
        Bootstrap the layer's output table if it doesn't exist.
        This prevents 'AnalysisException: Table schema is not set' for downstream consumers.
        """
        return True

    def run(self) -> StreamingQuery:
        """
        The Template Method.
        Orchestrates the pipeline: Read -> Transform -> Write.
        """
        logger.info(f"Starting {self.layer_name} Layer...")
        
        df_source = self.read()
        df_transformed = self.transform(df_source)
        query = self.write(df_transformed)
        
        logger.info(f"{self.layer_name} Layer started successfully.")
        return query
