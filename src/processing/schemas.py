"""
Schémas de données pour Spark Streaming
BEST PRACTICE: Définir explicitement tous les schémas
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, LongType, TimestampType
)

class StockSchemas:
    """Définition centralisée des schémas"""
    
    @staticmethod
    def get_input_schema():
        """Schéma des messages Kafka entrants"""
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("volume", LongType(), False),
            StructField("timestamp", StringType(), False),
            StructField("source", StringType(), True)
        ])
    
    @staticmethod
    def get_aggregated_schema():
        """Schéma des données agrégées"""
        return StructType([
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("symbol", StringType(), False),
            StructField("avg_price", DoubleType(), True),
            StructField("min_price", DoubleType(), True),
            StructField("max_price", DoubleType(), True),
            StructField("volatility", DoubleType(), True),
            StructField("total_volume", LongType(), True),
            StructField("trade_count", LongType(), True),
            StructField("price_change_pct", DoubleType(), True)
        ])