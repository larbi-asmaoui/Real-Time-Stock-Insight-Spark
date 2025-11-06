from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
from datetime import datetime

# Initialisation de la Spark Session
spark = SparkSession.builder \
    .appName("RealTimeStockProcessor") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Schéma explicite pour les données JSON
stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Lecture du flux Kafka
def create_streaming_dataframe():
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "stock-topic") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parser les messages JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), stock_schema).alias("data")
    ).select("data.*")
    
    return parsed_df

# Agrégations glissantes avec fenêtre temporelle
def apply_window_aggregations(df):
    # Fenêtre de 10 secondes avec slide de 5 secondes
    window_spec = Window \
        .partitionBy("symbol") \
        .orderBy(col("timestamp").cast("long")) \
        .rangeBetween(-10, 0)
    
    # Calcul des statistiques
    aggregated_df = df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            col("symbol"),
            window(col("timestamp"), "10 seconds", "5 seconds")
        ) \
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("price_volatility"),
            sum("volume").alias("total_volume"),
            count("price").alias("message_count"),
            min("price").alias("min_price"),
            max("price").alias("max_price")
        ) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end"))
    
    return aggregated_df

# Fonction principale
def main():
    print("Démarrage du traitement streaming...")
    
    # Création du DataFrame streaming
    raw_stream_df = create_streaming_dataframe()
    
    # Application des agrégations
    processed_stream_df = apply_window_aggregations(raw_stream_df)
    
    # Écriture des résultats
    query = processed_stream_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()