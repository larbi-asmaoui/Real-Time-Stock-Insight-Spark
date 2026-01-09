from pyspark.sql import DataFrame
from pyspark.sql.functions import col, log, lag, lit, when, coalesce
from pyspark.sql.window import Window

def transform_data(df: DataFrame) -> DataFrame:
    """
    Features:
    - Log Returns: log(avg_price / prev_avg_price)
    - Volatility: Passed from Gold (filled with 0.0 if null)
    - Log Volume: log(total_volume + 1)
    """
    w = Window.partitionBy("symbol").orderBy("window_start")
    
    df_feat = df \
        .withColumn("prev_price", lag("avg_price", 1).over(w)) \
        .withColumn("log_returns", log(col("avg_price") / col("prev_price"))) \
        .withColumn("log_volume", log(col("total_volume") + 1.0)) \
        .withColumn("volatility", coalesce(col("volatility"), lit(0.0)))
        
    return df_feat.na.drop(subset=["log_returns", "log_volume", "avg_price"])