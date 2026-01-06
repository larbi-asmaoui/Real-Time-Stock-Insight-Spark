from pyspark.sql import DataFrame
from pyspark.sql.functions import col, log, lag, lit
from pyspark.sql.window import Window

def transform_data(df: DataFrame) -> DataFrame:
    """
    Applies feature engineering to the Spark DataFrame.
    
    Features:
    - Log Returns: log(avg_price / prev_avg_price)
    - Scaled Volatility: volatility (Using raw volatility from Gold)
    - Log Volume: log(total_volume + 1)
    """
    
    # Define a window specification for lag calculation
    w = Window.partitionBy("symbol").orderBy("window_start")
    
    # We calculate features. 
    # Note: lag(1) will result in null for the first row of each symbol.
    # The caller is responsible for dropping nulls if strict completeness is required (e.g. for training).
    
    return (
        df
        .withColumn("prev_price", lag("avg_price", 1).over(w))
        .withColumn("log_returns", log(col("avg_price") / col("prev_price")))
        .withColumn("log_volume", log(col("total_volume") + 1.0))
        .select(
            col("symbol"),
            col("window_start"),
            col("avg_price"),
            col("log_returns"),
            col("volatility"),
            col("log_volume")
        )
    )
