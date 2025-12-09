import sys
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AnalyzeResults") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df_pred = spark.read.format("delta").load("/app/data/lake/predictions")
df_pred.createOrReplaceTempView("stock_predictions")

df_summary = spark.sql("""
    SELECT 
        symbol, 
        COUNT(*) as total_signals,
        SUM(prediction) as buy_signals,
        ROUND((SUM(prediction) / COUNT(*)) * 100, 2) as buy_percentage
    FROM stock_predictions
    GROUP BY symbol
    ORDER BY buy_percentage DESC
""")

df_recent = spark.sql("""
    SELECT symbol, window_end, avg_price, prediction, probability
    FROM stock_predictions
    ORDER BY window_end DESC
    LIMIT 10
""")

output_path = "/app/logs/analysis_report.txt"
original_stdout = sys.stdout 

print(f"📝 Generating report to {output_path} ...")

with open(output_path, "w") as f:
    sys.stdout = f 

    print("="*60)
    print("📊 STOCK PREDICTION ANALYTICS REPORT")
    print("="*60)
    print("\n")

    print("1. TOP STOCKS TO BUY (Ranking):")

    df_summary.show(truncate=False)
    
    print("\n" + "="*60 + "\n")

    print("2. LATEST PREDICTIONS (Raw Output):")

    df_recent.show(truncate=False)
    
    print("="*60)
    print("End of Report")

sys.stdout = original_stdout
print("✅ Done! Report saved with exact Spark table format.")   