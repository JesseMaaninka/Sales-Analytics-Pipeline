import redis
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, avg, sum, count

import sys
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("=== Spark ETL: Reading sales data from Redis ===\n")

raw_messages = r.lrange("sales_stream", 0, -1)
records = [json.loads(m) for m in raw_messages]
print(f"  Records read from Redis: {len(records)}\n")

spark = SparkSession.builder \
    .appName("SalesETL") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Let Spark infer the schema automatically
df = spark.createDataFrame(records)

print("=== Raw data sample ===\n")
df.show(5, truncate=False)

print("=== Summary per salesman ===\n")
summary = df.groupBy("SalesMan").agg(
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_units"),
    round(sum("revenue"), 2).alias("total_revenue"),
    round(avg("revenue"), 2).alias("avg_order_value"),
)
summary.show(truncate=False)

print("=== Summary per product ===\n")
product_summary = df.groupBy("Product").agg(
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_units_sold"),
    round(sum("revenue"), 2).alias("total_revenue"),
).orderBy(col("total_revenue").desc())
product_summary.show(truncate=False)

r.set("sales_processed", json.dumps(records))
summary_list = [row.asDict() for row in summary.collect()]
r.set("sales_summary", json.dumps(summary_list))

print("  Transformed data saved to Redis.\n")
print("ETL done. Run ml_model.py next.")

spark.stop()