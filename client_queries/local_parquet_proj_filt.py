#!/usr/bin/env python3
from common import session
import sys
from pyspark.sql import functions as F

# SELECT order_id, amount, status FROM '/home/dan/dev/empty/examples/parquet-test-data/data/small.parquet' WHERE is_priority = true

src = sys.argv[1] if len(
    sys.argv) > 1 else "/home/dan/dev/empty/examples/parquet-test-data/data/small.parquet"

spark = session()
df = spark.read.parquet(src)
print(df.filter(F.col("is_priority") & (F.col("amount") >= 50.0)).select(
    "order_id", "amount", "status").show())
