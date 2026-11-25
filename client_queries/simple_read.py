#!/usr/bin/env python3
from common import session
import sys

src = sys.argv[1] if len(
    sys.argv) > 1 else "http://127.0.0.1:8000/small.parquet"

spark = session()
print(spark.read.parquet(src).toPandas())
