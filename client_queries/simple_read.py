#!/usr/bin/env python3
from common import session

spark = session()
print(spark.read.parquet("").collect())
