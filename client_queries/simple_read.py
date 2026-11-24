#!/usr/bin/env python3
from common import session
import sys

src = sys.argv[1] if len(
    sys.argv) > 1 else "/home/dan/dev/empty/examples/parquet-test-data/data/all_types.parquet"

spark = session()
print(spark.read.parquet(src).collect())
