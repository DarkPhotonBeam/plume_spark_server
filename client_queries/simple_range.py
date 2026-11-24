#!/usr/bin/env python3
from common import session

spark = session()
print(spark.range(10).collect())
