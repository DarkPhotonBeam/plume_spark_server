import os
from pyspark.sql import SparkSession


def session() -> SparkSession:
    url = os.environ.get("PLUME_REMOTE", "sc://localhost:15002")
    return SparkSession.builder.remote(url).getOrCreate()
