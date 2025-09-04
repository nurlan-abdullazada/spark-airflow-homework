# hello_spark.py
from pyspark.sql import SparkSession
import time as t

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()
spark.createDataFrame([("Alice", 25), ("Bob", 33)], ["name", "age"]).show()

t.sleep(60)
spark.stop()


