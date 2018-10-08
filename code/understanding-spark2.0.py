
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession

# Starting spark session and creating RRD and dataFrame
s_config = SparkConf().setAppName("UnderstandingSpark2.0")

spark_session = SparkSession.builder.master("local").appName("UnderstandingSpark2.0")\
                .config(conf=s_config).getOrCreate()

n_slices = 10

df = spark_session.createDataFrame([[1, "Alice", 50], [2, "BOB", 60]], ['id','name','age'])

print(df.count())

print(df.first())

print(df.collect())

print(df.show())

spark_session.stop()

