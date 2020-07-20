from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import journeybegins.RediswithPython as RediswithPython
from operator import add

spark_conf = SparkConf().setAppName("Problem Solving")
spark_context = SparkContext(conf=spark_conf)
spark_session = SparkSession.builder.appName('abc').getOrCreate()

rdd_val = spark_context.textFile("/Users/amitsingh/Downloads/rmt.txt")
mapped_value = rdd_val.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(add)
# v2 = mapped_value.filter(lambda x: 'bridge' in x[0])
mapped_value.foreach(RediswithPython.insert)
# print(v2.collect())
# l = [('Alice', 1)]
# df = spark_session.createDataFrame(l).collect()
# print(df)

# firstDF = spark_session.range(3).toDF("myCol")
# newRow = spark_session.createDataFrame([[20]])
# appended = firstDF.union(newRow)
# print(appended.collect())
