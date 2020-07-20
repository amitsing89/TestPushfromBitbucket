import redis
from pyspark import SparkContext, SparkConf, Row
import re
import sys
from pyspark import SQLContext

# inputlist = [sys.argv[1], sys.argv[2], sys.argv[3]]
# percentage = int(sys.argv[4])
# print (percentage * len(inputlist) / 100)
# red = redis.Redis(host='localhost', db=0)
conf = SparkConf().setAppName("Pyspark Pgm")
sc = SparkContext('spark://quickstart.cloudera:7077', conf=conf)
sqlContext = SQLContext(sc)
contentRDD = sc.textFile("file:///root/check/IdeaProjects/pythonscripts/com.bt.big-data/ra/bigdata/impl/as.txt")
mappedVal = contentRDD.map(lambda x: int(float(x)))
total = mappedVal.count()
reducedVal = mappedVal.reduce(lambda x, y: x + y)
print float(reducedVal) / total

mappedValue = contentRDD.map(lambda x: Row(int(float(x))))
dataFrame = sqlContext.createDataFrame(mappedValue)
print dataFrame.agg({"_1": "avg"}).collect()

sample = dataFrame.sample(False, 0.1, seed=0).limit(100)
print sample.collect()
# nonempty_lines = contentRDD.filter(lambda x: len(x) > 0)
# words = nonempty_lines.flatMap(lambda x: x.split(' '))
# wordcount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey(False)
#
# for word in wordcount.collect():
#     result = re.sub('[^a-zA-Z0-9 \n\.]', '', word[1])
#     red.sadd("file", result)
