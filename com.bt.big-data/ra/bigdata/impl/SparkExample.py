import datetime
from pyspark import SparkContext

import re
from operator import add

sc = SparkContext('spark://quickstart.cloudera:7077', appName="test")
file_in = sc.textFile("file:////root/check/IdeaProjects/pythonscripts/com.bt.big-data/GCP/Plotting.py")
print('number of lines in file: %s' % file_in.count())
chars = file_in.map(lambda s: len(s)).reduce(add)
print('number of characters in file: %s' % chars)

words = file_in.flatMap(lambda line: re.split('\W+', line.lower().strip()))
words = words.filter(lambda x: len(x) > 3)
words = words.map(lambda w: (w, 1))
words = words.reduceByKey(add)
print words.collect()
words.saveAsTextFile("file:////root/check/IdeaProjects/pythonscripts/com.bt.big-data/ra/bigdata/impl/check{0}".format(
    datetime.datetime.now().strftime('%Y-%H-%M-%S')))
