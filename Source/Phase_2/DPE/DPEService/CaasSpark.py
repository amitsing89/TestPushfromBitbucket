import sys
from pyspark import SparkContext, SparkConf
# import Connections
import redis


def spark_Core_Rest_call():
    redis_conect = redis.Redis(host='localhost')  # Connections.redisConnection()
    val = redis_conect.get("caas")
    conf = SparkConf().setAppName("ChabalService").setMaster("spark://localhost:7077")
    sc = SparkContext(conf=conf)
    distFile = sc.textFile(sys.argv[1])
    counts = distFile.flatMap(lambda line: line.split(",")).map(lambda word: (word, 1)).filter(
        lambda line: val not in line)
    counts.saveAsTextFile(sys.argv[2])


spark_Core_Rest_call()
