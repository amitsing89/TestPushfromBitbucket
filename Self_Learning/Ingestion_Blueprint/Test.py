# from cassandra.query import BatchStatement, SimpleStatement
import json

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import ConfigParser
# from CassandraConnector import CassandraConnector
from pyspark.streaming.kafka import KafkaUtils

cfg = ConfigParser.RawConfigParser()
cfg.read('/root/check/IdeaProjects/com.bt.big-data/ra/bigdata/impl/TILReplica/parameters.cfg')
tab_cfg = ConfigParser.RawConfigParser()
tab_cfg.read('/root/check/IdeaProjects/com.bt.big-data/ra/bigdata/impl/TILReplica/table_elements.cfg')
conf = SparkConf().setAppName("TilIngestion").setMaster("spark://192.168.75.133:7077")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, int(cfg.get('SPARK', 'delay')))
zkQuorum, topic = cfg.get('KAFKA', 'zookeeper_url'), cfg.get('KAFKA', 'topic')
consumer_group_id = cfg.get('KAFKA', 'groupid')
partition_num = cfg.get('KAFKA', 'topic_partition')
optional_params = tab_cfg.get('TABLE_PARAMS', 'publisher')
print optional_params, type(optional_params)
x = list(optional_params.split(","))
print x, len(x)
insertQuery = "insert into adtracker {0}".format(tuple(x))
print insertQuery
