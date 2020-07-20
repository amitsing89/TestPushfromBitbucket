import json

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import ConfigParser
from CassandraConnector import CassandraConnector
from cassandra.query import BatchStatement, SimpleStatement
import pyspark.sql.window
from pyspark.streaming.kafka import KafkaUtils

cfg = ConfigParser.RawConfigParser()
cfg.read('/root/check/IdeaProjects/pythonscripts/com.bt.big-data/ra/bigdata/impl/TILReplica/parameters.cfg')
tab_cfg = ConfigParser.RawConfigParser()
tab_cfg.read('/root/check/IdeaProjects/pythonscripts/com.bt.big-data/ra/bigdata/impl/TILReplica/table_elements.cfg')
conf = SparkConf().setAppName("TilIngestion")  # .setMaster("spark://192.168.75.133:7077")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, int(cfg.get('SPARK', 'delay')))
zkQuorum, topic = cfg.get('KAFKA', 'zookeeper_url'), cfg.get('KAFKA', 'topic')
consumer_group_id = cfg.get('KAFKA', 'groupid')
partition_num = cfg.get('KAFKA', 'topic_partition')
optional_params = tab_cfg.get('TABLE_PARAMS', 'publisher')


def handler(vals):
    for val in vals:
        print "TYPE", type(val)
        # x = val.collect()
        # dict_json = json.loads(val)
        keys = val.keys()
        if 'adNotifyLog' in keys and val['adNotifyLog'] is not None:
            print "NOTIFY", val['adNotifyLog']
        if 'adImprLog' in keys and val['adImprLog'] is not None:
            print "adImprLog", val['adImprLog']
        if 'adClickLog' in keys and val['adClickLog'] is not None:
            print "adClickLog", val['adClickLog']
            # for i in val:
            #     print "Collected Value",i


def handler_new(iters):
    session = CassandraConnector().cassandraConnection()
    batch = BatchStatement()
    val = 0
    for record in iters:
        print ("REC", record)
        insertQuery = "insert into adtracker ({0}) values('{1}','{2}','{3}','{4}','{5}','{6}'){}".format(
            optional_params, str(record['date']),
            str(record[
                    'phone_number']),
            str(record[
                    'imein']),
            str(record[
                    'credit_card']),
            str(record[
                    'ip']),
            record[
                'ssn_number'])
        batch.add(SimpleStatement(insertQuery))
        val = val + 1
        if val > 20:
            session.execute(batch)
            batch = BatchStatement()
            val = 0
    if val != 0:
        session.execute(batch)


lines = KafkaUtils.createStream(ssc, zkQuorum, consumer_group_id, {topic: int(partition_num)})
data_dict = lines.map(lambda (k, v): json.loads(v))
# window = lines.countByWindow(15,60)
# data_processing = data_dict.foreachRDD(lambda rdd: rdd.foreachPartition(handler_new))
data_processing = data_dict.foreachRDD(lambda rdd: rdd.foreachPartition(handler))
ssc.start()
ssc.awaitTermination()
