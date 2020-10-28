import os
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import configparser
from textblob import TextBlob

cfg = configparser.RawConfigParser()
cfg.read('parameters.cfg')

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 pyspark-shell'


def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t ])|(\w+:\/\/\S+)", " ", tweet).split())


# write custom method to find lines, metadata and optional params
def processEachMessage(record_list):
    for inp in record_list:
        analysis = TextBlob(json.loads(inp[1])['text'])
        if analysis.sentiment.polarity > 0:
            print('positive')
        elif analysis.sentiment.polarity < 0:
            print('negative')
        else:
            print('neutral')

    # for it in record_list:
    #     analysis = TextBlob(clean_tweet(it['text']))
    #     if analysis.sentiment.polarity > 0:
    #         print 'positive'
    #     elif analysis.sentiment.polarity == 0:
    #         print 'neutral'
    #     else:
    #         print 'negative'


sc = SparkContext(appName=cfg.get('SPARK', 'app_name'))
ssc = StreamingContext(sc, int(cfg.get('SPARK', 'delay')))

# before we process and send request to taas, call login

print('connecting to the kafka topic')
zkQuorum, topic = cfg.get('KAFKA', 'zookeeper_url'), cfg.get('KAFKA', 'topic')
consumer_group_id = cfg.get('KAFKA', 'groupid')
partition_num = cfg.get('KAFKA', 'topic_partition')

lines = KafkaUtils.createStream(ssc, zkQuorum, consumer_group_id, {topic: int(partition_num)})
# data_dict = lines.map(lambda x: json.loads(x[1])) #.foreachRDD(lambda rdd: rdd.foreachPartition(processEachMessage))
# lines.pprint()
# lines.repartition(10)
processing = lines.foreachRDD(lambda rdd: rdd.foreachPartition(processEachMessage))
# data_processin
# g = data_dict.foreachRDD(lambda rdd: rdd.foreachPartition(processEachMessage))
# data_processing = lines.foreachRDD(processEachMessage)
# print type(data_processing)
# lines is a dstream of key, value where key is None, and value is kafka message you sent
# data_dict = lines.map(lambda (k, v): json.loads(v))
# print data_dict.count()
# data_dict.foreachRDD(processEachMessage)
# doing foreachPartition for more optimization and load balancing.
# data_processing = data_dict.foreachRDD(lambda rdd: rdd.foreachPartition(processEachMessage))
ssc.start()
ssc.awaitTermination()
