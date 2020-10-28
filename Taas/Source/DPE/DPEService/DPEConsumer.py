from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json
import ConfigParser
from TaasRequestUtil import TaasRequestUtil

taasRequestUtil = TaasRequestUtil()

cfg = ConfigParser.RawConfigParser()
cfg.read(
    'C:\\Users\\611432986\\IdeaProjects\\bt.taas\\svn\\dss-bigdata\\trunk\\TAAS\\Source\\Phase_2\\DPE\\DPEService'
    '\\parameters.cfg')


# write custom method to find lines, metadata and optional params
def processEachMessage(record_list):
    # record_list = record.collect()
    # record list is list of key, value from each rdd - record
    for record_dict in record_list:
        extra_dict = dict()
        # record is one dict {row:[[]], metadata:{}}
        for k, v in record_dict.iteritems():
            if k == 'row':
                row_data = record_dict[k]
            elif k == 'rule':
                rule_metadata = record_dict[k]
            else:
                extra_dict[k] = record_dict[k]

        process_each_row(row_data, rule_metadata, extra_dict)


def process_each_row(row, meta, opt):
    for index, rule in meta.iteritems():
        index_search = int(index)
        rule_id = int(rule)
        request_params = cfg.get('REQUEST', 'params').split(',')
        for lines in row:
            print 'lines', lines
            record = lines[index_search]
            request = {request_params[0]: record, request_params[1]: rule_id}
            # request['input'] = record
            # request['rule'] = rule_id
            if rule_id == 1 and opt is not None:
                request = dict(opt.items() + request.items())
            print 'requesting for %s with %s' % (record, request)
            request_taas_service(request)


def request_taas_service(req):
    try:
        token_pair = taasRequestUtil.tokenize(req)
        print 'output', token_pair
    except Exception as e:
        print "Issue in Tokenizing", e


sc = SparkContext(appName=cfg.get('SPARK', 'app_name'))
ssc = StreamingContext(sc, int(cfg.get('SPARK', 'delay')))

# before we process and send request to taas, call login
taasRequestUtil.login()

print 'connecting to the kafka topic'
zkQuorum, topic = cfg.get('KAFKA', 'zookeeper_url'), cfg.get('KAFKA', 'topic')
consumer_group_id = cfg.get('KAFKA', 'groupid')
partition_num = cfg.get('KAFKA', 'topic_partition')

lines = KafkaUtils.createStream(ssc, zkQuorum, consumer_group_id, {topic: int(partition_num)})
# lines is a dstream of key, value where key is None, and value is kafka message you sent
data_dict = lines.map(lambda (k, v): json.loads(v))
# data_dict.foreachRDD(processEachMessage)
# doing foreachPartition for more optimization and load balancing.
data_processing = data_dict.foreachRDD(lambda rdd: rdd.foreachPartition(processEachMessage))
ssc.start()
ssc.awaitTermination()
