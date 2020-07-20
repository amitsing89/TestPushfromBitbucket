from pyspark import SparkContext, SparkConf, StorageLevel
import sys

from DPEService.TaasRequestUtil import TaasRequestUtil

# from TaasRequestUtil import TaasRequestUtil
taasRequestUtil = TaasRequestUtil()

# before we process and send request to taas, call login
taasRequestUtil.login()

conf = SparkConf().setAppName("dpe-batch-process").setMaster("local[2]")
sc = SparkContext(conf=conf)
file_path = 'file:///home/cloudera/PycharmProjects/SparkExercise/test.csv'

lines = sc.textFile(file_path)

headers = sc.parallelize(['phone_number', 'code', 'credit_card', 'ip_address', 'email_id'])
columns_rule_map = {
    "phone_number": 2,
    "credit_card": 1,
    "prefix": 2
}
# fetch from Redis
columns_rule_map_bc = sc.broadcast(columns_rule_map)


def parseHeaders(field, index):
    column_rule_map = columns_rule_map_bc.value
    pair = {}
    if field in column_rule_map:
        pair = index, column_rule_map[field]
    return pair


rule = headers.zipWithIndex().map(lambda (key, index): parseHeaders(key, index)).filter(lambda val: val != {})
print rule.collect()


def parseLines(line):
    pair = []
    words = line.split(',')
    for index, word in enumerate(words):
        new_pair = (index, word)
        pair.append(new_pair)
    return pair


words = lines.flatMap(lambda l: parseLines(l)).filter(lambda val: val != {})
print words.collect()


def process(rdd):
    index, pair = rdd
    v, w = pair
    print v, w

    if w is None:
        # not changed
        return index, v

    else:  # changed
        request = dict()
        request['input'] = v
        request['rule'] = w
        token_mapping = taasRequestUtil.tokenize(request)
        return token_mapping


mapping = words.leftOuterJoin(rule).map(lambda rdd: process(rdd))

print mapping.collect()
mapping.saveAsTextFile("file:///home/cloudera/PycharmProjects/SparkExercise/out")
