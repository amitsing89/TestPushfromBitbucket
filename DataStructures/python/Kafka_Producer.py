import json
from kafka import KafkaConsumer, KafkaProducer
import redis
import DataCook
import base64

try:
    red = redis.Redis(host='localhost')
    bootstrap_servers = str('localhost') + ":" + str('9092')
    # consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], group_id='my-group', client_id='consumer1')
    producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(0, 10))
except Exception as e:
    print(e)


def kafkaProducer(test_data):
    # while True:
    # test_data = util.readFromFile()
    # objString = json.dumps(test_data['sentiment'], default=lambda o: o.__dict__)
    objString = json.dumps(test_data, default=lambda o: o.__dict__)
    # self.redis.hmset(test_data["Mob.No"], test_data)
    print(objString)
    result = producer.send('twitter-topic-sentiments', objString.encode('utf-8'))  # , base64.b64encode(objString))
    record_metadata = result.get(timeout=10)
    # print (record_metadata.topic)
    # print (record_metadata.partition)
    # print (record_metadata.offset)
    red.set("producerOffset", str(record_metadata.topic) + str(":") + str(record_metadata.partition) + str(":") + str(
        record_metadata.offset))
    # print(test_data)

# for i in range(100):
#     x = DataCook.testDataCreation()
#     kafkaProducer(x)
