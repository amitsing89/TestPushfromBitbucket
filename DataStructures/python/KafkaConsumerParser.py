import base64

from kafka import KafkaConsumer, KafkaProducer
import redis

red = redis.Redis(host='localhost')
consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                         auto_offset_reset='latest')
consumer.subscribe(['twitter-topic'])


def run():
    for message in consumer:
        # if message.offset == 1:
        red.set("consumerOffset", str(message.topic) + str(message.partition) + str(message.offset))
        print message.value, message.offset, base64.b64decode(message.value)


run()
