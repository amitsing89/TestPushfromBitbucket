from kafka import KafkaConsumer, KafkaProducer
import redis
from config import Config
import requests
import json

cfg = Config('connectioninstance.cfg')

bootstrap_servers = str(cfg.kafka) + ":" + str(cfg.kafkaport)
red = redis.Redis(host=cfg.redis, db=0)
consumer = KafkaConsumer(bootstrap_servers=[bootstrap_servers], group_id=cfg.groupid, client_id=cfg.clientid)
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
data = dict(cfg.data)
files = {'password': open('taas.jks', 'rb')}


def redisConnection():
    return red


def kafkaConsumerConnection():
    return consumer


def kafkaProducerConnection():
    return producer


token_val = None


def login():
    token = None
    if token_val is None:
        print type(data)
        response = requests.post(url=cfg.loginurl, files=files, data=data)
        json_response = json.loads(response.text)
        if 'token' in json_response:
            token = json_response['token']
        return token
