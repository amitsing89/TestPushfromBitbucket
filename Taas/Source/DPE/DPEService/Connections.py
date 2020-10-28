from kafka import KafkaConsumer, KafkaProducer
import redis
# from config import Config
import requests
import json
import ConfigParser
from TaasRequestUtil import TaasRequestUtil

cfg = ConfigParser.RawConfigParser()
cfg.read('parameters.cfg')

bootstrap_servers = str(cfg.get('KAFKA', 'host')) + ":" + str(cfg.get('KAFKA', 'port'))
red = redis.Redis(host=cfg.get('REDIS', 'host'), db=0)
consumer = KafkaConsumer(cfg.get('KAFKA', 'topic'), bootstrap_servers=[bootstrap_servers],
                         group_id=cfg.get('KAFKA', 'groupid'))
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def redisConnection():
    return red


def kafkaConsumerConnection():
    return consumer


def kafkaProducerConnection():
    return producer


def login():
    # hit login method in taasRequestUtil
    TaasRequestUtil.login()

    # token = None
    # if token_val is None:
    #     print type(data)
    #     login_url = cfg.get('TAAS_URL','service_url')+cfg.get('TAAS_URL','login_url')
    #     response = requests.post(url=login_url, files=files, data=data)
    #     json_response = json.loads(response.text)
    #     if 'token' in json_response:
    #         token = json_response['token']
    #     return token
