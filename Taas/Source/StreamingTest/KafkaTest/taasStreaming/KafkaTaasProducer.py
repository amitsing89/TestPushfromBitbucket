import json
import credentials
from config import Config
from utility import Utility

cfg = Config('connectioninstance.cfg')
util = Utility()


class KafkaTaasProducer:
    def __init__(self):
        # Redis Connection
        self.redis = credentials.redisConnection()
        # Connection to kafka server as a producer
        self.producer = credentials.kafkaProducerConnection()
        # Topic of kafka
        self.topic = cfg.kafkatopic

    # Producer Method for producing data
    def kafkaProducer(self):
        while True:
            test_data = util.testDataCreation()
            # test_data = util.readFromFile()
            objString = json.dumps(test_data, default=lambda o: o.__dict__)
            # self.redis.hmset(test_data["Mob.No"], test_data)
            self.producer.send('dpe-topic', objString)
            print objString
