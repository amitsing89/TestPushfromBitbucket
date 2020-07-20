import json
import credentials
import requests
from config import Config

cfg = Config('connectioninstance.cfg')


class KafkaTaasConsumer:
    def __init__(self):
        self.url_pattern = "{0}{1}".format(cfg.apiurl, cfg.token_service)
        self.consumer = credentials.kafkaConsumerConnection()
        self.red = credentials.redisConnection()
        self.consumer.subscribe([cfg.kafkatopic])
        self.token = credentials.login()
        if self.token:
            self.headers = {cfg.headers: self.token}
        else:
            print 'login failed, please try again'

    # login service to get the authorized token
    def consumerMessages(self):
        for message in self.consumer:
            json_response = json.loads(message.value)
            for val in json_response:
                data = {"input": str(json_response[val])}
                # print data
                response = requests.post(self.url_pattern, data, headers=self.headers)
                json_responses = response.json()
                print json_responses
                self.red.hmset('object', json_responses['obj'])
