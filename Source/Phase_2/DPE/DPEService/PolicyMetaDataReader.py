import redis
import json
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('parameters.cfg')

class redisMetaData():
    def __init__(self):
        self.red = redis.Redis(host=config.get('REDIS', 'host'))

    def readFromRedis(self, key, field):
        typeofkey = self.red.type(key)
        if "none" in typeofkey:
            return "Policy not Found.Please create one." + key
        elif 'hash' in typeofkey:
            val = self.red.hget(key, field)
            parsed_val = dict(json.loads(val))
            return parsed_val

    def updateMetadataInRedis(self, key, field, value):
        typeofkey = self.red.type(key)
        if "none" in typeofkey:
            return "Policy not found." + key
        elif 'hash' in typeofkey:
            self.red.hset(key, field, value)

    def readMetadatafromRedis(self, key, field):
        typeofkey = self.red.type(key)
        metadata = None
        if "none" in typeofkey:
            return "Policy not found." + key
        elif 'hash' in typeofkey:
            metadata = self.red.hget(key, field)
            print metadata, type(metadata)
            json_metadata = dict(json.loads(metadata))
        return json_metadata