import redis


class RedisConnector():

    def __init__(self):
        self.redis_connection = redis_connection = redis.Redis(host='localhost', db=0)

    def connection_instance(self):
        return self.redis_connection