from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import redis

import pymysql


class ConnectionInstances:
    # @staticmethod
    # def mySqlConnection(host='localhost', user='mysql', passwd='password123'):
    #     db = pymysql.connect(host="localhost", user="root")
    #     cur = db.cursor()
    #     print(cur.execute("show databases"))
    #     return cur
    #
    # @staticmethod
    # def cassandraConnector(hosts=['localhost']):
    #     cluster = Cluster(hosts, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='US_EAST'), port=9042)
    #     session = cluster.connect()
    #     return session

    @staticmethod
    def redisConnection(host='localhost', port=6379, db=0):
        pool = redis.ConnectionPool(host=host, port=port, db=db)
        con = redis.Redis(connection_pool=pool)
        return con


# x = ConnectionInstances()
# x.mySqlConnection()
