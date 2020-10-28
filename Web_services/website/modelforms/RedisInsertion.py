import redis

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
con = redis.Redis(connection_pool=pool)
hash_key = "MetaData"


def insertMetadata(k, v):
    con.hmset(hash_key, {k: v})
