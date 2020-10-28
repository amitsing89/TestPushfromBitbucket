import redis
import ClientRelated.DataSourceCall as DataSourceCall

red = DataSourceCall.connectionReturn('redis')

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
con = redis.Redis(connection_pool=pool)


def insert(iter):
    my_dict = {iter[0]: iter[1]}
    con.hmset("Iter", my_dict)


def delStringKeys():
    keys = con.hkeys("Iter")
    for i in keys:
        print(i)
        # if type(i) is str:
        print(con.hdel("Iter", i))
    # all_keys = list(con.hgetall('some_hash_name').keys())
    # con.hdel('some_hash_name', *all_keys)


def retrieve():
    return con.hgetall("Iter")

# delStringKeys()

