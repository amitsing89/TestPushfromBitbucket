import redis
import sys

red = redis.Redis(host='localhost', db=0)

def redisCFRDeletion():
        keys = red.keys("CFR_:*")
        for i in keys:
                print i
                print red.delete(i)


redisCFRDeletion()
