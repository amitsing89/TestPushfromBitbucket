import sys
import redis
import json
import datetime


def cpdDetails(key):
    red = redis.Redis(host='192.168.34.235', db=0)
    val = red.hvals(key)
    for i in val:
        if len(i) > 6:
            y = i.split(",")
            for j in y:
                strs = "LID:{0}".format(j)
                if red.exists(strs) == 1:
                    a = json.loads(red.get(strs))
                    if a['spstrategy'] == 1:
                        print "Lineitem which are running on CPD is-", j
                        spent = a['spend']
                        print "Spend in for the Day=", spent


print "CPD LIneItems Of", sys.argv[1]
cpdDetails(sys.argv[1])
