#!/usr/bin/env python
import redis
import sys
import logging
from redis.sentinel import Sentinel

logging.basicConfig(filename='/opt/amit_scripts_python/log/rediskeycleanup.log', level=logging.DEBUG)

sentinel = Sentinel([('localhost', 26379), ('localhost', 26379), ('localhost', 26379)],
                    socket_timeout=0.1)
master = sentinel.master_for('mymaster', socket_timeout=4)


def redisKeyDeletion(date):
    keys = master.keys('I:*')
    keyCount = 0
    countofonehkey = 0
    # print "Total Keys ", len(keys)
    for i in keys:
        keyCount = keyCount + 1
        if keyCount % 1000 == 0:
            logging.info("Keys read:" + str(keyCount))
        # print "Keys read:"+str(keyCount)
        hkey = master.hkeys(i)
        print len(hkey)
        countofonehkey = 0
        if len(hkey) < 2:
            countofonehkey = countofonehkey + 1
            print i, hkey
        for k in hkey:
            try:
                split_value = k.split(':')
                inp = split_value[1]
                split_value2 = inp.split('-')
                y = int(split_value2[0])
                if y <= int(date):
                    logging.info(str(i) + str(k))
                    # print k
                    master.hdel(i, k)
            except Exception as e:
                logging.error(e)
            # print countofonehkey


print "Deleting keys before ", sys.argv[1]

redisKeyDeletion(sys.argv[1])
