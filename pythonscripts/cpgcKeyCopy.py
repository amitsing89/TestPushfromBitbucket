import servercredentials
import redis
import sys
from redis.sentinel import Sentinel
sentinel = Sentinel([('192.169.34.221', 26379),('192.169.34.222',26379), ('192.169.33.163',26379)], socket_timeout=10)
redqc = sentinel.master_for('mymaster', socket_timeout=10)
red = servercredentials.redisconnection()
keys = "CPGC:{0}".format(sys.argv[1])
keyscl = "CL:{0}".format(sys.argv[1])
val = red.smembers(keys)
for i in val:
	#redqc.hset(keyscl,i,"0")
	redqc.sadd(keys,i)

