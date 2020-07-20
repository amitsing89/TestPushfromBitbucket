import redis
import json
import datetime
red = redis.Redis(host ='localhost', db =0)
def clickCountRedis(key):
	totalclick=0
	clickcount=0
	clickcount1=0
	val = red.hvals(key)
	for i in val:
		if ',' in i:
			y = i.split(",")
			for j in y:
				keyRedis = ("LIS:{0}").format(j)
				valueOfRedisLIneitem = red.hgetall(keyRedis)
				for j in valueOfRedisLIneitem:
					val = valueOfRedisLIneitem[j]
					b = int(j)/1000
					t=datetime.datetime.fromtimestamp(b).strftime('%Y-%m-%d %H:%M:%S')
					if t=='2016-04-05 00:00:00':
						v = json.loads(valueOfRedisLIneitem[j])
						clickcount = clickcount + v['click']
						#print v['click'],v['lineItemId']
		elif ',' not in i :
			keyRed = ("LIS:{0}").format(i)
			valueOfRedisLIneitem = red.hgetall(keyRed)
			for j in valueOfRedisLIneitem:
				val = valueOfRedisLIneitem[j]
				b = int(j)/1000
				t=datetime.datetime.fromtimestamp(b).strftime('%Y-%m-%d %H:%M:%S')
				if t=='2016-04-05 00:00:00':
					v = json.loads(valueOfRedisLIneitem[j])
					clickcount1 = clickcount1 + v['click']
	totalclick = clickcount+clickcount1
	print totalclick

s=input("Enter key in CL:yyyymmdd Format \n")
clickCountRedis(s)
