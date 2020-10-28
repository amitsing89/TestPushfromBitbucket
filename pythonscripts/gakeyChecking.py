import redis
import sys
red  = redis.Redis(host = '192.168.38.157', db = 0)
ikey = red.keys("I:*")
valgakey = red.keys("GA:*")
setofigakey = []
for key in ikey:
	hkey = red.hkeys(key)
	for k in hkey:
		split_value = k.split(':')
		inp = split_value[1]
		split_value2 = inp.split('-')
		if int(split_value2[0])==int(sys.argv[1]):
			if len(split_value2)==4:
				y = int(split_value2[3])
				setofigakey.append(y)

listofgakey = []
for gakey in valgakey:
	split_value = gakey.split(':')
	inp = split_value[1]
	split_value2=inp.split('-')
	if int(split_value2[0])==int(sys.argv[1]):
		if len(split_value2)==4:
			y=int(split_value2[3])
			listofgakey.append(y)

cmp1 = set(setofigakey)
cmp2 = set(listofgakey)
a = list(cmp2-cmp1)
b = list(cmp1-cmp2)

print "difference in GA key and I: key",b
print "difference in I: key and GA key",a
