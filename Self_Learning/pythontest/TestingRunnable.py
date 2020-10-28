# import sys
#
# import redis
#
# red = redis.Redis(host='192.168.36.70', db=0)
# red.set(sys.argv[1], sys.argv[2])


from datetime import *
from datetime import timedelta

date_N_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-28')
print("C",date_N_days_ago)
started_at = datetime.now()
ds=started_at
print(ds)
curr_date_minus1_pattern = (datetime.strptime(str(ds), '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
dest_dir = (datetime.strptime(str(ds), '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
print("minus1--"+curr_date_minus1_pattern)
print("Dir-- "+dest_dir)
