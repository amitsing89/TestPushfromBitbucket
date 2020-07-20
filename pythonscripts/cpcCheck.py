import servercredentials
import sys
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import pytz
import time

sessionadlog = servercredentials.sessionReturn()
sessionadlog.set_keyspace('adlog')
sessionadlog.execute('use adlog')



def clickBidCheck(year,month,dateVar):
	convDateVar = year+"-"+month+"-"+dateVar 
	for hour in range(0,24):
		for minutes in range(0,60):
			query = "select * from adclicklog{0}{1}{2} WHERE date ='{3} {4}:{5}:00'".format(year,month,dateVar,convDateVar,hour,minutes)
			rows = sessionadlog.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
			print query
			for a in rows:
				if a.advclientid==int(sys.argv[4]):
					#if a.clickbid==0:
					convertedDate = (a.time.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
					print convertedDate
					print a


clickBidCheck(sys.argv[1],sys.argv[2],sys.argv[3])
