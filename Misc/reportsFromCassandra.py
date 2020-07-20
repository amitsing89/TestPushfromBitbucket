import csv
import servercredentials
import sys
import datetime
import time
from datetime import datetime
import pytz


def dateToString(dateTime, format='%Y-%m-%d'):
        return datetime.strftime(dateTime, format)

def fieldsofTable(tablename,dated):
        strs = "select * from {0} limit 1".format(tablename)
        rows = servercredentials.cassandraProd(strs)
        x = rows[0]
        u = x._fields
        fieldsoftable = []
        for i in u:
                fieldsoftable.append(i)
        #lengthoffileds = len(fieldsoftable)
	#print fieldsoftable
        clientReport(tablename,dated,fieldsoftable)
	#with open("/opt/amit_scripts_python/"+tablename+".csv", 'a') as csvfile:
	#	sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
	#	sitewriter.writerow([s.encode("utf-8") for s in fieldsoftable])		


def clientReport(tablename,dated,fields):
        advclientids = servercredentials.advertiserList()
        datetimeobject = datetime.strptime(str(dated),'%Y%m%d')
        converteddate = dateToString(datetimeobject)
        #print converteddate
        pubclientids = servercredentials.publishersList()
	#print fields
        with open("/opt/amit_scripts_python/"+tablename+".csv", 'a') as csvfile:
                sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
                sitewriter.writerow([s.encode("utf-8") for s in fields])
                if "adv" in tablename:
                        for adv in advclientids:
                                strs = "select * from {0} where clientid = {1} and date ='{2}'".format(tablename,adv,converteddate)
                                rows = servercredentials.cassandraDictRowfinder(strs)
                                for a in rows:
					#print a
					convertedDate = (a['date'].replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
					sitewriter.writerow([str(convertedDate) if "date" in s else a[s] for s in fields])
                if "pub" in tablename:
                        for pub in pubclientids:
                                strs = "select * from {0} where clientid = {1} and date ='{2}'".format(tablename,pub,converteddate)
                                rows = servercredentials.cassandraDictRowfinder(strs)
                                for a in rows:
                                        #print a
                                        convertedDate = (a['date'].replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
                                        sitewriter.writerow([str(convertedDate) if "date" in s else a[s] for s in fields])


a = sys.argv[1]
b = sys.argv[2]
if len(sys.argv)>3:
	c = sys.argv[3]
	print(a,b,c)
else:
	fieldsofTable(a,b)
