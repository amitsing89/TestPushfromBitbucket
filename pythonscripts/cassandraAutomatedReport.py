import csv
import servercredentials
import sys
import datetime
import time
from datetime import datetime


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
        lengthoffileds = len(fieldsoftable)
        clientReport(tablename,dated,fieldsoftable)


def clientReport(tablename,dated,fields):
        advclientids = servercredentials.advertiserList()
        datetimeobject = datetime.strptime(str(dated),'%Y%m%d')
        converteddate = dateToString(datetimeobject)
        #print converteddate
        pubclientids = servercredentials.publishersList()
        with open("/opt/amit_scripts_python/"+tablename+".csv", 'a') as csvfile:
                sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
                sitewriter.writerow([fields])
                if "adv" in tablename:
                        for adv in advclientids:
                                strs = "select * from {0} where clientid = {1} and date ='{2}'".format(tablename,adv,converteddate)
                                rows = servercredentials.cassandraProd(strs)
                                for a in rows:
                                        #print type(a),"\n"
                                        sitewriter.writerow(a)
                if "pub" in tablename:
                        for pub in pubclientids:
                                strs = "select * from {0} where clientid = {1} and date ='{2}'".format(tablename,pub,converteddate)
                                rows = servercredentials.cassandraProd(strs)
                                for a in rows:
                                        #print type(a),"\n"
                                        sitewriter.writerow(a)

a = sys.argv[1]
b = sys.argv[2]
fieldsofTable(a,b)
