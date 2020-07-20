import redis
import json
from cassandra.cluster import Cluster
import sys
import MySQLdb
import datetime
import servercredentials
import logging

logging.basicConfig(filename='/opt/amit_scripts_python/log/attributionmysqlpush.log', level=logging.DEBUG)

sql = "create table if not exists conversion_count (clientid varchar(10),clientname varchar(75),date varchar(20),successful int(30), failed int(30), primary key(clientid,date))"
servercredentials.mysql(sql)

red = servercredentials.redisconnection()


# Method for calculating the Failed and Successful cases for conversion tracking.
def algoAttributionDetails(year, month, day):
    red = servercredentials.redisconnection()
    key = "CPGC:{0}{1}{2}".format(year, month, day)
    advclientids = red.smembers(key)
    mod = []
    for i in range(0, 1001):
        mod.append(i)
    print(tuple(mod))
    for adv in advclientids:
        for hour in range(0, 24):
            for minute in range(0, 60):
                datehourminvar = str(year) + str(month) + str(day)
                if hour <= 9:
                    if minute <= 9:
                        datehourminvar = datehourminvar + str(0) + str(hour) + str(0) + str(minute)
                    elif minute > 9:
                        datehourminvar = datehourminvar + str(0) + str(hour) + str(minute)
                elif hour > 9:
                    if minute <= 9:
                        datehourminvar = datehourminvar + str(hour) + str(0) + str(minute)
                    elif minute > 9:
                        datehourminvar = datehourminvar + str(hour) + str(minute)
                query = "select * from adtracker{0}{1} where clientid={2} and datehourmin = {3} and mod in {4}".format(
                    year, month, adv, int(datehourminvar), tuple(mod))
                logging.info(query)
                rows = servercredentials.cassandraProdadlog(query)
                # print rows
                # print query
                for user_row in rows:
                    jobStatus = user_row.jsts
                    isAtributed = user_row.isattributed
                    # print jobStatus
                    # print user_row.clientid
                    # Checing for job status, jsts = 3 is for successful
                    if isAtributed is True:
                        # print "Success",user_row.clientid,user_row.createtime
                        convertedDate = datetime.datetime.fromtimestamp(user_row.createtime / 1000).strftime(
                            '%Y-%m-%d 00:00:00')
                        insertionquerysuccessful = """INSERT INTO  conversion_count(clientid,clientname,date,successful,failed) VALUES({0},\"{1}\",\"{2}\",{3},{4}) ON DUPLICATE KEY UPDATE successful = (successful +1)"""
                        # print datainsertion
                        key = "CLNTS:{0}".format(user_row.clientid)
                        # print key
                        if red.exists(key) == 1:
                            val = json.loads(red.get(key))
                            clientname = val['cname']
                            datainsertion = insertionquerysuccessful.format(user_row.clientid, clientname,
                                                                            convertedDate, 1, 0)
                            # splitval = val.split(':')
                            # clientname = splitval[2].split('}')
                            # print clientname[0]
                            logging.info(datainsertion)
                            servercredentials.mysql(datainsertion)
                    elif isAtributed is not True:
                        # print "Failure",user_row.clientid,user_row.createtime
                        key = "CLNTS:{0}".format(user_row.clientid)
                        # print key
                        if red.exists(key) == 1:
                            val = json.loads(red.get(key))
                            clientname = val['cname']
                            convertedDate = datetime.datetime.fromtimestamp(user_row.createtime / 1000).strftime(
                                '%Y-%m-%d 00:00:00')
                            insertionqueryfailed = """INSERT INTO  conversion_count(clientid,clientname,date,successful,failed) VALUES({0},\"{1}\",\"{2}\",{3},{4}) ON DUPLICATE KEY UPDATE failed = (failed +1) """.format(
                                user_row.clientid, clientname, convertedDate, 0, 1)
                            logging.info(insertionqueryfailed)
                            servercredentials.mysql(insertionqueryfailed)


try:
    algoAttributionDetails(sys.argv[1], sys.argv[2], sys.argv[3])
except Exception as e:
    logging.error(e)
