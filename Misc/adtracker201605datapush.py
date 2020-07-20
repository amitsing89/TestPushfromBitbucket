from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import datetime
import sys

usingcluster = Cluster(contact_points=['192.169.34.239'], protocol_version=3)
session1 = usingcluster.connect()
session1.set_keyspace('adlog')
session1.execute('USE adlog')

localCluster = Cluster(contact_points=['192.168.34.231'], protocol_version=3)
session = localCluster.connect()
session.set_keyspace('adlog')
session.execute('USE adlog')


def insertiontoAdtracker201605():
    for hour in range(0, 24):
        for minute in range(0, 60):
            selectQuery = ''
            if hour <= 9:
                if minute <= 9:
                    selectQuery = "SELECT * FROM adtracker201605 WHERE datehourmin =201605190{0}0{1}".format(hour,
                                                                                                             minute)
                elif minute > 9:
                    selectQuery = "SELECT * FROM adtracker201605 WHERE datehourmin =201605190{0}{1}".format(hour,
                                                                                                            minute)
            elif hour > 9:
                if minute <= 9:
                    selectQuery = "SELECT * FROM adtracker201605 WHERE datehourmin =20160519{0}0{1}".format(hour,
                                                                                                            minute)
                if minute > 9:
                    selectQuery = "SELECT * FROM adtracker201605 WHERE datehourmin =20160519{0}{1}".format(hour, minute)
                # rows = servercredentials.cassandraProdadlog(selectQuery)
            rows = session1.execute(selectQuery)
            insertquery = "insert into adtracker{0}{1} (datehourmin,clientid,userid,createtime,adid,adlogtype,adunitid,advertiserappname,advertiserpkgname,androidadvertisingid,androidadvertisingidmd5,androidid,campaignname,clickid,cookiemap,countrycode,createdby,earlyattribution,eventname,geographyid,geoid,goalid,goaltime,group,impressionid,iosadvertisingid,iosadvertisingidmd5,ipaddress,isattributed,itemid,jsts,lineitemid,network,platformid,ptime,reqmap,source,sourceappname,sourcepkgname,v,vendor) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            for user_row in rows:
                millis = user_row.createtime / 1000.0
                dayvar = datetime.datetime.fromtimestamp(millis).strftime('%Y-%m-%d %H:%M:%S')
                splitValue = dayvar.split()
                datevar = splitValue[0]
                timevar = splitValue[1]
                dateSplit = datevar.split('-')
                timeSplit = timevar.split(':')
                insertionQuery = insertquery.format(dateSplit[0], dateSplit[1])
                clientidcheck = user_row.clientid
                useridcheck = user_row.userid
                createtimecheck = user_row.createtime
                datehourmin = str(dateSplit[0]) + str(dateSplit[1]) + str(dateSplit[2]) + str(timeSplit[0]) + str(
                    timeSplit[1])
                datehourmincheck = int(datehourmin)
                queryCheck = "select * from adtracker201605 where datehourmin={0} and clientid = {1} and userid ='{2}' and createtime = {3}".format(
                    datehourmincheck, clientidcheck, useridcheck, createtimecheck)
                rowscheck = session.execute(queryCheck)
                if not rowscheck:
                    print "Insertion Executed"
                    session.execute(insertionQuery,
                                    [int(datehourmin), user_row.clientid, user_row.userid, user_row.createtime,
                                     user_row.adid, user_row.adlogtype, user_row.adunitid, user_row.advertiserappname,
                                     user_row.advertiserpkgname, user_row.androidadvertisingid,
                                     user_row.androidadvertisingidmd5, user_row.androidid, user_row.campaignname,
                                     user_row.clickid, user_row.cookiemap, user_row.countrycode, user_row.createdby,
                                     user_row.earlyattribution, user_row.eventname, user_row.geographyid,
                                     user_row.geoid, user_row.goalid, user_row.goaltime, user_row.group,
                                     user_row.impressionid, user_row.iosadvertisingid, user_row.iosadvertisingidmd5,
                                     user_row.ipaddress, user_row.isattributed, user_row.itemid, user_row.jsts,
                                     user_row.lineitemid, user_row.network, user_row.platformid, user_row.ptime,
                                     user_row.reqmap, user_row.source, user_row.sourceappname, user_row.sourcepkgname,
                                     user_row.v, user_row.vendor])
                else:
                    print "ROW ALREADY FOUND"


insertiontoAdtracker201605()
