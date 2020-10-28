import servercredentials
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import datetime
import sys

qccluster = Cluster(contact_points=['192.168.34.234'], protocol_version=3)
session = qccluster.connect()
session.set_keyspace('adlog')
session.execute('USE adlog')


def insertiontoAdtracker201605():
    for day in range(18, 19):
        for hour in range(13, 14):
            strs = "select * from adtracker where year = 2016 and month = 05 and day = {0} and hour = {1}".format(day,
                                                                                                                  hour)
            rows = servercredentials.cassandraProdadlog(strs)
            insertquery = "insert into adtracker{0}{1} (datehourmin,clientid,userid,createtime,adid,adlogtype,adunitid,advertiserappname,advertiserpkgname,androidadvertisingid,androidadvertisingidmd5,androidid,campaignname,clickid,cookiemap,countrycode,createdby,earlyattribution,eventname,geographyid,geoid,goalid,goaltime,group,impressionid,iosadvertisingid,iosadvertisingidmd5,ipaddress,isattributed,itemid,jsts,lineitemid,network,platformid,ptime,reqmap,source,sourceappname,sourcepkgname,v,vendor) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for a in rows:
                millis = a.createtime / 1000.0
                dayvar = datetime.datetime.fromtimestamp(millis).strftime('%Y-%m-%d %H:%M:%S')
                splitValue = dayvar.split()
                datevar = splitValue[0]
                timevar = splitValue[1]
                dateSplit = datevar.split('-')
                timeSplit = timevar.split(':')
                datehourmin = str(dateSplit[0]) + str(dateSplit[1]) + str(dateSplit[2]) + str(timeSplit[0]) + str(
                    timeSplit[1])
                clientidcheck = a.clientid
                useridcheck = a.userid
                createtimecheck = a.createtime
                datehourmin = str(dateSplit[0]) + str(dateSplit[1]) + str(dateSplit[2]) + str(timeSplit[0]) + str(
                    timeSplit[1])
                datehourmincheck = int(datehourmin)
                insertionquery = insertquery.format(dateSplit[0], dateSplit[1])
                queryCheck = "select * from adtracker201605 where datehourmin={0} and clientid = {1} and userid ='{2}' and createtime = {3}".format(
                    datehourmincheck, clientidcheck, useridcheck, createtimecheck)
                rowscheck = servercredentials.cassandraProdadlog(queryCheck)
                # print (rowscheck)
                if not rowscheck:
                    print "Insertion Executed"
                    # print insertionquery
                    session.execute(insertionquery,
                                    [int(datehourmin), a.clientid, a.userid, a.createtime, a.adid, a.adlogtype,
                                     a.adunitid, a.advertiserappname, a.advertiserpkgname, a.androidadvertisingid,
                                     a.androidadvertisingidmd5, a.androidid, a.campaignname, a.clickid, a.cookiemap,
                                     a.countrycode, a.createdby, a.earlyattribution, a.eventname, a.geographyid,
                                     a.geoid, a.goalid, a.goaltime, a.group, a.impressionid, a.iosadvertisingid,
                                     a.iosadvertisingidmd5, a.ipaddress, a.isattributed, a.itemid, a.jsts, a.lineitemid,
                                     a.network, a.platformid, a.ptime, a.reqmap, a.source, a.sourceappname,
                                     a.sourcepkgname, a.v, a.vendor])
                else:
                    print "ROW ALREADY FOUND"


insertiontoAdtracker201605()
