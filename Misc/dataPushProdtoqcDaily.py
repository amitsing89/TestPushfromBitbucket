import servercredentials
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import sys

lineitemlist = servercredentials.lineitemList(sys.argv[1])
print lineitemlist
red = servercredentials.redisconnection()
qccluster = Cluster(contact_points=['192.169.33.131', '192.169.33.135', '192.169.33.133'], protocol_version=3)
session2 = qccluster.connect()
session2.set_keyspace('wls')
session2.execute('USE wls')
key = red.keys("CLNTS:*")
advclientids = []
for i in key:
    s = i.split(":")
    advclientids.append(s[1])


def insertintoPubDashboard(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            strs = "select * from pubdashboarddailyv2 WHERE clientid ={0} and date ='2016-04-0{1}'".format(k, l)
            insertQuery = "insert into pubdashboarddailyv2 (clientid,date,siteid,sectionid,click,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(strs)
            # print k,l
            print strs
            for x in rows:
                session2.execute(insertQuery,
                                 [x.clientid, x.date, x.siteid, x.sectionid, x.click, x.impr, x.inorgclick, x.orgclick,
                                  x.pv, x.spend])


def insertionintoPubGeoDaily(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            strspubgeodailyv2 = "select * from pubgeodailyv2 WHERE clientid ={0} and date ='2016-04-0{1}'".format(k, l)
            insertQuerypubgeodailyv2 = "insert into pubgeodailyv2 (clientid,date,geodimensionid,click,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(strspubgeodailyv2)
            print strspubgeodailyv2
            for x in rows:
                session2.execute(insertQuerypubgeodailyv2,
                                 [x.clientid, x.date, x.geodimensionid, x.click, x.impr, x.inorgclick, x.orgclick, x.pv,
                                  x.spend])


def insertionintoPubAdalotDaily(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            strspubadslotdailyv2 = "select * from pubadslotdailyv2 WHERE clientid ={0} and date ='2016-04-0{1}'".format(
                k, l)
            insertQuerypubadslotdailyv2 = "insert into pubadslotdailyv2 (clientid,date,siteid,sectionid,adslotid,click,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(strspubadslotdailyv2)
            print strspubadslotdailyv2
            for x in rows:
                session2.execute(insertQuerypubadslotdailyv2,
                                 [x.clientid, x.date, x.siteid, x.sectionid, x.adslotid, x.click, x.impr, x.inorgclick,
                                  x.orgclick, x.pv, x.spend])


def insertionPubSiteWiseGeoDaily(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            strspubsitewisegeodailyv2 = "select * from pubsitewisegeodailyv2 WHERE clientid ={0} and date ='2016-04-0{1}'".format(
                k, l)
            insertQuerypubsitewisegeodailyv2 = "insert into pubsitewisegeodailyv2 (clientid,date,siteid,sectionid,geodimensionid,click,impr,inorgclick,orgclick,pv,spend) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(strspubsitewisegeodailyv2)
            print strspubsitewisegeodailyv2
            for x in rows:
                session2.execute(insertQuerypubsitewisegeodailyv2,
                                 [x.clientid, x.date, x.siteid, x.sectionid, x.geodimensionid, x.click, x.impr,
                                  x.inorgclick, x.orgclick, x.pv, x.spend])


key1 = red.keys("PUB:*")
pubclientids = []
for i in key1:
    s = i.split(":")
    pubclientids.append(s[1])


def insertionadvSectionDaily(advclientids, dates):
    for l in dates:
        for k in advclientids:
            strsadvsection = "SELECT * FROM advsectiondailyv2 WHERE clientid={0} and date IN ('2016-04-0{1} 00:00:00+0530') ".format(
                k, l)
            insertQuery = "insert into advsectiondailyv2 (clientid,date,siteid,lineitemid,sectionid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(strsadvsection)
            print strsadvsection
            for user_row in rows:
                session2.execute(insertQuery, [user_row.clientid, user_row.date, user_row.siteid, user_row.lineitemid,
                                               user_row.sectionid, user_row.click, user_row.impr, user_row.spend])


def insertionadvContentDaily(advclientids, dates):
    for l in dates:
        for k in advclientids:
            stsradvcontentdailyv2 = "SELECT * FROM advcontentdailyv2 WHERE clientid={0} and date IN ('2016-04-0{1} 00:00:00+0530') ".format(
                k, l)
            insertQuerycontentdailyv2 = "insert into advcontentdailyv2 (clientid,date,lineitemid,itemid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(stsradvcontentdailyv2)
            print stsradvcontentdailyv2
            for user_row in rows:
                session2.execute(insertQuerycontentdailyv2,
                                 [user_row.clientid, user_row.date, user_row.lineitemid, user_row.itemid,
                                  user_row.click, user_row.impr, user_row.spend])


def insertionstrsadvesitewisedailyv2(advclientids, dates):
    for l in dates:
        for k in advclientids:
            strsadvesitewisedailyv2 = "SELECT * FROM advsitewisegeodailyv2 WHERE clientid={0} and date IN ('2016-04-0{1} 00:00:00+0530') ".format(
                k, l)
            insertQueryadvsitewisegeodailyv2 = "insert into advsitewisegeodailyv2 (clientid,date,geodimensionid,siteid,lineitemid,click,impr,inorgclick,orgclick,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(strsadvesitewisedailyv2)
            print strsadvesitewisedailyv2
            for user_row in rows:
                session2.execute(insertQueryadvsitewisegeodailyv2,
                                 [user_row.clientid, user_row.date, user_row.geodimensionid, user_row.siteid,
                                  user_row.lineitemid, user_row.click, user_row.impr, user_row.inorgclick,
                                  user_row.orgclick, user_row.spend])


def insertionadvGeoDaily(advclientids, dates):
    for l in dates:
        for k in advclientids:
            strsadvgeodailyv2 = "SELECT * FROM advgeodailyv2 WHERE clientid={0} and date IN ('2016-04-0{1} 00:00:00+0530') ".format(
                k, l)
            insertQuerystrsadvgeodailyv2 = "insert into advgeodailyv2 (clientid,date,lineitemid,geodimensionid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(strsadvgeodailyv2)
            print strsadvgeodailyv2
            for user_row in rows:
                session2.execute(insertQuerystrsadvgeodailyv2,
                                 [user_row.clientid, user_row.date, user_row.lineitemid, user_row.geodimensionid,
                                  user_row.click, user_row.impr, user_row.spend])


def insertionadvLineitemSectionDaily(advclientids, dates):
    for l in dates:
        for k in advclientids:
            strsadvlineitemsection = "SELECT * FROM advlineitemsectiondailyv2 WHERE clientid={0} and date IN ('2016-06-0{1} 00:00:00+0530')".format(
                k, l)
            insertQuery = "insert into advlineitemsectiondailyv2 (clientid,date,lineitemid,siteid,sectionid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(strsadvlineitemsection)
            print strsadvlineitemsection
            for user_row in rows:
                session2.execute(insertQuery, [user_row.clientid, user_row.date, user_row.lineitemid, user_row.siteid,
                                               user_row.sectionid, user_row.click, user_row.impr, user_row.spend])


def insertionAudAggregationdaily(lineitems, day):
    for lineitem in lineitems:
        straudaggregation = "select * from audaggregationdaily where lineitemid ={0} and date = '2016-08-{1}'".format(
            lineitem, day)
        insertQuery = "insert into audaggregationdaily (lineitemid,date,audid,click,conv,erpm,impr,sd,sg,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        rows = servercredentials.cassandraProd(straudaggregation)
        print straudaggregation
        for user_row in rows:
            session2.execute(insertQuery,
                             [user_row.lineitemid, user_row.date, user_row.audid, user_row.click, user_row.conv,
                              user_row.erpm, user_row.impr, user_row.sd, user_row.sg, user_row.spend])


def insertionadvadslotdaily(lineitems, days):
    for lineitem in lineitems:
        for day in days:
            straudaggregation = "select * from advadslotdailyv2 where lineitemid ={0} and date = '2016-10-{1}'".format(
                lineitem, day)
            insertQuery = "insert into advadslotdailyv2 (lineitemid,date,adslotdimid,click,conv,impr,sd,sg,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = servercredentials.cassandraProd(straudaggregation)
            print straudaggregation
            for user_row in rows:
                session2.execute(insertQuery, [user_row.lineitemid, user_row.date, user_row.adslotdimid, user_row.click,
                                               user_row.conv, user_row.impr, user_row.sd, user_row.sg, user_row.spend])


if 'advadslot' in sys.argv[2]:
    insertionadvadslotdaily([483532], range(int(sys.argv[3]), int(sys.argv[4])))
if 'pubashboard' in sys.argv[2]:
    insertintoPubDashboard(pubclientids, range(int(sys.argv[3]), int(sys.argv[4])))
if 'pubgeo' in sys.argv[2]:
    insertionintoPubGeoDaily(pubclientids, range(sys.argv[3], sys.argv[4]))
if 'pubadslot' in sys.argv[2]:
    insertionintoPubAdalotDaily(pubclientids, range(sys.argv[3], sys.argv[4]))
if 'pubsitewise' in sys.argv[2]:
    insertionPubSiteWiseGeoDaily(pubclientids, range(sys.argv[3], sys.argv[4]))
if 'advsection' in sys.argv[2]:
    insertionadvSectionDaily(advclientids, range(sys.argv[3], sys.argv[4]))
if 'advcontent' in sys.argv[2]:
    insertionadvContentDaily(advclientids, range(sys.argv[3], sys.argv[4]))
if 'advesitewise' in sys.argv[2]:
    insertionstrsadvesitewisedailyv2(advclientids, range(sys.argv[3], sys.argv[4]))
if 'advlineitem' in sys.argv[2]:
    insertionadvLineitemSectionDaily(advclientids, range(sys.argv[3], sys.argv[4]))
if 'advgeo' in sys.argv[2]:
    insertionadvGeoDaily(advclientids, range(sys.argv[3], sys.argv[4]))
if 'audag' in sys.argv[2]:
    insertionAudAggregationdaily(lineitemlist, sys.argv[3])
