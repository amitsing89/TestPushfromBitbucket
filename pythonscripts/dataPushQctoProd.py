import redis
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

cluster = Cluster(contact_points=['192.168.34.234', '192.168.34.231', '192.168.34.232', '192.168.34.233'],
                  protocol_version=3)
session = cluster.connect()
session.set_keyspace('wls')
session.execute('USE wls')
qccluster = Cluster(contact_points=['192.169.33.131', '192.169.33.132', '192.169.33.133'], protocol_version=3)
session2 = qccluster.connect()
session2.set_keyspace('wls')
session2.execute('USE wls')
red = redis.Redis(host='192.168.34.235', db=0)
key = red.keys("CLNTS:*")
advclientids = []
for i in key:
    s = i.split(":")
    advclientids.append(s[1])


def insertintoPubDashboard(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            imprcount = 0
            clickcount = 0
            spendcount = 0
            strs = "select * from pubdashboarddailyv2 WHERE clientid ={0} and date ='2016-01-0{1}'".format(k, l)
            insertQuery = "insert into pubdashboarddailyv2 (clientid,date,siteid,sectionid,click,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = session.execute(SimpleStatement(strs, consistency_level=ConsistencyLevel.QUORUM))
            print strs
            # print k,l
            for x in rows:
                session2.execute(insertQuery,
                                 [x.clientid, x.date, x.siteid, x.sectionid, x.click, x.impr, x.inorgclick, x.orgclick,
                                  x.pv, x.spend])


def insertionintoPubGeoDaily(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            strspubgeodailyv2 = "select * from pubgeodailyv2 WHERE clientid ={0} and date ='2016-01-0{1}'".format(k, l)
            insertQuerypubgeodailyv2 = "insert into pubgeodailyv2 (clientid,date,geodimensionid,click,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = session.execute(SimpleStatement(strspubgeodailyv2, consistency_level=ConsistencyLevel.QUORUM))
            print strspubgeodailyv2
            for x in rows:
                session2.execute(insertQuerypubgeodailyv2,
                                 [x.clientid, x.date, x.geodimensionid, x.click, x.impr, x.inorgclick, x.orgclick, x.pv,
                                  x.spend])


def insertionintoPubAdalotDaily(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            strspubadslotdailyv2 = "select * from pubadslotdailyv2 WHERE clientid ={0} and date ='2016-01-0{1}'".format(
                k, l)
            insertQuerypubadslotdailyv2 = "insert into pubadslotdailyv2 (clientid,date,siteid,sectionid,adslotid,click,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows2 = session.execute(SimpleStatement(strspubadslotdailyv2, consistency_level=ConsistencyLevel.QUORUM))
            print strspubadslotdailyv2
            for x in rows2:
                session2.execute(insertQuerypubadslotdailyv2,
                                 [x.clientid, x.date, x.siteid, x.sectionid, x.adslotid, x.click, x.impr, x.inorgclick,
                                  x.orgclick, x.pv, x.spend])


def insertionPubSiteWiseGeoDaily(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            strspubsitewisegeodailyv2 = "select * from pubsitewisegeodailyv2 WHERE clientid ={0} and date ='2016-01-0{1}'".format(
                k, l)
            insertQuerypubsitewisegeodailyv2 = "insert into pubsitewisegeodailyv2 (clientid,date,siteid,sectionid,geodimensionid,click,impr,inorgclick,orgclick,pv,spend) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows3 = session.execute(
                SimpleStatement(strspubsitewisegeodailyv2, consistency_level=ConsistencyLevel.QUORUM))
            print strspubsitewisegeodailyv2
            for x in rows3:
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
            strsadvsection = "SELECT * FROM advsectiondailyv2 WHERE clientid={0} and date IN ('2016-01-0{1} 00:00:00+0530') ".format(
                k, l)
            insertQuery = "insert into advsectiondailyv2 (clientid,date,siteid,lineitemid,sectionid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            rows = session.execute(SimpleStatement(strsadvsection, consistency_level=ConsistencyLevel.QUORUM))
            print strsadvsection
            for user_row in rows:
                session2.execute(insertQuery, [user_row.clientid, user_row.date, user_row.siteid, user_row.lineitemid,
                                               user_row.sectionid, user_row.click, user_row.impr, user_row.spend])


def insertionadvContentDaily(advclientids, dates):
    for l in dates:
        for k in advclientids:
            stsradvcontentdailyv2 = "SELECT * FROM advcontentdailyv2 WHERE clientid={0} and date IN ('2016-01-0{1} 00:00:00+0530') ".format(
                k, l)
            insertQuerycontentdailyv2 = "insert into advcontentdailyv2 (clientid,date,lineitemid,itemid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s)"
            rows = session.execute(SimpleStatement(stsradvcontentdailyv2, consistency_level=ConsistencyLevel.QUORUM))
            print stsradvcontentdailyv2
            for user_row in rows:
                session2.execute(insertQuerycontentdailyv2,
                                 [user_row.clientid, user_row.date, user_row.lineitemid, user_row.itemid,
                                  user_row.click, user_row.impr, user_row.spend])


def insertionstrsadvesitewisedailyv2(advclientids, dates):
    for l in dates:
        for k in advclientids:
            strsadvesitewisedailyv2 = "SELECT * FROM advsitewisegeodailyv2 WHERE clientid={0} and date IN ('2016-01-0{1} 00:00:00+0530') ".format(
                k, l)
            insertQueryadvsitewisegeodailyv2 = "insert into advsitewisegeodailyv2 (clientid,date,geodimensionid,siteid,lineitemid,click,impr,inorgclick,orgclick,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            rows2 = session.execute(SimpleStatement(strsadvesitewisedailyv2, consistency_level=ConsistencyLevel.QUORUM))
            print strsadvesitewisedailyv2
            for user_row in rows2:
                session2.execute(insertQueryadvsitewisegeodailyv2,
                                 [user_row.clientid, user_row.date, user_row.geodimensionid, user_row.siteid,
                                  user_row.lineitemid, user_row.click, user_row.impr, user_row.inorgclick,
                                  user_row.orgclick, user_row.spend])


def insertionadvGeoDaily(advclientids, dates):
    for l in dates:
        for k in advclientids:
            strsadvgeodailyv2 = "SELECT * FROM advgeodailyv2 WHERE clientid={0} and date IN ('2016-01-0{1} 00:00:00+0530') ".format(
                k, l)
            insertQuerystrsadvgeodailyv2 = "insert into advgeodailyv2 (clientid,date,lineitemid,geodimensionid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s)"
            rows3 = session.execute(SimpleStatement(strsadvgeodailyv2, consistency_level=ConsistencyLevel.QUORUM))
            print strsadvgeodailyv2
            for user_row in rows3:
                session2.execute(insertQuerystrsadvgeodailyv2,
                                 [user_row.clientid, user_row.date, user_row.lineitemid, user_row.geodimensionid,
                                  user_row.click, user_row.impr, user_row.spend])


insertintoPubDashboard(pubclientids, range(1, 32))
insertionintoPubGeoDaily(pubclientids, range(1, 32))
insertionintoPubAdalotDaily(pubclientids, range(1, 32))
insertionPubSiteWiseGeoDaily(pubclientids, range(1, 32))
insertionadvSectionDaily(advclientids, range(1, 32))
insertionadvContentDaily(advclientids, range(1, 32))
insertionstrsadvesitewisedailyv2(advclientids, range(1, 32))
insertionadvGeoDaily(advclientids, range(1, 32))
