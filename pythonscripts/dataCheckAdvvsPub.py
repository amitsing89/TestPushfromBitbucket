import redis
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

cluster = Cluster(contact_points=['localhost', 'localhost1', 'localhost2', 'localhost3'],
                  protocol_version=3)
session = cluster.connect()
session.set_keyspace('wls')
session.execute('USE wls')
qccluster = Cluster(contact_points=['localhost', 'localhost1', 'localhost2', 'localhost3'], protocol_version=3)
session2 = qccluster.connect()
session2.set_keyspace('wls')
session2.execute('USE wls')
red = redis.Redis(host='localhost', db=0)
key = red.keys("CLNTS:*")
advclientids = []
for i in key:
    s = i.split(":")
    advclientids.append(s[1])

key1 = red.keys("PUB:*")
pubclientids = []
for i in key1:
    s = i.split(":")
    pubclientids.append(s[1])

totalclicks = 0
for k in pubclientids:
    imprcount = 0
    clickcount = 0
    spendcount = 0
    strs1 = "SELECT * FROM pubsitewisegeov2 WHERE clientid={0} and date IN ('2015-09-01 00:00:00+0530','2015-09-01 01:00:00+0530','2015-09-01 02:00:00+0530','2015-09-01 03:00:00+0530','2015-09-01 04:00:00+0530','2015-09-01 05:00:00+0530','2015-09-01 06:00:00+0530','2015-09-01 07:00:00+0530','2015-09-01 08:00:00+0530','2015-09-01 09:00:00+0530','2015-09-01 10:00:00+0530','2015-09-01 11:00:00+0530','2015-09-01 12:00:00+0530','2015-09-01 13:00:00+0530','2015-09-01 14:00:00+0530','2015-09-01 15:00:00+0530','2015-09-01 16:00:00+0530','2015-09-01 17:00:00+0530','2015-09-01 18:00:00+0530','2015-09-01 19:00:00+0530','2015-09-01 20:00:00+0530','2015-09-01 21:00:00+0530','2015-09-01 22:00:00+0530','2015-09-01 23:00:00+0530')".format(
        k)
    strs = "select * from pubsitewisegeodailyv2 WHERE clientid ={0} and date ='2015-09-01'".format(k)
    rows = session.execute(strs)
    for user_row in rows:
        spendcount = spendcount + user_row.spend
        clickcount = clickcount + user_row.click
        imprcount = imprcount + user_row.impr
    # print k,imprcount,clickcount,spendcount
    # print k,imprcount,clickcount,spendcount
    totalclicks = totalclicks + clickcount

print totalclicks

totalclicks = 0
for j in range(1, 31):
    totalclicks = 0
    for k in advclientids:
        strsadvsection = "SELECT * FROM advsectiondailyv2 WHERE clientid={0} and date IN ('2015-09-0{1} 00:00:00+0530') ".format(
            k, j)
        insertQuery = "insert into advsectiondailyv2 (clientid,date,siteid,lineitemid,sectionid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s,%s)"
        rows = session.execute(SimpleStatement(strsadvsection, consistency_level=ConsistencyLevel.QUORUM))
        for user_row in rows:
            # session2.execute(insertQuery,[user_row.clientid,user_row.date,user_row.siteid,user_row.lineitemid,user_row.sectionid,user_row.click,user_row.impr,user_row.spend])
            imprcount = imprcount + user_row.impr
            clickcount = clickcount + user_row.click
            spendcount = spendcount + user_row.spend
        # print k,imprcount,clickcount,spendcount
        stsradvcontentdailyv2 = "SELECT * FROM advcontentdailyv2 WHERE clientid={0} and date IN ('2015-09-0{1} 00:00:00+0530') ".format(
            k, j)
        insertQuerycontentdailyv2 = "insert into advsectiondailyv2 (clientid,date,lineitemid,itemid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s)"
        rows1 = session.execute(SimpleStatement(stsradvcontentdailyv2, consistency_level=ConsistencyLevel.QUORUM))
        for user_row in rows1:
            # session2.execute(insertQuerycontentdailyv2,[user_row.clientid,user_row.date,user_row.lineitemid,user_row.itemid,user_row.click,user_row.impr,user_row.spend])
            imprcount = imprcount + user_row.impr
            clickcount = clickcount + user_row.click
            spendcount = spendcount + user_row.spend
        advesitewisedailyv2 = "SELECT * FROM advsitewisegeodailyv2 WHERE clientid={0} and date IN ('2015-09-0{1} 00:00:00+0530') ".format(
            k, j)
        insertQueryadvsitewisegeodailyv2 = "insert into advsitewisegeodailyv2 (clientid,date,geodimensionid,siteid,lineitemid,click,impr,inorgclick,orgclick,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        rows2 = session.execute(SimpleStatement(advesitewisedailyv2, consistency_level=ConsistencyLevel.QUORUM))
        for user_row in rows2:
            # session2.execute(insertQueryadvsitewisegeodailyv2,[user_row.clientid,user_row.date,user_row.geodimensionid,user_row.siteid,user_row.lineitemid,user_row.click,user_row.impr,user_row.inorgclick,user_row.orgclick,user_row.spend])
            imprcount = imprcount + user_row.impr
            clickcount = clickcount + user_row.click
            spendcount = spendcount + user_row.spend
        advgeodailyv2 = "SELECT * FROM advgeodailyv2 WHERE clientid={0} and date IN ('2015-09-0{1} 00:00:00+0530') ".format(
            k, j)
        insertQueryadvgeodailyv2 = "insert into advgeodailyv2 (clientid,date,lineitemid,geodimensionid,click,impr,spend) values(%s,%s,%s,%s,%s,%s,%s)"
        rows3 = session.execute(SimpleStatement(advgeodailyv2, consistency_level=ConsistencyLevel.QUORUM))
        for user_row in rows3:
            # session2.execute(insertQueryadvgeodailyv2,[user_row.clientid,user_row.date,user_row.lineitemid,user_row.geodimensionid,user_row.click,user_row.impr,user_row.spend])
            imprcount = imprcount + user_row.impr
            clickcount = clickcount + user_row.click
            spendcount = spendcount + user_row.spend
