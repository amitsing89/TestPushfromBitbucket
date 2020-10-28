import servercredentials
import sys
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

red = servercredentials.redisconnection()

# QC Cluster Informations
# qccluster = Cluster(contact_points=['192.169.34.239'], protocol_version=3)
qccluster = Cluster(contact_points=['192.169.33.135'], protocol_version=3)
session2 = qccluster.connect()
session2.default_timeout = 30
session2.set_keyspace('wls')
session2.execute('USE wls')
key = red.keys("CLNTS:*")
advclientids = []

# Fetching and adding the advertiserclientids
for i in key:
    s = i.split(":")
    advclientids.append(s[1])

key1 = red.keys("PUB:*")
pubclientids = []

# Fetching and adding the publisherclientids
for i in key1:
    s = i.split(":")
    pubclientids.append(s[1])


# Insertion to pubdashboardv2 table
def insertintoPubDashboard(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            for hour in range(0, 2):
                strs = "select * from pubdashboardv2 WHERE clientid ={0} and date ='2016-09-0{1} 0{2}:00:00'".format(k,
                                                                                                                     l,
                                                                                                                     hour)
                insertQuery = "insert into pubdashboardv2 (clientid,date,siteid,sectionid,click,sg,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(strs)
                # print k,l
                print strs
                for x in rows:
                    session2.execute(insertQuery,
                                     [x.clientid, x.date, x.siteid, x.sectionid, x.click, x.sg, x.impr, x.inorgclick,
                                      x.orgclick, x.pv, x.spend])


# Insertion to pubgeov2 table
def insertionintoPubGeo(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            for hour in range(0, 2):
                strspubgeov2 = "select * from pubgeov2 WHERE clientid ={0} and date ='2016-09-0{1} 0{2}:00:00'".format(
                    k, l, hour)
                insertQuerypubgeov2 = "insert into pubgeov2 (clientid,date,geodimensionid,click,sg,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(strspubgeov2)
                print strspubgeov2
                for x in rows:
                    session2.execute(insertQuerypubgeov2,
                                     [x.clientid, x.date, x.geodimensionid, x.click, x.sg, x.impr, x.inorgclick,
                                      x.orgclick, x.pv, x.spend])


# Insertion to pubadslotv2 table
def insertionintoPubAdalot(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            for hour in range(0, 2):
                strspubadslotv2 = "select * from pubadslotv2 WHERE clientid ={0} and date ='2016-09-0{1} 0{2}:00:00'".format(
                    k, l, hour)
                insertQuerypubadslotv2 = "insert into pubadslotv2 (clientid,date,siteid,sectionid,adslotid,click,sg,impr,inorgclick,orgclick,pv,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(strspubadslotv2)
                print strspubadslotv2
                for x in rows:
                    session2.execute(insertQuerypubadslotv2,
                                     [x.clientid, x.date, x.siteid, x.sectionid, x.adslotid, x.click, x.sg, x.impr,
                                      x.inorgclick, x.orgclick, x.pv, x.spend])


# Insertion into pubsitewisegeov2 table
def insertionPubSiteWiseGeo(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            for hour in range(0, 2):
                strspubsitewisegeov2 = "select * from pubsitewisegeov2 WHERE clientid ={0} and date ='2016-09-0{1} 0{2}:00:00'".format(
                    k, l, hour)
                insertQuerypubsitewisegeov2 = "insert into pubsitewisegeov2 (clientid,date,siteid,sectionid,geodimensionid,click,sg,impr,inorgclick,orgclick,pv,spend) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(strspubsitewisegeov2)
                print strspubsitewisegeov2
                for x in rows:
                    session2.execute(insertQuerypubsitewisegeov2,
                                     [x.clientid, x.date, x.siteid, x.sectionid, x.geodimensionid, x.click, x.sg,
                                      x.impr, x.inorgclick, x.orgclick, x.pv, x.spend])


# Insertion into advsectionv2 table
def insertionadvSection(advclientids, dates):
    for l in dates:
        for k in advclientids:
            for hour in range(0, 2):
                strsadvsection = "SELECT * FROM advsectionv2 WHERE clientid={0} and date IN ('2016-09-0{1} 0{2}:00:00+0530') ".format(
                    k, l, hour)
                insertQuery = "insert into advsectionv2 (clientid,date,siteid,lineitemid,sectionid,click,sg,impr,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(strsadvsection)
                print strsadvsection
                for user_row in rows:
                    session2.execute(insertQuery,
                                     [user_row.clientid, user_row.date, user_row.siteid, user_row.lineitemid,
                                      user_row.sectionid, user_row.click, user_row.sg, user_row.impr, user_row.spend])


# Insertion into advcontentv2 table
def insertionadvContent(advclientids, dates):
    for l in dates:
        for k in advclientids:
            for hour in range(0, 2):
                stsradvcontentv2 = "SELECT * FROM advcontentv2 WHERE clientid={0} and date IN ('2016-09-0{1} 0{2}:00:00+0530') ".format(
                    k, l, hour)
                insertQuerycontentv2 = "insert into advcontentv2 (clientid,date,lineitemid,itemid,click,sg,impr,spend) values(%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(stsradvcontentv2)
                print stsradvcontentv2
                for user_row in rows:
                    session2.execute(insertQuerycontentv2,
                                     [user_row.clientid, user_row.date, user_row.lineitemid, user_row.itemid,
                                      user_row.click, user_row.sg, user_row.impr, user_row.spend])


# insertion into advsitewisegeov2 table
def insertionstrsadvesitewisev2(advclientids, dates):
    for l in dates:
        for k in advclientids:
            for hour in range(0, 2):
                strsadvesitewisev2 = "SELECT * FROM advsitewisegeov2 WHERE clientid={0} and date IN ('2016-09-0{1} 0{2}:00:00+0530') ".format(
                    k, l, hour)
                insertQueryadvsitewisegeov2 = "insert into advsitewisegeov2 (clientid,date,geodimensionid,siteid,lineitemid,click,sg,impr,inorgclick,orgclick,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(strsadvesitewisev2)
                print strsadvesitewisev2
                for user_row in rows:
                    session2.execute(insertQueryadvsitewisegeov2,
                                     [user_row.clientid, user_row.date, user_row.geodimensionid, user_row.siteid,
                                      user_row.lineitemid, user_row.click, user_row.sg, user_row.impr,
                                      user_row.inorgclick, user_row.orgclick, user_row.spend])


# Insertion into advgeov2 table
def insertionadvGeo(advclientids, dates):
    for l in dates:
        for k in advclientids:
            for hour in range(11, 12):
                strsadvgeov2 = "SELECT * FROM advgeov2 WHERE clientid={0} and date IN ('2016-09-0{1} 0{2}:00:00+0530') ".format(
                    k, l, hour)
                insertQuerystrsadvgeov2 = "insert into advgeov2 (clientid,date,lineitemid,geodimensionid,click,sg,impr,spend) values(%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(strsadvgeov2)
                print strsadvgeov2
                for user_row in rows:
                    session2.execute(insertQuerystrsadvgeov2,
                                     [user_row.clientid, user_row.date, user_row.lineitemid, user_row.geodimensionid,
                                      user_row.click, user_row.sg, user_row.impr, user_row.spend])


def insertionpubadslotgeoestv2(pubclientids, dates):
    for l in dates:
        for k in pubclientids:
            for hour in range(0, 24):
                strspubadslot = "select * from pubadslotgeoestv2 WHERE clientid ={0} and date ='2016-09-0{1} 0{2}:00:00+0530'".format(
                    k, l, hour)
                insertQuerystrspubadslot = "insert into pubadslotgeoestv2 (clientid,date,adslotdimid,geodimensionid,click,sg,impr,inorgclick,orgclick,spend) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                rows = servercredentials.cassandraProd(strspubadslot)
                print strspubadslot
                for user_row in rows:
                    session2.execute(insertQuerystrspubadslot,
                                     [user_row.clientid, user_row.date, long(user_row.adslotdimid),
                                      user_row.geodimensionid, user_row.click, user_row.sg, user_row.impr,
                                      user_row.inorgclick, user_row.orgclick, user_row.spend])


# limit is For Days upto which data should be added
# Example 31 for full month which is of 30 days
# and 32 for full month which is of 31 days

# limit = sys.argv[1]


# 2016-09-05 16:00:00
# insertintoPubDashboard(pubclientids, range(21,22))
# insertionintoPubGeo(pubclientids, range(21,22))
# insertionintoPubAdalot(pubclientids, range(21,22))
# insertionPubSiteWiseGeo(pubclientids, range(21,22))
# insertionadvSection(advclientids, range(21,22))
# insertionadvContent(advclientids, range(21,22))
# insertionstrsadvesitewisev2(advclientids, range(21,22))
insertionadvGeo(advclientids, range(21, 22))
# insertionpubadslotgeoestv2(pubclientids, range(6,7))
