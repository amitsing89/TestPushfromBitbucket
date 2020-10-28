import redis
import json
import datetime
from datetime import date, timedelta
import sys
# from cassandra.cluster import *

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, dict_factory
from cassandra import ConsistencyLevel
from mailer import Mailer
from mailer import Message
import smtplib
import MySQLdb
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import csv
from redis.sentinel import Sentinel

# print "Connecting to redis"
sentinel = Sentinel([('localhost157', 26379), ('localhost236', 26379), ('localhost235', 26379)],
                    socket_timeout=10)
red = sentinel.master_for('mymaster', socket_timeout=10)
# red = redis.Redis(host='localhost235', db=0)


pubKeys = red.keys("PUB:*")
advKeys = red.keys("CLNTS:*")

# print "Connecting to Cassandra"
cluster = Cluster(contact_points=['localhost234', 'localhost231', 'localhost232', 'localhost233'],
                  protocol_version=3)
session = cluster.connect()

qccluster = Cluster(contact_points=['localhost131', 'localhost132', 'localhost133'], protocol_version=3)
session2 = qccluster.connect()
print("Connecting to MySQL")
database = MySQLdb.connect(host="localhost145", user="expresso145", passwd="expresso@145", db="til_expresso_db")
cur = database.cursor()


def cassandraDictRowfinder(query):
    session.set_keyspace('wls')
    # print dir(session)
    session.row_factory = dict_factory
    rows = session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
    return rows


def sessionReturn():
    session.set_keyspace('wls')
    session.execute('use wls')
    return session


def sessionReturnadlog():
    session.set_keyspace('adlog')
    session.execute('use adlog')
    return session


def cassandraProd(query):
    session.set_keyspace('wls')
    session.execute('use wls')
    rows = session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
    # rows = session.execute(query)
    return rows


def cassandraProdDmp(query):
    session.set_keyspace('dmp')
    session.execute('use dmp')
    rows = session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
    # rows = session.execute(query)
    return rows


def cassandraDictRowfinderDmp(query):
    session.set_keyspace('dmp')
    session.execute('use dmp')
    # print dir(session)
    session.row_factory = dict_factory
    rows = session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
    return rows


def cassandraProdadlog(query):
    session.set_keyspace('adlog')
    session.execute('use adlog')
    rows = session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
    return rows


def cassandraQc(query):
    session2.set_keyspace('wls')
    rows = session2.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
    return rows


def redisconnection():
    master = sentinel.master_for('mymaster', socket_timeout=4)
    return master


def mysql(sql):
    cur.execute(sql)
    database.commit()


def publishersList():
    pubclientids = []
    # red = sentinel.master_for('mymaster', socket_timeout=4)
    # key1 = red.keys("PUB:*")
    for i in pubKeys:
        s = i.split(":")
        pubclientids.append(s[1])
    return pubclientids


def activeAdvertiserList(day):
    activeadvclient = []
    clientkey = "CL:{0}".format(day)
    # red = sentinel.master_for('mymaster', socket_timeout=4)
    val = red.hgetall(clientkey)
    for i in val:
        activeadvclient.append(i)
    return activeadvclient


def advertiserList():
    advclientids = []
    # red = sentinel.master_for('mymaster', socket_timeout=4)
    # key = red.keys("CLNTS:*")
    for i in advKeys:
        s = i.split(":")
        advclientids.append(s[1])
    return advclientids


def lineitemList(day):
    lineitems = []
    # red = sentinel.master_for('mymaster', socket_timeout=4)
    clientkey = "CL:{0}".format(day)
    key = red.keys(clientkey)
    val = red.hvals(key[0])
    for i in val:
        y = i.split(",")
        for j in y:
            lineitems.append(j)
    return lineitems
