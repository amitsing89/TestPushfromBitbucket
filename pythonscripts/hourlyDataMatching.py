import redis
import json
import datetime
from datetime import date, timedelta
import sys
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from mailer import Mailer
from mailer import Message
import smtplib
import servercredentials
import threading
from threading import Thread
from _thread import start_new_thread
import time

red = servercredentials.redisconnection()


# print red.keys("CLNTS:*")
# key = red.keys("CLNTS:*")
# key1=red.keys("PUB:*")

def advclientdetails(day, lowerlimit, upperlimit):
    # print "LS"
    # key = red.keys("CLNTS:*")
    advclientids = servercredentials.activeAdvertiserList(convdate)
    # for i in key:
    #        s = i.split(":")
    #        advclientids.append(s[1])
    datadetails = {}
    datadetailsImpression = {}
    for hour in range(lowerlimit, upperlimit):
        clickcount = 0
        imprcount = 0
        for k in advclientids:
            strs1 = "SELECT * FROM advgeov2 WHERE clientid={0} and date IN ('{1} 0{2}:00:00+0530')".format(k, day, hour)
            rows = servercredentials.cassandraProd(strs1)
            for user_row in rows:
                clickcount = clickcount + user_row.click
                imprcount = imprcount + user_row.impr
        datadetails[hour] = clickcount
        datadetailsImpression[hour] = imprcount
    print(datadetails)
    pubclientdetails(datadetails, datadetailsImpression, day, lowerlimit, upperlimit)


def advclientdetails22(day, lowerlimit, upperlimit):
    # print "LS"
    # key =red.keys("CLNTS:*")
    advclientids = servercredentials.advertiserList()
    # for i in key:
    #	s=i.split(":")
    #	advclientids.append(s[1])
    datadetails = {}
    datadetailsImpression = {}
    datadetailsSd = {}
    datadetailsConversion = {}
    for hour in range(lowerlimit, upperlimit):
        clickcount = 0
        imprcount = 0
        sd = 0
        conversion = 0
        for k in advclientids:
            strs1 = "SELECT * FROM {0} WHERE clientid={1} and date IN ('{2} 0{3}:00:00+0530')".format(sys.argv[2], k,
                                                                                                      day, hour)
            rows = servercredentials.cassandraProd(strs1)
            for user_row in rows:
                if user_row.click:
                    clickcount = clickcount + user_row.click
                if user_row.impr:
                    imprcount = imprcount + user_row.impr
                if user_row.sd is not None:
                    sd = sd + user_row.sd
                if user_row.conv:
                    conversion = conversion + user_row.conv
        datadetails[hour] = clickcount
        datadetailsImpression[hour] = imprcount
        datadetailsSd[hour] = sd
        datadetailsConversion[hour] = conversion
    # print clickcount
    # print datadetails
    pubclientdetails(datadetails, datadetailsImpression, datadetailsSd, day, lowerlimit, upperlimit,
                     datadetailsConversion)


# key1=red.keys("PUB:*")

def pubclientdetails(datadetails, datadetailsImpression, datadetailsSd, day, lowerlimit, upperlimit,
                     datadetailsConversion):
    # key1=red.keys("PUB:*")
    pubclientids = servercredentials.publishersList()
    # for i in key1:
    #	s=i.split(":")
    #	pubclientids.append(s[1])
    datadetailspublisher = {}
    datadetailimprpublisher = {}
    datadetailsSdpublisher = {}
    datadetailsConversionpublisher = {}
    for hour in range(lowerlimit, upperlimit):
        clickcount = 0
        imprcount = 0
        sd = 0
        conversion = 0
        for k in pubclientids:
            strs1 = "SELECT * FROM {0} WHERE clientid={1} and date IN ('{2} 0{3}:00:00+0530')".format(sys.argv[3], k,
                                                                                                      day, hour)
            rows = servercredentials.cassandraProd(strs1)
            for user_row in rows:
                if user_row.click:
                    clickcount = clickcount + user_row.click
                if user_row.impr:
                    imprcount = imprcount + user_row.impr
                # print strs1
                # if user_row.sd is not None:
                # sd = sd + user_row.sd
                # if user_row.conv:
                # conversion = conversion + user_row.conv
        datadetailimprpublisher[hour] = imprcount
        datadetailspublisher[hour] = clickcount
        # datadetailsSdpublisher[hour] = sd
        datadetailsConversionpublisher[hour] = conversion
    # print clickcount
    summationcount = 0
    for keys in datadetails:
        if keys in datadetailspublisher:
            if datadetailspublisher[keys] == datadetails[keys]:
                summationcount = summationcount + int(datadetails[keys])
                print("CLICK Data is Matching at every hour", keys, datadetailspublisher[keys], datadetails[keys])
            else:
                summationcount = summationcount + int(datadetails[keys])
                print("CLICK Mismatch at hour with data key,publisher,advertiser-", keys, datadetailspublisher[keys], \
                    datadetails[keys])
            # print summationcount
            # print "PUBSD",datadetailsSdpublisher
            # print "ADVSD",datadetailsSd
            # print "ADVSIDE",datadetails
            # ,datadetailsImpression,datadetailsConversion
            # print "PUBSIDE",datadetailspublisher
            # ,datadetailimprpublisher,datadetailsConversionpublisher
            # print datadetailspublisher
            # for keys in datadetailimprpublisher:
            # if keys in datadetailimprpublisher:
            # if datadetailimprpublisher[keys]==datadetailsImpression[keys]:
            # print "IMPRESSION Data is Matching at every hour",keys,datadetailimprpublisher[keys],datadetailsImpression[keys]
            # else:
            # print "IMPRESSION Mismatch at hour with data key,publisher,advertiser-",keys,datadetailimprpublisher[keys],datadetailsImpression[keys]


convdate = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').strftime('%Y%m%d')
print(convdate)
try:
    for i in range(0, 24):
        if i < 24:
            t = threading.Thread(target=advclientdetails22,
                                 kwargs={'day': sys.argv[1], 'lowerlimit': i, 'upperlimit': int(i + 1)})
            time.sleep(1)
            t.start()
except Exception as e:
    print(e)
