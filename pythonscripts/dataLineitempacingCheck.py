import csv
from datetime import date, timedelta
import json
from datetime import datetime
from collections import OrderedDict
import servercredentials
import pytz
import sys
import simplejson
from urllib2 import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime

pubclientids = servercredentials.publishersList()
advclientids = servercredentials.advertiserList()
advclientids = map(int,advclientids)
pubclientids = map(int,pubclientids)

lineitemlist = servercredentials.lineitemList(sys.argv[1])

def reporting(lineitemlist):
        totalclicks = 0
        for lineitem in lineitemlist:
                for day in range(int(sys.argv[2]),int(sys.argv[3])):
                        for hour in range(0,24):
                                minute = 0
                                while minute <60:
                                        strs = "select * from lineitempacingv2 where lineitemid={0} and date='{1} 0{2}:0{3}:00'".format(lineitem,sys.argv[4],hour,minute)
                                        rows = servercredentials.cassandraProd(strs)
                                        minute = minute +15
                                        clicks = 0
                                        for a in rows:
                                                clicks = clicks + a.click
                                        totalclicks = totalclicks+clicks
        print totalclicks

reporting(lineitemlist)

