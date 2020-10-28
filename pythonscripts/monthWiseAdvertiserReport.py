import csv
from datetime import date, timedelta
import json
from datetime import datetime
from collections import OrderedDict
import servercredentials
import pytz
import simplejson
from urllib2 import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import datetime

today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
lastyear = int(lastMonth.strftime("%Y"))
lastm = int(lastMonth.strftime("%m"))
lastdayofmonth = int(lastMonth.strftime("%d"))
monthName = lastMonth.strftime("%B")

advclientids = servercredentials.advertiserList()
advclientids = map(int, advclientids)


def reportMonthweise():
    with open("/var/www/html/monthreport/" + monthName + str(lastyear) + "Report.csv", 'a') as csvfile:
        sitewriter = csv.writer(csvfile, delimiter=',', quotechar=',', quoting=csv.QUOTE_MINIMAL)
        sitewriter.writerow(
            ["Date", "clientid", "lineitemid", "siteid", "sectionid", "click", "conv", "impr", "sd", "sg", "spend"])
        for day in range(1, (lastdayofmonth + 1)):
            query = "select * from advlineitemsectiondailyreportv2 where date = '{0}-{1}-{2}'".format(lastyear, lastm,
                                                                                                      day)
            rows = servercredentials.cassandraProd(query)
            for a in rows:
                convertedDate = (a.date.replace(tzinfo=pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
                sitewriter.writerow(
                    [convertedDate, a.clientid, a.lineitemid, a.siteid, a.sectionid, a.click, a.conv, a.impr, a.sd,
                     a.sg, a.spend])


reportMonthweise()
