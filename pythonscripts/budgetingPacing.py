import redis
import sys
import json
import datetime
from mailer import Mailer
from mailer import Message
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date, timedelta

red = redis.Redis(host='localhost', db=0)

me = "amit.singh2@timesinternet.in"
to = ["asheesh.mahor@timesinternet.in", "saurabh.chandolia@timesinternet.in", "amit.singh2@timesinternet.in",
      "kundan.kumar1@timesinternet.in", "gaurav.sharma8@timesinternet.in"]
msg = MIMEMultipart('alternative')
msg['Subject'] = "Difference in Clicks of LIS and RLIS"
msg['From'] = me
msg['To'] = ", ".join(to)


def dateToString(dateTime, format='%Y-%m-%d %H:%M:%S'):
    return datetime.datetime.strftime(dateTime, format)


def clickCountRedis(key):
    totalclick = 0
    clickcount = 0
    clickcount1 = 0
    keyconverted = 'CL:{0}'.format(key)
    val = []
    redlisclicks = {}
    val = val + red.hvals(keyconverted)
    # print val
    for i in val:
        clicklis = 0
        y = i.split(",")
        for j in y:
            # print j
            keyRedis = ("LIS:{0}").format(j)
            valueOfRedisLIneitem = red.hgetall(keyRedis)
            # print valueOfRedisLIneitem
            for j in valueOfRedisLIneitem:
                val = valueOfRedisLIneitem[j]
                b = int(j) / 1000
                t = datetime.datetime.fromtimestamp(b).strftime('%Y-%m-%d %H:%M:%S')
                s = datetime.date.fromordinal(datetime.date.today().toordinal() - 1)
                convdate = dateToString(s)
                if t in convdate:
                    v = json.loads(valueOfRedisLIneitem[j])
                    clickcount = clickcount + v['click']
                    # clicklis = clicklis + v['click']
                    lid = v['lineItemId']
                    redlisclicks[lid] = v['click']
                # print "LIS,Lineitem,Clicks",lid,v['click']

    # print clickcount
    # print clickcount1
    # print redlisclicks
    totalclick = clickcount + clickcount1
    # print key
    # print "LIS-",totalclick
    clickCountRedisrlis(sys.argv[1], totalclick, redlisclicks)


def clickCountRedisrlis(key, totalclick, redlisclicks):
    totalclickrlis = 0
    clickcount = 0
    clickcount1 = 0
    keyconverted = 'CL:{0}'.format(key)
    val = []
    redlineitemclick = {}
    val = val + red.hvals(keyconverted)
    for i in val:
        y = i.split(",")
        for j in y:
            keyRedis = ("RLIS:{0}").format(j)
            valueOfRedisLIneitem = red.hgetall(keyRedis)
            for j in valueOfRedisLIneitem:
                # clickrlis = 0
                val = valueOfRedisLIneitem[j]
                b = int(j) / 1000
                t = datetime.datetime.fromtimestamp(b).strftime('%Y-%m-%d %H:%M:%S')
                s = datetime.date.fromordinal(datetime.date.today().toordinal() - 1)
                convdate = dateToString(s)
                if t in convdate:
                    v = json.loads(valueOfRedisLIneitem[j])
                    clickcount = clickcount + v['click']
                    lid = v['lineItemId']
                    # print "RLIS,Lineitemid,Click",lid,v['click']
                    redlineitemclick[lid] = v['click']
    # print clickcount
    # print clickcount1
    # print redlineitemclick
    # print "LIS-",redlineitemclick
    # print "RLIS-",redlisclicks
    totalclickrlis = clickcount + clickcount1
    print "RLIS-", totalclickrlis
    print "LIS-", totalclick
    for i in redlineitemclick:
        if redlineitemclick[i] != redlisclicks[i]:
            print i, redlineitemclick[i], redlisclicks[i]
    if totalclick != totalclickrlis:
        differenceinclicks = str(totalclick - totalclickrlis)
        text = "Difference in clicks in LIS and RLIS,\n and that is equal to \t"
        text = text + differenceinclicks
        part1 = MIMEText(text, 'plain')
        msg.attach(part1)
        # s = smtplib.SMTP('192.168.24.21')
        # s.sendmail(me,to, msg.as_string())
        print 'difference is found', differenceinclicks
    else:
        print 'Data is fine'


clickCountRedis(sys.argv[1])
