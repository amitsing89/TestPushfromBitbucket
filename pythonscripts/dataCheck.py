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
import MySQLdb
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import servercredentials
import logging

logging.basicConfig(filename='/opt/amit_scripts_python/log/datacomparison.log', level=logging.DEBUG)

# me = "amit.singh2@timesinternet.in"
# database = MySQLdb.connect(host="192.169.33.145",user ="expresso145",passwd="expresso@145",db="til_expresso_db")
# cur = database.cursor()
# sql = "create table if not exists data_count_allaggregations (Date varchar(20) primary key,RLIS varchar(10),LIS varchar(10),Publisher varchar(10),Advertiser varchar(10))"
# cur.execute(sql)
red = servercredentials.redisconnection()


def clickCountRedisrlis(key, totalclicklis, totalimprrlis):
    totalclickrlis = 0
    clickcount = 0
    clickcount1 = 0
    imprcountrlis = 0
    keyconverted = 'CL:{0}'.format(key)
    val = ['0']
    val = val + red.hvals(keyconverted)
    for i in val:
        y = i.split(",")
        for j in y:
            keyRedis = ("RLIS:{0}").format(j)
            valueOfRedisLIneitem = red.hgetall(keyRedis)
            for j in valueOfRedisLIneitem:
                val = valueOfRedisLIneitem[j]
                b = int(j) / 1000
                t = datetime.datetime.fromtimestamp(b).strftime('%Y-%m-%d %H:%M:%S')
                if sys.argv[1] in t:
                    v = json.loads(valueOfRedisLIneitem[j])
                    if "click" in v:
                        clickcount = clickcount + v['click']
                        if 'impr' in v:
                            imprcountrlis = imprcountrlis + v['impr']
    totalclickrlis = clickcount + clickcount1
    pubclientids = servercredentials.publishersList()
    totalclickpublishers = 0
    totalimpressionpublisher = 0
    totalspendpublisher = 0
    totalconversioncount = 0
    sdclick = 0
    sgpub = 0
    for k in pubclientids:
        imprcount = 0
        clickcount = 0
        spendcount = 0
        clickcountsd = 0
        conversioncount = 0
        sgcount = 0
        strs = "select * from pubdashboarddailyv2 WHERE clientid ={0} and date ='{1}'".format(k, sys.argv[1])
        rows = servercredentials.cassandraProd(strs)
        for user_row in rows:
            if user_row.spend:
                spendcount = spendcount + user_row.spend
            if user_row.click:
                clickcount = clickcount + user_row.click
            if user_row.impr:
                imprcount = imprcount + user_row.impr
            if user_row.sd:
                clickcountsd = clickcountsd + user_row.sd
            if user_row.conv:
                conversioncount = conversioncount + user_row.conv
            if user_row.sg:
                sgcount = sgcount + user_row.sg
        totalclickpublishers = totalclickpublishers + clickcount
        totalimpressionpublisher = totalimpressionpublisher + imprcount
        totalspendpublisher = totalspendpublisher + spendcount
        totalconversioncount = totalconversioncount + conversioncount
        sdclick = sdclick + clickcountsd
        sgpub = sgpub + sgcount
    advclientids = servercredentials.advertiserList()
    totaladvclicks = 0
    totalimpradv = 0
    totalspendadv = 0
    totalsdadv = 0
    totaladvconv = 0
    sgadv = 0
    for client in advclientids:
        imprcount = 0
        clickcount = 0
        spendcount = 0
        sdadv = 0
        convadv = 0
        sgcountad = 0
        strs = "select * from advdashboarddailyv2 where clientid ={0} and date ='{1}'".format(client, sys.argv[1])
        rows = servercredentials.cassandraProd(strs)
        for user_row in rows:
            if user_row.spend:
                spendcount = spendcount + user_row.spend
            if user_row.click:
                clickcount = clickcount + user_row.click
            if user_row.impr:
                imprcount = imprcount + user_row.impr
            if user_row.sd:
                sdadv = sdadv + user_row.sd
            if user_row.conv:
                convadv = convadv + user_row.conv
            if user_row.sg:
                sgcountad = sgcountad + user_row.sg
        totaladvclicks = totaladvclicks + clickcount
        totalimpradv = totalimpradv + imprcount
        totalspendadv = totalspendadv + spendcount
        totalsdadv = totalsdadv + sdadv
        totaladvconv = totaladvconv + convadv
        sgadv = sgadv + sgcountad
    me = "columbia.data@timesinternet.in"
    to = ["abhishek.maheshwari@timesinternet.in", "asheesh.mahor@timesinternet.in", "amit.singh2@timesinternet.in",
          "kundan.kumar1@timesinternet.in", "gaurav.sharma8@timesinternet.in", "mohit.kundra@timesinternet.in",
          "varun.arora1@timesinternet.in", "colombia.analytics@timesinternet.in"]
    text = ""
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Difference in Data Stats for day ={0}".format(sys.argv[1])
    msg['From'] = me
    msg['To'] = ", ".join(to)
    if totalclickpublishers != totaladvclicks:
        text = "Click Difference in Publisher and Advertiser,\n and that is equalt to- \t"
        clickdiff = str(totaladvclicks - totalclickpublishers)
        text = text + clickdiff
    if totalclicklis != totaladvclicks:
        text = text + "\n\nClick Difference in LIS key and Advertiser,\n and that is equalt to- \t"
        clickdiffer = str(totaladvclicks - totalclicklis)
        text = text + clickdiffer
    if totalclickrlis != totalclicklis:
        text = text + "\n\nClick Difference in LIS key and RLIS key,\n and that is equalt to- \t"
        clickdiff = str(totalclicklis - totalclickrlis)
        text = text + clickdiff
    if totalconversioncount != totaladvconv:
        text = text + "\n\nDifference in Conversion,\n and that is equalt to- \t"
        clickdiff = str(totaladvconv - totalconversioncount)
        text = text + clickdiff
    if totalspendadv != totalspendpublisher:
        text = text + "\n\nDifference in Spend,\n and that is equalt to- \t"
        clickdiff = str(totalspendadv - totalspendpublisher)
        text = text + clickdiff
    if sdclick != totalsdadv:
        text = text + "\n\nDifference in Sd,\n and that is equalt to- \t"
        sdDiff = str(sdclick - totalsdadv)
        text = text + sdDiff
    if sgadv != sgpub:
        text = text + "\n\nDifference in Sg,\n which is equal to- \t"
        sgDiff = str(sgadv - sgpub)
        text = text + sgDiff
    part = MIMEText(text, 'plain')
    msg.attach(part)
    s = smtplib.SMTP('192.168.24.21')
    s.sendmail(me, to, msg.as_string())
    logging.info(part)
    print ("PUB CLICKS-", totalclickpublishers)
    print ("ADV CLICKS", totaladvclicks)
    print ("LIS CLICKS", totalclicklis)
    print ("RLIS CLICKS", totalclickrlis)
    print ("ADV IMPR", totalimpradv)
    print ("LIS IMPR", totalimprrlis)
    print ("RLIS IMPR", imprcountrlis)
    print ("ADV SPEND", totalspendadv)
    print ("PUB SPEND", totalspendpublisher)
    print ("PUB Impr", totalimpressionpublisher)
    print ("PUB SD", sdclick)
    print ("ADV SD", totalsdadv)
    print ("PUB CONV", totalconversioncount)
    print ("ADV CONV", totaladvconv)
    print ("ADV SG", sgadv)
    print ("PUB SG", sgpub)
    print sys.argv[
        1], ",", totalimpradv, ",", totaladvclicks, ",", totalspendadv, ",", totalsdadv, ",,", totalimpressionpublisher, ",", totalclickpublishers, ",", totalspendpublisher, ",", sdclick, ",,", totalimprrlis, ",", totalclicklis, ",", imprcountrlis, ",", totalclickrlis, ",", totalconversioncount, ",", totaladvconv


# insertionquery = "insert into data_count_allaggregations (Date ,RLIS ,LIS ,Publisher ,Advertiser) values(\"{0}\",{1},{2},{3},{4}) on duplicate key update RLIS=values(RLIS),LIS=values(LIS),Publisher=values(Publisher),Advertiser=values(Advertiser)"
# insertingdata = insertionquery.format(sys.argv[1],totalclickrlis,totalclicklis,totalclickpublishers,totaladvclicks)
# cur.execute(insertingdata)
# logging.info(insertingdata)
# database.commit()



def clickCountRedis(key):
    print "Date- ", sys.argv[1]
    totalclicklis = 0
    clickcount = 0
    clickcount1 = 0
    imprcount = 0
    keyconverted = 'CL:{0}'.format(key)
    val = ['0']
    val = val + red.hvals(keyconverted)
    for i in val:
        y = i.split(",")
        for j in y:
            keyRedis = ("LIS:{0}").format(j)
            valueOfRedisLIneitem = red.hgetall(keyRedis)
            for j in valueOfRedisLIneitem:
                val = valueOfRedisLIneitem[j]
                b = int(j) / 1000
                t = datetime.datetime.fromtimestamp(b).strftime('%Y-%m-%d %H:%M:%S')
                if sys.argv[1] in t:
                    v = json.loads(valueOfRedisLIneitem[j])
                    if 'click' in v:
                        clickcount = clickcount + v['click']
                        if 'impr' in v:
                            imprcount = imprcount + v['impr']
    totalclicklis = clickcount + clickcount1
    clickCountRedisrlis(key, totalclicklis, imprcount)


convdate = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').strftime('%Y%m%d')
try:
    clickCountRedis(convdate)
except Exception as e:
    print e
    logging.error(e)
