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
from tabulate import tabulate
import threading
from threading import Thread
from thread import start_new_thread
import logging

# Log Info
logging.basicConfig(filename='/opt/amit_scripts_python/log/aggregation.log', level=logging.DEBUG)

red = servercredentials.redisconnection()
convdate = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').strftime('%Y%m%d')

table = []
headers = ["TableName", "TotalClicks", "TotalImpr", "TotalSpend", "TotalSd", "TotalConv", "TotalSg"]

advclientids = servercredentials.activeAdvertiserList(convdate)

pubclientids = servercredentials.publishersList()


# qccluster = Cluster(contact_points=['localhost131','localhost133','localhost135'], protocol_version=3)
# session2 = qccluster.connect()
# session2.default_timeout = 30
# session2.set_keyspace('wls')
# session2.execute('USE wls')

def aggregationmethod(tab_name):
    if 'adv' in tab_name:
        if 'daily' in tab_name:
            totaladvclicks = 0
            totalimpradv = 0
            totalspendadv = 0
            totalsdadv = 0
            totaladvconv = 0
            totalsg = 0
            nt = tab_name.replace("\n", "")
            for client in advclientids:
                imprcount = 0
                clickcount = 0
                spendcount = 0
                sdadv = 0
                convadv = 0
                sgadv = 0
                # nt = tab_name.replace("\n", "")
                strsadvertiser = "select * from wls.{0} where clientid ={1} and date ='{2}'".format(nt, client,
                                                                                                    sys.argv[1])
                # print strsadvertiser
                logging.info(strsadvertiser)
                rowsadv = servercredentials.cassandraDictRowfinder(strsadvertiser)
                for user_row in rowsadv:
                    if 'spend' in user_row:
                        spendcount = spendcount + user_row['spend']
                    if 'click' in user_row:
                        clickcount = clickcount + user_row['click']
                    if 'impr' in user_row:
                        imprcount = imprcount + user_row['impr']
                    if 'sd' in user_row:
                        sdadv = sdadv + user_row['sd']
                    if 'conv' in user_row:
                        convadv = convadv + user_row['conv']
                    if 'sg' in user_row:
                        sgadv = sgadv + user_row['sg']
                totaladvclicks = totaladvclicks + clickcount
                totalimpradv = totalimpradv + imprcount
                totalspendadv = totalspendadv + spendcount
                totalsdadv = totalsdadv + sdadv
                totaladvconv = totaladvconv + convadv
                totalsg = totalsg + sgadv
            table.append([nt, totaladvclicks, totalimpradv, totalspendadv, totalsdadv, totaladvconv, totalsg])
        # print table
        else:
            totaladvclicks = 0
            totalimpradv = 0
            totalspendadv = 0
            totalsdadv = 0
            totaladvconv = 0
            totalsg = 0
            nt = tab_name.replace("\n", "")
            for client in advclientids:
                for hour in range(0, 24):
                    imprcount = 0
                    clickcount = 0
                    spendcount = 0
                    sdadv = 0
                    convadv = 0
                    sgadv = 0
                    # nt = tab_name.replace("\n", "")
                    strsadvertiser = "select * from wls.{0} where clientid ={1} and date ='{2} 0{3}:00:00'".format(nt,
                                                                                                                   client,
                                                                                                                   sys.argv[
                                                                                                                       1],
                                                                                                                   hour)
                    logging.info(strsadvertiser)
                    rowsadv = servercredentials.cassandraDictRowfinder(strsadvertiser)
                    for user_row in rowsadv:
                        if 'spend' in user_row and user_row['spend'] is not None:
                            spendcount = spendcount + user_row['spend']
                        if 'click' in user_row and user_row['click'] is not None:
                            clickcount = clickcount + user_row['click']
                        if 'impr' in user_row and user_row['impr'] is not None:
                            imprcount = imprcount + user_row['impr']
                        if 'sd' in user_row and user_row['sd'] is not None:
                            sdadv = sdadv + user_row['sd']
                        if 'conv' in user_row and user_row['conv'] is not None:
                            convadv = convadv + user_row['conv']
                        if 'sg' in user_row and user_row['sg'] is not None:
                            sgadv = sgadv + user_row['sg']
                    totaladvclicks = totaladvclicks + clickcount
                    totalimpradv = totalimpradv + imprcount
                    totalspendadv = totalspendadv + spendcount
                    totalsdadv = totalsdadv + sdadv
                    totaladvconv = totaladvconv + convadv
                    totalsg = totalsg + sgadv
            table.append([nt, totaladvclicks, totalimpradv, totalspendadv, totalsdadv, totaladvconv, totalsg])
    elif 'pub' in tab_name:
        if 'daily' in tab_name:
            totalpubclicks = 0
            totalimprpub = 0
            totalspendpub = 0
            totalsdpub = 0
            totalpubconv = 0
            totalsg = 0
            nt = tab_name.replace("\n", "")
            for client in pubclientids:
                imprcount = 0
                clickcount = 0
                spendcount = 0
                sdpub = 0
                convpub = 0
                sgpub = 0
                # nt = tab_name.replace("\n", "")
                strspublisher = "select * from wls.{0} where clientid ={1} and date ='{2}'".format(nt, client,
                                                                                                   sys.argv[1])
                # rows = servercredentials.cassandraProd(strs)
                logging.info(strspublisher)
                rowspub = servercredentials.cassandraDictRowfinder(strspublisher)
                for user_row in rowspub:
                    if 'spend' in user_row:
                        spendcount = spendcount + user_row['spend']
                    if 'click' in user_row:
                        clickcount = clickcount + user_row['click']
                    if 'impr' in user_row:
                        imprcount = imprcount + user_row['impr']
                    if 'sd' in user_row:
                        sdpub = sdpub + user_row['sd']
                    if 'conv' in user_row:
                        convpub = convpub + user_row['conv']
                    if 'sg' in user_row:
                        sgpub = sgpub + user_row['sg']
                totalpubclicks = totalpubclicks + clickcount
                totalimprpub = totalimprpub + imprcount
                totalspendpub = totalspendpub + spendcount
                totalsdpub = totalsdpub + sdpub
                totalpubconv = totalpubconv + convpub
                totalsg = totalsg + sgpub
            table.append([nt, totalpubclicks, totalimprpub, totalspendpub, totalsdpub, totalpubconv, totalsg])
        else:
            totalpubclicks = 0
            totalimprpub = 0
            totalspendpub = 0
            totalsdpub = 0
            totalpubconv = 0
            totalsg = 0
            nt = tab_name.replace("\n", "")
            for client in pubclientids:
                imprcount = 0
                clickcount = 0
                spendcount = 0
                sdpub = 0
                convpub = 0
                sgpub = 0
                for hour in range(0, 24):
                    strspublisher = "select * from wls.{0} where clientid ={1} and date ='{2} 0{3}:00:00'".format(nt,
                                                                                                                  client,
                                                                                                                  sys.argv[
                                                                                                                      1],
                                                                                                                  hour)
                    logging.info(strspublisher)
                    rowspub = servercredentials.cassandraDictRowfinder(strspublisher)
                    for user_row in rowspub:
                        if 'spend' in user_row and user_row['spend'] is not None:
                            spendcount = spendcount + user_row['spend']
                        if 'click' in user_row and user_row['click'] is not None:
                            clickcount = clickcount + user_row['click']
                        if 'impr' in user_row and user_row['impr'] is not None:
                            imprcount = imprcount + user_row['impr']
                        if 'sd' in user_row and user_row['sd'] is not None:
                            sdpub = sdpub + user_row['sd']
                        if 'conv' in user_row and user_row['conv'] is not None:
                            convpub = convpub + user_row['conv']
                        if 'sg' in user_row and user_row['sg'] is not None:
                            sgpub = sgpub + user_row['sg']
                totalpubclicks = totalpubclicks + clickcount
                totalimprpub = totalimprpub + imprcount
                totalspendpub = totalspendpub + spendcount
                totalsdpub = totalsdpub + sdpub
                totalpubconv = totalpubconv + convpub
                totalsg = totalsg + sgpub
            table.append([nt, totalpubclicks, totalimprpub, totalspendpub, totalsdpub, totalpubconv, totalsg])
        # print table


def countRedisrlisAndlis(key):
    totalclickrlis = 0
    clickcountrlis = 0
    imprcountrlis = 0
    totalimprcountrlis = 0
    totalspendcountrlis = 0
    spendcountrlis = 0
    totalclicklis = 0
    clickcountlis = 0
    imprcountlis = 0
    totalimprcountlis = 0
    totalspendcountlis = 0
    spendcountlis = 0
    keyconverted = 'CL:{0}'.format(key)
    val = ['0']
    val = val + red.hvals(keyconverted)
    totalclick = 0
    totalimpr = 0
    totalspend = 0
    totalsg = 0
    totalsd = 0
    totalconversion = 0
    for i in val:
        y = i.split(",")
        for lineitem in y:
            spendcount = 0
            clickcount = 0
            imprcount = 0
            sdadv = 0
            convadv = 0
            sgadv = 0
            stradvadslot = "select * from wls.advadslotdailyv2 where lineitemid ={0} and date ='{1}'".format(lineitem,
                                                                                                             sys.argv[
                                                                                                                 1])
            rows = servercredentials.cassandraDictRowfinder(stradvadslot)
            logging.info(stradvadslot)
            for user_row in rows:
                if 'spend' in user_row:
                    spendcount = spendcount + user_row['spend']
                if 'click' in user_row:
                    clickcount = clickcount + user_row['click']
                if 'impr' in user_row:
                    imprcount = imprcount + user_row['impr']
                if 'sd' in user_row:
                    sdadv = sdadv + long(user_row['sd'])
                if 'conv' in user_row:
                    convadv = convadv + user_row['conv']
                if 'sg' in user_row:
                    sgadv = sgadv + user_row['sg']
            totalclick = totalclick + clickcount
            totalimpr = totalimpr + imprcount
            totalspend = totalspend + spendcount
            totalsg = totalsg + sgadv
            totalsd = totalsd + sdadv
            totalconversion = totalconversion + convadv
            keyRedis = ("RLIS:{0}").format(lineitem)
            valueOfRedisLIneitem = red.hgetall(keyRedis)
            for j in valueOfRedisLIneitem:
                val = valueOfRedisLIneitem[j]
                b = int(j) / 1000
                t = datetime.datetime.fromtimestamp(b).strftime('%Y-%m-%d %H:%M:%S')
                if sys.argv[1] in t:
                    v = json.loads(valueOfRedisLIneitem[j])
                    if "click" in v:
                        clickcountrlis = clickcountrlis + v['click']
                    if 'impr' in v:
                        imprcountrlis = imprcountrlis + v['impr']
                    if 'spend' in v:
                        spendcountrlis = spendcountrlis + v['spend']
            keyRedislis = ("LIS:{0}").format(lineitem)
            valueOfRedisLineitemlis = red.hgetall(keyRedislis)
            for li in valueOfRedisLineitemlis:
                val = valueOfRedisLineitemlis[li]
                b = int(li) / 1000
                t = datetime.datetime.fromtimestamp(b).strftime('%Y-%m-%d %H:%M:%S')
                if sys.argv[1] in t:
                    v = json.loads(valueOfRedisLineitemlis[li])
                    if "click" in v:
                        clickcountlis = clickcountlis + v['click']
                    if 'impr' in v:
                        imprcountlis = imprcountlis + v['impr']
                    if 'spend' in v:
                        spendcountlis = spendcountlis + v['spend']
    table.append(["advadslotdailyv2", totalclick, totalimpr, totalspend, totalsd, totalconversion, totalsg])
    totalclickrlis = totalclickrlis + clickcountrlis
    totalimprcountrlis = totalimprcountrlis + imprcountrlis
    totalspendcountrlis = totalspendcountrlis + spendcountrlis
    table.append(["RLIS", totalclickrlis, totalimprcountrlis, totalspendcountrlis, 0, 0, 0])
    totalclicklis = totalclicklis + clickcountlis
    totalimprcountlis = totalimprcountlis + imprcountlis
    totalspendcountlis = totalspendcountlis + spendcountlis
    table.append(["LIS", totalclicklis, totalimprcountlis, totalspendcountlis, 0, 0, 0])


filename = open('cassandraTablefile.txt', 'r')

# countRedisrlisAndlis(convdate)

print sys.argv[1]

try:
    for lines in filename:
        threadalgoBilAttributionDetails = threading.Thread(target=aggregationmethod, kwargs={'tab_name': lines})
        threadalgoBilAttributionDetails.start()
        threadalgoBilAttributionDetails.join()
except Exception as e:
    logging.error(e)
    print "except", e

countRedisrlisAndlis(convdate)

v = tabulate(table, headers, tablefmt="html")
print "tab", tabulate(table, headers, tablefmt='orgtbl')
# me = "amit.singh2@timesinternet.in"
# to = ["amit.singh2@timesinternet.in"]
me = "columbia.data@timesinternet.in"
to = ["vishal.arora1@timesinternet.in", "asheesh.mahor@timesinternet.in", "amit.singh2@timesinternet.in",
      "kundan.kumar1@timesinternet.in", "gaurav.sharma8@timesinternet.in", "mohit.kundra@timesinternet.in",
      "varun.arora1@timesinternet.in", "vijendra.dewda@timesinternet.in", "mankeshwar.kumar@timesinternet.in",
      "vinay.pandey4@timesinternet.in", "abhishek.maheshwari@timesinternet.in"]

msg = MIMEMultipart('alternative')
msg['Subject'] = "Data Stats for day ={0}".format(sys.argv[1])
msg['From'] = me
msg['To'] = ", ".join(to)
part = MIMEText(v, 'html')
msg.attach(part)
s = smtplib.SMTP('localhost21')
s.sendmail(me, to, msg.as_string())
