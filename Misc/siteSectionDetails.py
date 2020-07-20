import redis
from datetime import date, timedelta
import json
from cassandra.cluster import Cluster
from datetime import datetime
from collections import OrderedDict
import csv
import sys
from mailer import Mailer
from mailer import Message
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import servercredentials
import logging

logging.basicConfig(filename='/opt/amit_scripts_python/log/sitesection.log', level=logging.DEBUG)

red = servercredentials.redisconnection()


def dateToString(dateTime, format='%Y-%m-%d'):
    return datetime.strftime(dateTime, format)


def mailSender(body, dTime):
    me = "amit.singh2@timesinternet.in"
    to = "amit.singh2@timesinternet.in", "saurabh.chandolia@timesinternet.in", "asheesh.mahor@timesinternet.in", "mukesh.parmar@timesinternet.in", "Ankit.Bansal@timesinternet.in", "varun.arora1@timesinternet.in"
    # cc = "saurabh.chandolia@timesinternet.in"
    # to = "amit.singh2@timesinternet.in"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Lineitems which run on same site and section with other lineitem on Client on day {0}'.format(
        dTime)
    msg['From'] = me
    msg['To'] = ", ".join(to)
    part1 = MIMEText(body, 'plain')
    msg.attach(part1)
    logging.info(msg.as_string())
    # print msg.as_string()
    s = smtplib.SMTP('localhost21')
    s.sendmail(me, to, msg.as_string())


# Method for calculating the Lineitems which were running on CPD
# Further get the ClientIds of advertiser for which
# def cpdRevenueMismatch(clientkey):
input_clientkey = sys.argv[1]
print "Date- ", sys.argv[1]
clientkey = "CL:{0}".format(input_clientkey)
key = red.keys(clientkey)
val = red.hvals(key[0])
liddetail = []
# Collecting all the lineitems from CL: cache
for i in val:
    if len(i) > 6:
        y = i.split(",")
        for j in y:
            liddetail.append(j)
    else:
        liddetail.append(i)

cpdlineitems = []
# Collecting lineitems which run on cpd basis
for lid in liddetail:
    strs = "LID:{0}".format(lid)
    if red.exists(strs) == 1:
        a = json.loads(red.get(strs))
        if a['spstrategy'] == 1:
            # print lid
            cpdlineitems.append(lid)

cId = []
# getting the client id for cpd lineitems
for i in cpdlineitems:
    strs1 = "ES:{0}".format(i)
    key_es = red.keys(strs1)
    value_es = json.loads(red.get(key_es[0]))
    cId.append(value_es['cId'])


# print cId


# Calculating the site and section of lineitems whichn runs on cpd and the other lineitem which runs on same site and section
def datacount(strs, cpdlineitems, clientid, dTime):
    # print cpdlineitems,clientid
    text = "site | Section | CPDlineitem | OtherLineitem | ClientId\n"
    text1 = "site | Section | CPDlineitem | ClientId\n"
    comparisonstr = "site | Section | CPDlineitem | OtherLineitem | ClientId\n"
    comparisonstr1 = "site | Section | CPDlineitem | ClientId\n"
    listoflineitem = []
    listofSiteandSection = []
    rows = servercredentials.cassandraProd(strs)
    otherlineitem = []
    othercpdlineitem = []
    sitecpdlineitem = {}
    mix = {}
    cpdline = 0
    sitesection = ''
    for user_row in rows:
        listoflineitem.append(user_row.lineitemid)
        listofSiteandSection.append(str(user_row.siteid) + " | " + str(user_row.sectionid))
    for i in range(0, len(listofSiteandSection)):
        a = listofSiteandSection[i]
        for j in range(0, len(listofSiteandSection)):
            b = listofSiteandSection[j]
            if a == b:
                if str(listoflineitem[i]) in cpdlineitems:
                    if int(listoflineitem[i]) != int(listoflineitem[j]):
                        cpdline = listoflineitem[i]
                        othercpdlineitem.append(cpdline)
                        sitesection = a
                        if listoflineitem[j] not in otherlineitem:
                            if listoflineitem[j] not in othercpdlineitem:
                                othercpdlineitem.append(cpdline)
                                if str(listoflineitem[j]) not in cpdlineitems:
                                    otherlineitem.append(listoflineitem[j])
                if cpdline != 0 and len(otherlineitem) != 0:
                    othercpdlineitemset1 = list(OrderedDict.fromkeys(othercpdlineitem))
                    if otherlineitem:
                        mix[str(sitesection)] = (
                            '  |  ' + ','.join(str(x) for x in othercpdlineitemset1) + '  |  ' + ','.join(
                                str(x) for x in otherlineitem) + '  |  ' + str(clientid))
                    otherlineitem = []
                if len(othercpdlineitem) >= 2:
                    othercpdlineitemset1 = list(OrderedDict.fromkeys(othercpdlineitem))
                    sitecpdlineitem[str(sitesection)] = (
                        '  |  ' + ','.join(str(x) for x in othercpdlineitemset1) + '  |  ' + str(clientid))
    for line in sitecpdlineitem:
        if line not in mix:
            # print mix[line]
            text1 = text1 + line + str(sitecpdlineitem[line]) + "\n"
            # print "text1",text1
    for lines in mix:
        text = text + lines + str(mix[lines]) + "\n"
    if comparisonstr == text:
        print "Nothing"
    else:
        print "FFFF"
        mailSender(text, dTime)
    if comparisonstr1 == text1:
        print "Nothing"
    else:
        mailSender(text1, dTime)


t = list(OrderedDict.fromkeys(cId))
for i in t:
    d = date.today() - timedelta(days=1)
    dTime = dateToString(d)
    strs = "select * from advsectiondailyv2 where clientid={0} and date='{1}'".format(i, d)
    datacount(strs, cpdlineitems, i, dTime)
