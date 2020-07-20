import redis
from cassandra.cluster import Cluster
import sys
from mailer import Mailer
from mailer import Message
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import servercredentials
import logging

logging.basicConfig(filename='/opt/amit_scripts_python/log/algoattribution.log', level=logging.DEBUG)

red = servercredentials.redisconnection()


def algoAttributionDetails(year, month, day):
    red = servercredentials.redisconnection()
    key = "CPGC:{0}{1}{2}".format(year, month, day)
    advclientids = red.smembers(key)
    me = "amit.singh2@timesinternet.in"
    # to = "amit.singh2@timesinternet.in"
    to = ["asheesh.mahor@timesinternet.in", "saurabh.chandolia@timesinternet.in", "amit.singh2@timesinternet.in"]
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Apsalar and Mat Data Of Year= {0} , Month={1}, day={2}".format(year, month, day)
    msg['From'] = me
    msg['To'] = ", ".join(to)
    row_counter = 0
    apsalarSuccessCount = 0
    matSuccessfulCount = 0
    othersuccessfulcount = 0
    apsalarCount = 0
    matCount = 0
    othercount = 0
    apsalarFailedCount = 0
    matFailedCount = 0
    otherfailedcount = 0
    mod = []
    for i in range(0, 1001):
        mod.append(i)
    for adv in advclientids:
        for hour in range(0, 24):
            for minute in range(0, 60):
                datehourminvar = str(year) + str(month) + str(day)
                if hour <= 9:
                    if minute <= 9:
                        datehourminvar = datehourminvar + str(0) + str(hour) + str(0) + str(minute)
                    elif minute > 9:
                        datehourminvar = datehourminvar + str(0) + str(hour) + str(minute)
                elif hour > 9:
                    if minute <= 9:
                        datehourminvar = datehourminvar + str(hour) + str(0) + str(minute)
                    elif minute > 9:
                        datehourminvar = datehourminvar + str(hour) + str(minute)
                query = "select * from adtracker{0}{1} where clientid = {2} and datehourmin = {3} and mod in {4}".format(
                    year, month, adv, int(datehourminvar), tuple(mod))
                # print query
                logging.info(query)
                rows = servercredentials.cassandraProdadlog(query)
                for user_row in rows:
                    row_counter = row_counter + 1
                    requestMap = user_row.reqmap
                    jobStatus = user_row.jsts
                    isAttributed = user_row.isattributed
                    imprid = user_row.impressionid
                    other = True
                    for data in requestMap:
                        if 'os' in data:
                            osDetails = requestMap[data]
                            if 'UnKnown, More-Info: Apsalar-Postback' in osDetails:
                                other = False
                                apsalarCount = apsalarCount + 1
                                if isAttributed is True:
                                    apsalarSuccessCount = apsalarSuccessCount + 1
                                elif isAttributed is not True:
                                    apsalarFailedCount = apsalarFailedCount + 1
                                    key = ("JSTS:{0}").format(jobStatus)
                                    val = red.get(key)
                                    # print "Apsalar-",user_row.clientid
                                    # print "ImpressionId-",imprid,"\nReason For Failure-",val
                            elif 'UnKnown, More-Info: HasOffers Mobile AppTracking v1.0' in osDetails:
                                other = False
                                matCount = matCount + 1
                                if isAttributed is True:
                                    matSuccessfulCount = matSuccessfulCount + 1
                                elif isAttributed is not True:
                                    matFailedCount = matFailedCount + 1
                                    key = ("JSTS:{0}").format(jobStatus)
                                    val = red.get(key)
                                    # print"MAT", user_row
                                    # print "ImpressionId-",imprid,"\nReason For Failure-",val
                    if other == True:
                        othercount = othercount + 1
                        if isAttributed is True:
                            othersuccessfulcount = othersuccessfulcount + 1
                        elif isAttributed is not True:
                            otherfailedcount = otherfailedcount + 1
                            key = ("JSTS:{0}").format(jobStatus)
                            val = red.get(key)
            datehourminvar = ''
    print "Apsalar Data - ", apsalarCount, apsalarSuccessCount, apsalarFailedCount
    print "MAT Data - ", matCount, matSuccessfulCount, matFailedCount
    print "Rest Data - ", othercount, othersuccessfulcount, otherfailedcount
    print "Total Rows in Table -", row_counter
    apsFailPercent = float(apsalarFailedCount) / float(apsalarCount) * 100
    matFailPercent = float(matFailedCount) / float(matCount) * 100
    restFailPercent = float(otherfailedcount) / float(othercount) * 100
    text = "Matrics      Total Count      SuccessFullCount    FailedCount     Difference%"
    text = text + "\nApsalar Data - {0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(apsalarCount, apsalarSuccessCount,
                                                                         apsalarFailedCount, apsFailPercent)
    text = text + "\nMat Data      - {0}\t\t  {1}\t\t\t  {2}\t\t\t{3}".format(matCount, matSuccessfulCount,
                                                                              matFailedCount, matFailPercent)
    text = text + "\nRest Data     - {0}\t\t  {1}\t\t\t  {2}\t\t\t{3}".format(othercount, othersuccessfulcount,
                                                                              otherfailedcount, restFailPercent)
    part1 = MIMEText(text, 'plain')
    msg.attach(part1)
    logging.info(part1)
    s = smtplib.SMTP('localhost')
    s.sendmail(me, to, msg.as_string())


try:
    algoAttributionDetails(sys.argv[1], sys.argv[2], sys.argv[3])
except Exception as e:
    logging.error(e)
