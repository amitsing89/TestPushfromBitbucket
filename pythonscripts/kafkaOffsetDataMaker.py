#/usr/bin
import redis
import collections
import sys
import datetime
import json
from subprocess import call
import commands
import logging
from subprocess import call
from subprocess import Popen
import subprocess
from mailer import Mailer
from mailer import Message
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import commands

serverip = commands.getoutput("/sbin/ifconfig").split("\n")[1].split()[1][5:]


#Required format for the conversion attribution
#0-0:4160,1-0:5486,2-0:6143,3-0:5486,4-0:6143 2015-06-08-14-00-00,2015-06-08-14-00-00


os.chdir("/opt/deploy/bin")

#Process=Popen(["cd /opt/deploy/bin"],shell=True)

logging.basicConfig(filename='/log/kafkaOffsetGenerator.log',level=logging.DEBUG)
red = redis.Redis(host='192.169.34.221', db =0)

keyscurrentoffset = red.keys('BACOFST*')

def currentOffsetFetcher():
    currentOffset = {}
    for i in keyscurrentoffset:
        split_val = i.split(':')
        currentOffset[split_val[1]] = red.get(i)
    logging.info(currentOffset)
    previousHourOffset(currentOffset)



def previousHourOffset(currentOffset):
    reqstr = ''
    convFormateddate = datetime.datetime.strptime(sys.argv[1], '%Y%m%d%H').strftime('%Y-%m-%d-%H-%M-%S')
    keylist = currentOffset.keys()
    dayvar = sys.argv[1]
    prevHourOffSet = {}
    i = 0
    mydate = datetime.datetime.strptime(dayvar, '%Y%m%d%H')
    while i <24:
        k = mydate-datetime.timedelta(hours = 1)
        i = i+1
        mydate = k
        keyvar = "BAOFST:{0}".format(mydate.strftime('%Y%m%d%H'))
	#print keyvar
        offsetDict = red.hgetall(keyvar)
	#print offsetDict
        if not offsetDict:
            logging.info("BAOFST: Key Not Found")
        else:
            for j in offsetDict:
                if j in keylist:
                    keylist.remove(j)
                    prevHourOffSet[j] = offsetDict[j]
		    #print prevHourOffSet[j],offsetDict[j]
    lengthofpartition = len(prevHourOffSet)
    lengthofpartitionprevoffset = len(currentOffset)
    logging.info(prevHourOffSet)
    logging.info(currentOffset)
    #print currentOffset,prevHourOffSet
    if lengthofpartitionprevoffset!= lengthofpartition:
	me = "colombia.data@timesinternet.in"
	to = ["asheesh.mahor@timesinternet.in","abhishek.maheshwari@timesinternet.in","kundan.kumar1@timesinternet.in","gaurav.sharma8@timesinternet.in","varun.arora1@timesinternet.in"]
	msg = MIMEMultipart('alternative')
        msg['Subject'] = "Offset not found within 24 hours "
        msg['From'] = me
        msg['To'] = ", ".join(to)
        text = "FATAL ERROR within 24 Hour Key offset not Found"
	text = text + "\n for ip :- " +serverip
        part1 = MIMEText(text, 'plain')
        msg.attach(part1)
	logging.info(part1)
        s = smtplib.SMTP('192.168.24.21')
        s.sendmail(me,to, msg.as_string())
        logging.info("FATAL ERROR within 24 Hour Key offset not Found")
	
    commaseprationCheck = 0
    for partition in prevHourOffSet:
        commaseprationCheck = commaseprationCheck + 1
        reqstr = reqstr+partition+"-"+prevHourOffSet[partition]+":"+currentOffset[partition]
        if commaseprationCheck<lengthofpartition:
            reqstr = reqstr+","
    completeStr = reqstr+" "+convFormateddate+","+convFormateddate
    convertedDateparam = convFormateddate+","+convFormateddate
    #print completeStr
    #print reqstr
    checkstr = "./startConversionAttributionJob.sh COLUMBIA-AGG-RELEASE-0.99-TAG26 0 %s "% completeStr
    print checkstr
    logging.info(checkstr)
    #Process = Popen([checkstr],shell=True)

   


currentOffsetFetcher()
