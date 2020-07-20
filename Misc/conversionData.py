import sys
import csv
import servercredentials
import logging
import pytz
import threading
from threading import Thread
import time
import datetime
from cassandra.cluster import *
from cassandra.cluster import Cluster
from mailer import Mailer
from mailer import Message
import smtplib
import MySQLdb
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(filename='/opt/amit_scripts_python/log/conversion.log',level=logging.DEBUG)

red = servercredentials.redisconnection()
session = servercredentials.sessionReturn()

sessionadlog = servercredentials.sessionReturn()
sessionadlog.set_keyspace('adlog')
sessionadlog.execute('use adlog')

key = "CPGC:{0}{1}{2}".format(sys.argv[1],sys.argv[2],sys.argv[3])
advclientids = red.smembers(key)



cluster = Cluster(contact_points=['192.168.34.231','192.168.34.232','192.168.34.233','192.168.34.234'], protocol_version=3)
session2 = cluster.connect()
session2.set_keyspace('wls')
session2.execute('USE wls')



#DateHourMin Variable Creation
def datehourminlistCreator(year,month,day):
  hourvar = int(time.strftime("%H"))
  rangeval = 0
  if hourvar==0 or hourvar==23:
    rangeval = hourvar + 1
  else:
    rangeval = hourvar
    hourvar = hourvar - 1
  datehourminlist = []
  for hour in range(hourvar,rangeval):
  #for hour in range(0,24):
    for minute in range(0,60):
      datehourminvar = str(year)+str(month)+str(day)
      if hour<=9:
        if minute <=9:
          datehourminlist.append(datehourminvar+str(0)+str(hour)+str(0)+str(minute))
        elif minute>9:
          datehourminlist.append(datehourminvar+str(0)+str(hour)+str(minute))
      elif hour>9:
        if minute <=9:
          datehourminlist.append(datehourminvar+str(hour)+str(0)+str(minute))
        elif minute>9:
          datehourminlist.append(datehourminvar+str(hour)+str(minute))
  return datehourminlist



def algoAttributionDetails(year,month,datehourminlist,adv):
	mod = []
	hourlyDict = {}
	hourvar = int(time.strftime("%H")) - 1
	#hourvar = 0
        for num in range(0,1000):
                mod.append(int(num))
        for datehourminvar in datehourminlist:
		hourcheck = int(str(datehourminvar)[8:10])
		if hourcheck>hourvar:
			hourvar = hourvar + 1
		if hourcheck==hourvar:
                        datehourminvarkey = int(str(datehourminvar)[0:10])
                        query = "select * from adlog.adtracker{0}{1} where clientid = {2} and datehourmin = {3} and mod in {4}".format(year,month,adv,int(datehourminvar),tuple(mod))
                        logval = "select * from adlog.adtracker{0}{1} where clientid = {2} and datehourmin = {3}".format(year,month,adv,int(datehourminvar))
			print logval
                        logging.info(logval)
                        rows = sessionadlog.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
                        for a in rows:
                                countccf = 0
                                spendna = 0
                                counteatrs = 0
                                countvc = 0
                                countcc = 0
                                isAtributed = a.isattributed
                                eattribution = a.earlyattribution
                                datehourmin = a.datehourmin
                                lineitemid = a.lineitemid
                                mod1 = a.mod
                                goalid = a.goalid
                                logging.info("datehourmin"+str(datehourmin) )
                                logging.info("mod"+str(mod1))
                                logging.info("Clid"+str(adv))
                                if isAtributed:
                                        if eattribution:
                                                counteatrs = counteatrs + 1
						bilAttributionQuery = "select spend,itemcolumbiaid from wls.ba{0}{1} where clientid = {2} and datehourmin = {3} and mod = {4}".format(year,month,adv,datehourmin,mod1)
                                                rows = session.execute(SimpleStatement(bilAttributionQuery, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
                                                logging.info(bilAttributionQuery)
                                                for user_row in rows:
                                                        spendna = user_row.spend
							lineitemid = user_row.itemcolumbiaid
                                        if not eattribution:
                                                adtype = a.adlogtype
                                                bilAttributionQuery = "select spend,itemcolumbiaid from wls.ba{0}{1} where clientid = {2} and datehourmin = {3} and mod = {4}".format(year,month,adv,datehourmin,mod1)
                                                rows = session.execute(SimpleStatement(bilAttributionQuery, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
                                                logging.info(bilAttributionQuery)
                                                for user_row in rows:
                                                        spendna = user_row.spend
							lineitemid = user_row.itemcolumbiaid
                                                if adtype == 1:
                                                        countvc = countvc + 1
                                                        countcc = countcc
						elif adtype == 2 :
							countcc = countcc + 1
							countvc = countvc
				if not isAtributed:
                                        countccf = countccf + 1
                                checkingkey = str(lineitemid)+"|"+str(goalid)+"|"+str(datehourminvarkey)+"|"+str(adv)
				if checkingkey not in hourlyDict:
					hourlyDict[checkingkey] = (countvc,countcc,spendna,counteatrs,datehourmin,countccf)
				else:
					a = hourlyDict[checkingkey]
					countviewthrough = a[0]
                                        if countvc!=0:
                                                countviewthrough = a[0] + 1
                                        countattribution = a[3]
                                        if counteatrs!=0:
                                                countattribution = a[3] + 1
                                        countclickthrough = a[1]
                                        if countcc!=0:
						countclickthrough = a[1] + 1
					countfailure = a[5]
					if countccf!=0:
						countfailure = a[5] + 1
					if spendna!=0:
						spendna = a[2] + spendna
                                        hourlyDict[checkingkey] = (countviewthrough,countclickthrough,spendna,countattribution,datehourmin,countfailure)
	for h in hourlyDict:
		insertQuery = "insert into advconversionv2 (clientid,date,lineitemid,goalid,clickconv,earlyattributions,fail,imprconv,sg,succ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
		y = h.split('|')
		convstr = str(hourlyDict[h][4])[0:8]+" "+str(hourlyDict[h][4])[8:10]	
		logging.info(insertQuery)
		z = int(hourlyDict[h][0])+int(hourlyDict[h][1])+hourlyDict[h][3]
		hournew = str(hourlyDict[h][4])[0:10]
		convdate = datetime.datetime.strptime(convstr, '%Y%m%d %H').strftime('%Y-%m-%d %H:00:00')
		keyredisHourly = ""
		session2.execute(insertQuery,[int(adv),convdate,int(y[0]),int(y[1]),hourlyDict[h][1],hourlyDict[h][3],hourlyDict[h][5],hourlyDict[h][0],hourlyDict[h][2],z])




try :
	for adv in advclientids:
		datehourminlist = datehourminlistCreator(sys.argv[1],sys.argv[2],sys.argv[3]) 
	        threadalgoattribution = threading.Thread(target = algoAttributionDetails,kwargs={'year':sys.argv[1],'month':sys.argv[2],'datehourminlist':datehourminlist,'adv':adv})
	        threadalgoattribution.start()
except Exception as e:
	logging.error(e)
        print "Exception",e
	me = "asheesh.mahor@timesinternet.in"
	to = ["amit.singh2@timesinternet.in","asheesh.mahor@timesinternet.in","vijendra.dewda@timesinternet.in"]
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Exception in AdvConversion"
        msg['From'] = me
        msg['To'] = ", ".join(to)
	part = MIMEText(e, 'plain')
	msg.attach(part)
        s = smtplib.SMTP('192.168.24.21')
        s.sendmail(me,to, msg.as_string())
        logging.error(e)


