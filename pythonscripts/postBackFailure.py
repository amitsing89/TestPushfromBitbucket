import sys
from mailer import Mailer
import csv
from mailer import Message
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import servercredentials
import logging
import pytz
import threading
from threading import Thread
from thread import start_new_thread
import time
from multiprocessing import Process, Manager
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel


#Log Info
logging.basicConfig(filename='/opt/amit_scripts_python/log/postbackfailure.log',level=logging.DEBUG)

#Redis Server Connectivity
red = servercredentials.redisconnection()
session = servercredentials.sessionReturn()

#Cassandra Server Connectivity
sessionadlog = servercredentials.sessionReturn()
sessionadlog.set_keyspace('adlog')
sessionadlog.execute('use adlog')


#Csv File Location
f2 = open('/var/www/html/conversion/attribution'+sys.argv[1]+sys.argv[2]+sys.argv[3]+".csv",'w')
csv_file2 = csv.writer(f2)
csv_file2.writerow(["AdType","Clientid","AdlogType","Userid","GeoId","GoalId","ItemId","jsts","Lineitemid","V","EarlyAttribution","IsAttributed","Datehourmin","Mod","CreatedTime"])


#Csv File Location
f1 = open('/var/www/html/conversion/bilattribution'+sys.argv[1]+sys.argv[2]+sys.argv[3]+".csv",'w')
csv_file = csv.writer(f1)
csv_file.writerow(["Date","AdvertiserId","PublisherId","LineitemId","ItemId","AdslotId","GoalId","SiteId","SectionId","GeoId","Spend","EarlyAttribution","DatehourMin","Mod","UserId"])



#DateHourMin Variable Creation
def datehourminlistCreator(year,month,day):
  datehourminlist = []
  for hour in range(0,24):
    for minute in range(0,60):
      datehourminvar = str(year)+str(month)+str(day)
      if hour<=9:
        if minute <=9:
          #datehourminvar = datehourminvar+str(0)+str(hour)+str(0)+str(minute)
          datehourminlist.append(datehourminvar+str(0)+str(hour)+str(0)+str(minute))
        elif minute>9:
          #datehourminvar = datehourminvar+str(0)+str(hour)+str(minute)
          datehourminlist.append(datehourminvar+str(0)+str(hour)+str(minute))
      elif hour>9:
        if minute <=9:
          #datehourminvar = datehourminvar+str(hour)+str(0)+str(minute)
          datehourminlist.append(datehourminvar+str(hour)+str(0)+str(minute))
        elif minute>9:
          #datehourminvar = datehourminvar+str(hour)+str(minute)
          datehourminlist.append(datehourminvar+str(hour)+str(minute))
  return datehourminlist



#Method For Creating the csv file of cassandra data from table baYYYYMM
def algoBilAttributionDetails(year,month,datehourminlist,adv):
	mod = []
	for num in range(0,1000):
		mod.append(int(num))
	for datehourminvar in datehourminlist:
		query = "select * from wls.ba{0}{1} where clientid = {2} and datehourmin = {3} and mod in {4}".format(year,month,adv,int(datehourminvar),tuple(mod))
		logval = "select * from wls.ba{0}{1} where clientid = {2} and datehourmin = {3}".format(year,month,adv,int(datehourminvar))
		logging.info(logval)
		rows = session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
		for a in rows:
			convertedDate = (a.time.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
			csv_file.writerow([convertedDate,a.clientid,a.pubclientid,a.itemcolumbiaid,a.itemid,a.adslotdimid,a.goalid,a.siteid,a.section,a.geodimid,a.spend,a.earlyattribution,a.datehourmin,a.mod,a.clmbuserid])



#Method For Creating the csv file of cassandra data from table AdtrackerYYYYMM
def algoAttributionDetails(year,month,datehourminlist,adv):
	mod = []
	for num in range(0,1000):
		mod.append(int(num))
	for datehourminvar in datehourminlist:
		query = "select * from adtracker{0}{1} where clientid = {2} and datehourmin = {3} and mod in {4}".format(year,month,adv,int(datehourminvar),tuple(mod))
		logval = "select * from adtracker{0}{1} where clientid = {2} and datehourmin = {3}".format(year,month,adv,int(datehourminvar))
		logging.info(logval)
		rows = sessionadlog.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
		for user_row in rows:
			jobStatus = user_row.jsts
			isAtributed = user_row.isattributed
			requestMap = user_row.reqmap
			osDetails = ""
			for data in requestMap:
				if 'os' in data:
					osDetails = requestMap[data]
			if 'UnKnown, More-Info: Apsalar-Postback' in osDetails:
				if isAtributed is True:
					csv_file2.writerow(["Apsalar True",user_row.clientid,user_row.adlogtype,user_row.userid,user_row.geoid,user_row.goalid,user_row.itemid,user_row.jsts,user_row.lineitemid,user_row.v,user_row.earlyattribution,user_row.isattributed,user_row.datehourmin,user_row.mod,user_row.createtime])
				elif isAtributed is not True:
					csv_file2.writerow(["Apsalar Failure",user_row.clientid,user_row.adlogtype,user_row.userid,user_row.geoid,user_row.goalid,user_row.itemid,user_row.jsts,user_row.lineitemid,user_row.v,user_row.earlyattribution,user_row.isattributed,user_row.datehourmin,user_row.mod,user_row.createtime])
			elif 'UnKnown, More-Info: HasOffers Mobile AppTracking v1.0' in osDetails:
				if isAtributed is True:
					csv_file2.writerow(["Mat True",user_row.clientid,user_row.adlogtype,user_row.userid,user_row.geoid,user_row.goalid,user_row.itemid,user_row.jsts,user_row.lineitemid,user_row.v,user_row.earlyattribution,user_row.isattributed,user_row.datehourmin,user_row.mod,user_row.createtime])
				elif isAtributed is not True:
					csv_file2.writerow(["Mat Failure",user_row.clientid,user_row.adlogtype,user_row.userid,user_row.geoid,user_row.goalid,user_row.itemid,user_row.jsts,user_row.lineitemid,user_row.v,user_row.earlyattribution,user_row.isattributed,user_row.datehourmin,user_row.mod,user_row.createtime])
			else:
				if isAtributed is True:
					csv_file2.writerow(["Web True",user_row.clientid,user_row.adlogtype,user_row.userid,user_row.geoid,user_row.goalid,user_row.itemid,user_row.jsts,user_row.lineitemid,user_row.v,user_row.earlyattribution,user_row.isattributed,user_row.datehourmin,user_row.mod,user_row.createtime])
				elif isAtributed is not True:
					csv_file2.writerow(["Web Failure",user_row.clientid,user_row.adlogtype,user_row.userid,user_row.geoid,user_row.goalid,user_row.itemid,user_row.jsts,user_row.lineitemid,user_row.v,user_row.earlyattribution,user_row.isattributed,user_row.datehourmin,user_row.mod,user_row.createtime])



#Gathering the Client list for the day of conversion
key = "CPGC:{0}{1}{2}".format(sys.argv[1],sys.argv[2],sys.argv[3])	
advclientids = red.smembers(key)



#Thread for Adtracker Table		
try :
	for adv in advclientids:
		datehourminlist = datehourminlistCreator(sys.argv[1],sys.argv[2],sys.argv[3])
		threadalgoattribution = threading.Thread(target = algoAttributionDetails,kwargs={'year':sys.argv[1],'month':sys.argv[2],'datehourminlist':datehourminlist,'adv':adv})
		threadalgoattribution.start()
except Exception as e:
       logging.error(e)


#Thread for Bilattribution Table
try:
	for adv in advclientids:
		datehourminlist = datehourminlistCreator(sys.argv[1],sys.argv[2],sys.argv[3])
		threadalgoBilAttributionDetails = threading.Thread(target = algoBilAttributionDetails,kwargs={'year':sys.argv[1],'month':sys.argv[2],'datehourminlist':datehourminlist,'adv':adv})
		threadalgoBilAttributionDetails.start()
except Exception as e:
	me = "amit.singh2@timesinternet.in"
	to = ["amit.singh2@timesinternet.in"]
	#to = ["asheesh.mahor@timesinternet.in","saurabh.chandolia@timesinternet.in","amit.singh2@timesinternet.in","kundan.kumar1@timesinternet.in","gaurav.sharma8@timesinternet.in","mohit.kundra@timesinternet.in","varun.arora1@timesinternet.in","vijendra.dewda@timesinternet.in","mankeshwar.kumar@timesinternet.in"]
	msg = MIMEMultipart('alternative')
	msg['Subject'] = "Exception in Postbackfailure"
	msg['From'] = me
	msg['To'] = ", ".join(to)
	part = MIMEText(e, 'plain')
	msg.attach(part)
	s = smtplib.SMTP('localhost')
	s.sendmail(me,to, msg.as_string())
	logging.error(e)

