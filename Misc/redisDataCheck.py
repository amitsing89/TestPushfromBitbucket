from mailer import Mailer
from mailer import Message
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#import servercredentials
import datetime
import time
from datetime import datetime
import sys
import redis
import json
import logging

red = redis.Redis(host = 'localhost157', db = 0)


logging.basicConfig(filename='/opt/amit_scripts_python/log/redisDataCheck.log',level=logging.DEBUG)

me = "columbia.data@timesinternet.in"
#to = "amit.singh2@timesinternet.in"
to = ["asheesh.mahor@timesinternet.in","amit.singh2@timesinternet.in","kundan.kumar1@timesinternet.in","gaurav.sharma8@timesinternet.in","mohit.kundra@timesinternet.in"]
msg = MIMEMultipart('alternative')
msg['Subject'] = "Difference in Redis Data"
msg['From'] = me
msg['To'] = ", ".join(to)
msg1 = MIMEMultipart('alternative')
msg1['Subject'] = "Difference in Redis Data"
msg1['From'] = me
msg1['To'] = ", ".join(to)



def redisDataCheck(day):
	datevar = datetime.strptime(day, "%Y%m%d").date()
	seconds_since_epoch = int(time.mktime(datevar.timetuple()) * 1000)
	#print seconds_since_epoch
	#LIS is for one minute job (Budgeting Pacing)
	#RLIS is for fifteen(15) minute job (Dashboard)
	#246618 is the lineitem of taboola
	keylis = "LIS:246618"
	keyrlis = "RLIS:246618"
	datalis = json.loads(red.hget(keylis,seconds_since_epoch))
	datarlis = json.loads(red.hget(keyrlis,seconds_since_epoch))
	logging.info(datalis)
	logging.info(datarlis)
	imprrlis = datarlis['impr']
	imprlis = datalis['impr']
	diffimpr = imprlis-imprrlis
	if imprlis!=0:
		percentdiffimpr = float(diffimpr*100/imprlis)
		logging.info(percentdiffimpr)
		if percentdiffimpr>=10:
			text = "Difference in LIS and RLIS greater than 10%\n"
			concstr = "LIS-Impression-{0}\nRLIS-Impression-{1}\nImpression-Difference-{2}".format(str(imprlis),str(imprrlis),str(diffimpr))
			text = text + concstr
			part1 = MIMEText(text,'plain')
			msg.attach(part1)
			logging.info(part1)
			s = smtplib.SMTP('localhost21')
			s.sendmail(me,to, msg.as_string())
	else:
		text2 = "Difference in LIS and RLIS greater than 10%\n"
		text2 = text2 + "LIS-Impr"+str(imprlis) +"\nRLIS-Impr"+ str(imprrlis) 
                part13 = MIMEText(text2,'plain')
                msg.attach(part13)
                s = smtplib.SMTP('localhost21')
                s.sendmail(me,to, msg.as_string())
	clicklis = datalis['click']
	clickrlis = datarlis['click']
	clickData(clicklis,clickrlis)



def clickData(clicklis,clickrlis):
	diffclick = clicklis-clickrlis
        if clicklis!=0:
		#print float(diffclick*100/clicklis)
                percentdiffclick = float(diffclick*100/clicklis)
                logging.info(percentdiffclick)
                if percentdiffclick>=10:
                        text1 = "Difference in LIS and RLIS greater than 10%\n"
                        concstr = "LIS-Click-{0}\nRLIS-Click-{1}\nClick-Difference-{2}".format(str(clicklis),str(clickrlis),str(diffclick))
                        text1 = text1 + concstr
                        part2 = MIMEText(text1,'plain')
			#print part2
                        msg1.attach(part2)
                        logging.info(part2)
                        s = smtplib.SMTP('localhost21')
                        s.sendmail(me,to, msg1.as_string())
        else:
                text3 = "Difference in LIS and RLIS greater than 10%\n"
                text3 = text3 + "LIS-CLICK"+str(clicklis) +"\nRLIS-Click"+ str(clickrlis)
                part3 = MIMEText(text3,'plain')
                msg1.attach(part3)
                s = smtplib.SMTP('localhost21')
                s.sendmail(me,to, msg1.as_string())



try:
	redisDataCheck(sys.argv[1])
except Exception as e:
	logging.info(e)

