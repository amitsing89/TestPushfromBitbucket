import smtplib
import sys
import os
import datetime
from datetime import date, timedelta,time
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import Encoders
import zipfile

#HOURS_FLAG=int(sys.argv[1])

def send_mail(send_from, send_to, subject, text, files=[], server="localhost"):
  assert type(send_to)==list
  assert type(files)==list
  msg = MIMEMultipart()
  msg['From'] = send_from
  msg['To'] = COMMASPACE.join(send_to)
  msg['Date'] = formatdate(localtime=True)
  msg['Subject'] = subject
  msg.attach( MIMEText(text) )
  for f in files:
    print f
    part = MIMEBase('application', 'zip')
    part.set_payload( open(f,"rb").read() )
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
    msg.attach(part)
  smtp = smtplib.SMTP(server)
  smtp.sendmail(send_from, send_to, msg.as_string())
  smtp.close()


fileList = ['/opt/amit_scripts_python/MonthlyReportJanuary2016.csv']

#print fileList
#send_mail("adtech.production@timesinternet.in",['mohit.kundra@timesinternet.in'], "DATA SANITY", "Mismatch Data Only", files=['/tmp/DailyData/SiteIDMatch2016-01-27_0.csv','/tmp/DailyData/RedisLineItemId2016-01-27_0.csv','/tmp/DailyData/DailyAdvertiser2016-01-27_0.csv','/tmp/DailyData/DailyPublisher2016-01-27_0.csv'], server="192.168.24.21")

#send_mail("adtech.production@timesinternet.in",['mohit.kundra@timesinternet.in','vishal.arora@timesinternet.in','kundan.kumar1@timesinternet.in','gaurav.sharma8@timesinternet.in','saurabh.chandolia@timesinternet.in','asheesh.mahor@timesinternet.in','anil.dobhal@timesinternet.in','amit.singh2@timesinternet.in','akanksha.govil1@timesinternet.in'], "Last Hour Data Sanity Reports", "Mismatch Data Only",fileList, server="192.168.24.21")

#send_mail("adtech.production@timesinternet.in",['mohit.kundra@timesinternet.in','vishal.arora@timesinternet.in','gaurav.sharma8@timesinternet.in','saurabh.chandolia@timesinternet.in','anil.dobhal@timesinternet.in','amit.singh2@timesinternet.in','akanksha.govil1@timesinternet.in'], "Last Hour Data Sanity Reports", "Mismatch Data Only",fileList, server="192.168.24.21")

send_mail("amit.singh2@timesinternet.in",['bhupesh.karankar@timesinternet.in'], "Data Report", "Data",fileList, server="192.168.24.21")

#send_mail("amit.singh2@timesinternet.in",['saurabh.chandolia@timesinternet.in','debarpita.das@timesinternet.in','ankur.saxena1@timesinternet.in','amit.singh2@timesinternet.in','mohit.kundra@timesinternet.in'], "Data Report", "Data",fileList, server="192.168.24.21")



