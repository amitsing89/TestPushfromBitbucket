import csv
from datetime import date, timedelta
import json
from datetime import datetime
from collections import OrderedDict
import servercredentials
import pytz
import sys
import simplejson
from urllib2 import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import datetime

#red = servercredentials.redisconnection()
#key1 = red.keys("PUB:*")
#pubclientids = []
#for i in key1:
#	s = i.split(":")
#	pubclientids.append(s[1])


pubclientids = servercredentials.publishersList()
advclientids = servercredentials.advertiserList()
advclientids = map(int,advclientids)
pubclientids = map(int,pubclientids)

lineitemlist = servercredentials.lineitemList(sys.argv[1])



def reportAsheeshSir():
	with open("/opt/amit_scripts_python/"+sys.argv[3]+"Report.csv", 'a') as csvfile:
		sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
		sitewriter.writerow(["Date","clientid","lineitemid","siteid","sectionid","click","conv","impr","sd","sg","spend"])
		for day in range(1,int(sys.argv[1])):
			query = "select * from advlineitemsectiondailyreportv2 where date = '{0}-{1}'".format(sys.argv[2],day)
			rows = servercredentials.cassandraProd(query)
			for a in rows:
				convertedDate = (a.date.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
				sitewriter.writerow([convertedDate,a.clientid,a.lineitemid,a.siteid,a.sectionid,a.click,a.conv,a.impr,a.sd,a.sg,a.spend])


#reportAsheeshSir()
			



def audienceReport():
	with open("/opt/amit_scripts_python/audreport3days.csv", 'a') as csvfile:
		#sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
		audids = []
		#sitewriter.writerow(["Audienceid"])
		lineitemlist = servercredentials.lineitemList(sys.argv[1])
		for lineitem in lineitemlist:
			for day in range(4,5):
				print int(sys.argv[1])
				#for hour in range(0,24):
				strs = "select * from audaggregationdaily where lineitemid={0} and date='2016-10-0{1}'".format(lineitem,day)
				rows = servercredentials.cassandraProd(strs)
				for a in rows:
					audids.append(a.audid)
		
		#lineitemlistnext = servercredentials.lineitemList(int(sys.argv[1])+1)
		#for lineitem in lineitemlistnext:
                        #for day in range(26,27):
				#print int(sys.argv[1])+1
                                #for hour in range(0,24):
                                        #strs = "select * from audaggregation where lineitemid={0} and date='2016-06-27 0{1}:00:00'".format(lineitem,hour)
                                        #rows = servercredentials.cassandraProd(strs)
                                        #for a in rows:
                                                #audids.append(a.audid)
		
		#lineitemlistnextagain = servercredentials.lineitemList(int(sys.argv[1])+2)
		#for lineitem in lineitemlistnextagain:
                        #for day in range(27,28):
				#print int(sys.argv[1])+2
                                #for hour in range(0,24):
                                        #strs = "select * from audaggregation where lineitemid={0} and date='2016-06-27 0{1}:00:00'".format(lineitem,hour)
                                        #rows = servercredentials.cassandraProd(strs)
                                        #for a in rows:
                                                #audids.append(a.audid)
		print set(audids)

audienceReport()


def reporting(lineitemlist):
	#with open("/opt/amit_scripts_python/audreportreport.csv", 'a') as csvfile:
		#sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
		#sitewriter.writerow(["Date","Lineitem","Adslot","Click","Impr","Spend"])
	totalclicks = 0
	for lineitem in lineitemlist:
		#totalclicks = 0
		for day in range(int(sys.argv[2]),int(sys.argv[3])):
			for hour in range(0,24):
				#totalclicks = 0 
				minute = 0
				while minute <60:
					strs = "select * from lineitempacingv2 where lineitemid={0} and date='2016-08-0{1} 0{2}:0{3}:00'".format(lineitem,day,hour,minute)
					rows = servercredentials.cassandraProd(strs)
					print strs
					minute = minute +15
					clicks = 0
					for a in rows:
						#convertedDate = (a.date.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
						#sitewriter.writerow([convertedDate,a.lineitemid,a.adslotdimid,a.click,a.impr,a.spend])
						#print a.audid
						clicks = clicks + a.click
					totalclicks = totalclicks+clicks
	print totalclicks

#reporting(lineitemlist)


#print pubclientids
#print advclientids

def reportingadvclicks():
	clicks = 0 
	for lineitem in lineitemlist:
		strs = "select * from {0} where lineitemid = {1} and date = '{2}'".format(sys.argv[2],lineitem,sys.argv[3])
		rows = servercredentials.cassandraProd(strs)
		for a in rows:
			clicks = clicks +a.click
	print clicks


#reportingadvclicks()


def reportingpclicks():
        clicks = 0
	if 'pub' in sys.argv[2]:
	        for pub in pubclientids:
	                strs = "select * from {0} where clientid = {1} and date = '{2}'".format(sys.argv[2],pub,sys.argv[3])
	                rows = servercredentials.cassandraProd(strs)
	                for a in rows:
	                        clicks = clicks +a.click
	elif 'adv' in sys.argv[2]:
		for adv in advclientids:
                	strs = "select * from {0} where clientid = {1} and date = '{2}'".format(sys.argv[2],adv,sys.argv[3])
	                rows = servercredentials.cassandraProd(strs)
	                for a in rows:
	                        clicks = clicks +a.click
	print clicks

#reportingclicks()






def clientReport(advclientids):
	with open("/opt/amit_scripts_python/"+sys.argv[3]+sys.argv[4]+".csv", 'a') as csvfile:
		sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
		sitewriter.writerow(["Date","Clientid","GeoId","Lineitemid","Click","Impr","Spend"])
		for adv in advclientids:
			for day in range(int(sys.argv[1]),int(sys.argv[2])):
				strs = "select * from {0} where clientid = {1} and date ='{2}-{3}'".format(sys.argv[3],adv,sys.argv[4],day)
				rows = servercredentials.cassandraProd(strs)
				for a in rows:
					convertedDate = (a.date.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
					#print convertedDate,a.clientid,a.siteid,a.geodimensionid,a.lineitemid,a.click,a.impr,a.spend
					#sectionid = a.sectionid
					#decodedsectionid = sectionid.encode('ascii', 'ignore').decode('ascii')
					sitewriter.writerow([convertedDate,a.clientid,a.geodimensionid,a.lineitemid,a.click,a.impr,a.spend])


#clientReport(advclientids)


def advMonthlyReport(advclientids):
	with open("/opt/amit_scripts_python/AdvReport"+sys.argv[2]+".csv", 'a') as csvfile:
		sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
	        sitewriter.writerow(["Date","Clientid","Siteid","GeoId","Lineitemid","Click","Impr","Spend"])
		for day in range(int(sys.argv[3]),int(sys.argv[4])):
		        for adv in advclientids:
				strs = "select * from advsectiondailyv2 where clientid = {0} and date = '{1}-{2}'".format(adv,sys.argv[1],day)
				#print strs
				rows = servercredentials.cassandraProd(strs)
				for a in rows:
					convertedDate = (a.date.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
					#sectionid = a.sectionid
					#decodedsectionid = sectionid.encode('ascii', 'ignore').decode('ascii')
					sitewriter.writerow([convertedDate,a.clientid,a.siteid,a.geodimensionid,a.lineitemid,a.click,a.impr,a.spend])

#advMonthlyReport(advclientids)


def pubHourlyReport(pubclientids):
	with open("/opt/amit_scripts_python/PubReport"+sys.argv[2]+".csv", 'a') as csvfile:
		sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
	        sitewriter.writerow(["Date","Clientid","Siteid","Sectionid","Click","Impr","Spend"])
		for pub in pubclientids:
			for day in range(int(sys.argv[3]),int(sys.argv[4])):
				strs = "select * from pubdashboardv2 where clientid = {0} and date = '{1}-{2}'".format(pub,sys.argv[1],day)
				rows = servercredentials.cassandraProd(strs)
				print strs
				for a in rows:
					convertedDate = (a.date.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
		                        sectionid = a.sectionid
		                        decodedsectionid = sectionid.encode('ascii', 'ignore').decode('ascii')
		                        sitewriter.writerow([convertedDate,a.clientid,a.siteid,decodedsectionid,a.click,a.impr,a.spend])


#pubHourlyReport(pubclientids)


def report(advclientids):
	for adv in advclientids:
		clicks  = 0
		strs = "select * from advlineitemsectiondailyv2 where clientid = {0} and date = '2016-10-02'".format(adv)
		rows = servercredentials.cassandraProd(strs)
		for a in rows:
			clicks = clicks+a.click
			#convertedDate = (a.date.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
			#sectionid = a.sectionid
			#decodedsectionid = sectionid.encode('ascii', 'ignore').decode('ascii')
			#sitewriter.writerow([convertedDate,a.clientid,a.siteid,decodedsectionid,a.click,a.impr,a.spend,a.sd])
		print adv,clicks

#report(advclientids)

def reportingadlog():
	with open("/opt/amit_scripts_python/BillingAttributionReport16330.csv", 'a') as csvfile:
		sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
		sitewriter.writerow(["Date","UserId","Earlyattribution","Goalid","Lineitemid","Spend"])
		for d in range(1,2):
			strs = "select * from billattribution where year =2016 and month = 06 and day ={0}".format(d)
			rows = servercredentials.cassandraProd(strs)
			for a in rows:
				if a.clientid==16330:
					convertedDate = (a.time.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
					#millis = a.createtime / 1000.0
			                #dayvar = datetime.datetime.fromtimestamp(millis).strftime('%Y-%m-%d %H:%M:%S')
					sitewriter.writerow([convertedDate,a.clmbuserid,a.earlyattribution,a.goalid,a.itemcolumbiaid,a.spend])

#reportingadlog()


def reportAsheeshSir():
	with open("/opt/amit_scripts_python/ClientReport2322.csv", 'a') as csvfile:
		sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
                #sitewriter.writerow(["Date","UserId","Clientid","Goalid","ImpressionId","Lineitemid","Itemid","Geoid"])
		for day in range(20,24):
			for hour in range(0,24):
				for minute in range(0,60):
					selectQuery = ''
					if hour<=9:
						if minute <=9:
							selectQuery = "SELECT * FROM adtracker201605 WHERE datehourmin =201605{0}0{1}0{2} and clientid = 2322".format(day,hour,minute)
						elif minute>9:
							selectQuery = "SELECT * FROM adtracker201605 WHERE datehourmin =201605{0}0{1}{2} and clientid = 2322".format(day,hour,minute)
					elif hour>9:
						if minute <=9:
							selectQuery = "SELECT * FROM adtracker201605 WHERE datehourmin =201605{0}{1}0{2} and clientid = 2322".format(day,hour,minute)
						if minute>9:
							selectQuery = "SELECT * FROM adtracker201605 WHERE datehourmin =201605{0}{1}{2} and clientid = 2322".format(day,hour,minute)
					rows = servercredentials.cassandraProdadlog(selectQuery)
					for a in rows:
						millis = a.createtime / 1000.0
	                                        dayvar = datetime.datetime.fromtimestamp(millis).strftime('%Y-%m-%d %H:%M:%S')
						#print (a.impressionid,a.lineitemid,a.itemid,a.geoid)
						#print selectQuery
	                                        sitewriter.writerow([dayvar,a.isattributed,a.earlyattribution,a.v,a.userid,a.clientid,a.goalid,a.impressionid,a.lineitemid,a.itemid,a.geoid,a.adlogtype])
						
			
#reportAsheeshSir()

def hiteshReport():
	with open("/opt/amit_scripts_python/MonthlyReport"+sys.argv[2]+".csv", 'a') as csvfile:
		sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
		sitewriter.writerow(["Date","Clientid","Siteid","Sectionid","Adslot","Click","Impr","Inorgclick","Orgclick","Pv","Spend"])
		for d in range(1,32):
			strs = "select * from pubadslotdailyv2 where clientid = 9782 and date = '{0}-0{1}'".format(sys.argv[1],d)
			rows = servercredentials.cassandraProd(strs)
			for a in rows:
				convertedDate = (a.date.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
                                sectionid = a.sectionid
                                decodedsectionid = sectionid.encode('ascii', 'ignore').decode('ascii')
                                sitewriter.writerow([convertedDate,a.clientid,a.siteid,decodedsectionid,a.adslotid,a.click,a.impr,a.inorgclick,a.orgclick,a.pv,a.spend])
		


#hiteshReport()

def pubReport(pubclientids):
	with open("/opt/amit_scripts_python/MonthlyReportApril.csv", 'a') as csvfile:
		sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
		sitewriter.writerow(["Date","Clientid","Siteid","Sectionid","Click","Impr","Inorgclick","Orgclick","Pv","Spend"])
		for pub in pubclientids:
			for d in range(1,31):
				#day = date.today() - timedelta(days=d)
				strs = " select * from pubdashboarddailyv2 where clientid = {0} and date='2016-04-0{1}'".format(pub,d)
				rows = servercredentials.cassandraProd(strs)
				print strs
				for a in rows:
					convertedDate = (a.date.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
					sectionid = a.sectionid
					decodedsectionid = sectionid.encode('ascii', 'ignore').decode('ascii')
					sitewriter.writerow([convertedDate,a.clientid,a.siteid,decodedsectionid,a.click,a.impr,a.inorgclick,a.orgclick,a.pv,a.spend])


#itemids = []




def advReports(advclientids):
        with open("/opt/amit_scripts_python/MonthlyReportApril.csv", 'a') as csvfile:
                sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
                sitewriter.writerow([])
		#click = 0
		for adv in advclientids:
			strs = "select * from advcontentdailyv2 where clientid = {0} and date = '2016-05-23'".format(adv)
			rows = servercredentials.cassandraProd(strs)
			#click = 0
			for a in rows:
				click = 0
				click = click + a.click
				if int(click)!=0:
					itemids = [a.itemid]
					getTitlefromSolr(itemids,a.click,a.impr)
			#click = 0

def getTitlefromSolr(itemids,click,impression):
	for itemid in itemids:
		url = "http://192.168.34.218:8080/solr/client_sku_shard1_replica3/select?q=*%3A*&fq=unique_sku_id%3A+{0}&wt=json&indent=true".format(itemid)
		connection = urlopen(url)
		response = simplejson.load(connection)
		jsonresponse = ([response[x] for x in response if 'response' in response])
		jsonlink = ([y['docs'] for y in jsonresponse if 'docs' in y])
		link = ([k['title'] for j in jsonlink for k in j])
		linkstr = str(link[0])
		#linkstr.encode('utf-8', "ignore")
		print impression,":",click,":",itemids[0],":",linkstr



#advReports(advclientids)
#getTitlefromSolr(itemids)

#clientReport(advclientids)
#report()
#advHourlyReport(advclientids)
#pubHourlyReport(pubclientids)
