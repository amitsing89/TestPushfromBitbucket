from cassandra.cluster import Cluster
import redis
from datetime import date, timedelta
import json
from cassandra.cluster import Cluster
from datetime import datetime
from collections import OrderedDict
import csv

cluster = Cluster(contact_points=['localhost'], protocol_version=3)
session = cluster.connect()
session.set_keyspace('wls')
session.execute('USE wls')
totalclick=0
clientId = [16153,16153,16153,16153,16153,16153,8359,8163,2501,8163,2300,2299,10525,9669,8227,8505,13539,13540,13542,2308,2360,8504,8357,13120,8166,2308,14851,14852,14852,14852,12836,14854,14854,14926,14926,8358,10110,8360,14182,15679,14182,14553,14553,14553,16105,16128,15681]
siteids = [22,171,172,173,174,175,176,177,189,190,234,259,260,261,262,263,264,267,268,269,275,276,277,278,279,280,307,341,342,343,344,345,346,347,352,353,390,391,392,393,395,398,415,416,417,431,435,436]
with open("/home/amit_scripts_python/Report.csv", 'a') as csvfile:
	sitewriter = csv.writer(csvfile, delimiter=',',quotechar=',', quoting=csv.QUOTE_MINIMAL)
	sitewriter.writerow(["Date","ClientId","Siteid","Click","Impression","SectionId","InorganicClick","OrganicClick","Pv","Spend"])
	for i in clientId:
		for j in siteids:
			for x in range(1,32):
				strs = "select * from pubdashboarddailyv2 where clientId={0} and date='2016-03-0{1}' and siteid={2}".format(i,x,j)
				rows=session.execute(strs)
				for a in rows:
					sitewriter.writerow([a.date,i,j,a.click,a.impr,a.sectionid,a.inorgclick,a.orgclick,a.pv,a.spend])


