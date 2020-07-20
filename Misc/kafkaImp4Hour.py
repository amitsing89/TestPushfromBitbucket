# pip install kafka-python
import sys
import datetime
import time
# from kafka import KafkaConsumer
from kafka import SimpleProducer, KafkaClient
import json
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
import datetime
import json
import time
import redis
import csv
import calendar
import pytz
from mailer import Mailer
from mailer import Message
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email import Encoders
from email.mime.base import MIMEBase
consumer = KafkaConsumer('AdServe',group_id='my-group',bootstrap_servers=['192.169.34.61','192.169.34.62','192.169.34.63'])
cluster = Cluster(contact_points=['192.169.33.131'], protocol_version=3)
session = cluster.connect()
session.set_keyspace('wls')
session.execute('USE wls')
red = redis.Redis(host='192.169.33.163', db=0)
local_tz = pytz.timezone('Asia/Kolkata')
global myglobal
myglobal = []
producerServer='192.169.34.63:9092'
producerTopic='AdServe'

# Kafka Producer Config
kafkaProducerClient=KafkaClient(producerServer)
producer=SimpleProducer(kafkaProducerClient, async=True, req_acks=SimpleProducer.ACK_NOT_REQUIRED)

#Read file
#for each line:
    # push to kafka


class AdClickLog(object):
    imprId="impressionproduced"	
    clmbUserId="iproeado"
    adSltDimId="234567"
    auds="au,er,rj"
    itemid="2000"
    algo=5
    itmClmbLId=10000
    tmpltId=321
    refUrl="google.com"
    geoDimId="144"
    clickBid=3.0
    ip="192.168.33.192"
    section="0"
    position="0"
    paid=1
    crtd=None
    advClientId=2658
    pubClientId=2310
    siteId=1
    cityDimId="10316"
    osDimId="196061"
    devTypeDimId="196047"


    # The class "constructor" - It's actually an initializer 
    def __init__(self, itmClmbLId, crtd,num):
        self.imprId="impressionproduced{0}".format(num)
        self.clmbUserId="iproeado"
        self.adSltDimId="76767676"
        self.auds="au,er,rj"
        self.itemid="2000"
        self.algo=5
        self.itmClmbLId=itmClmbLId
        self.tmpltId=321
        self.refUrl="google.com"
        self.geoDimId="144"
        self.clickBid=3.0
        self.ip="192.168.33.192"
        self.section="0"
        self.position="0"
        self.paid=1
        seconds_since_epoch=int(time.mktime(crtd.timetuple()) * 1000)
        self.crtd=seconds_since_epoch
        self.advClientId=2658
        self.pubClientId=2310
        self.siteId=1 
        self.cityDimId="10316"
        self.osDimId="196061"
        self.devTypeDimId="196047"   

        def make_adlog(itmClmbLId, crtd):    
            adlog=AdClickLog(itmClmbLId, crtd)
            return adlog

class AdImprLog(object):
    imprId="impressionproduced"    
    clmbUserId="iproeado"
    adSltDimId="234567"
    auds="au,er,rj"
    itemid=[2000]
    algo=[5]
    itmClmbLIds=10000
    tmpltId=321
    refUrl="google.com"
    geoDimId=144
    clickBid=3
    ip="192.168.33.192"
    section="0"
    position="0"
    paid=[1]
    crtd=None
    advClientIds=[2658]
    pubClientId=2310
    siteId=1
    spend=[0.0]
    cpa=[0.0]
    pv=1
    visible=False
    expctdPayout=0.1
    cityDimId="10316"
    osDimId="196063"
    devTypeDimId="196047"
    ctxIds=[]
    meItmIds=[0]
    

    # The class "constructor" - It's actually an initializer 
    def __init__(self, itmClmbLId, crtd,num):
        #Value from line will be added to the required param of the kafkaproducer
        self.imprId="impressionproduced{0}".format(num)
        self.spend=[0.0]
        self.cpa=[0.0]
        self.auds="au,er,rj"
        self.clmbUserId="iproeado"
        self.adSltDimId="76767676"
        self.position="au,er,rj"
        self.itmIds=["2000"]
        self.algo=[5]
        self.itmClmbLIds=itmClmbLId
        self.tmpltId=321
        self.refUrl="google.com"
        self.geoDimId="144"
        #self.clickBid=3
        self.ip="192.168.33.192"
        self.section="0"
        self.position="0"
        self.paid=[1]
        seconds_since_epoch=int(time.mktime(crtd.timetuple()) * 1000)
        self.crtd=seconds_since_epoch
        self.advClientIds=[2658]
        self.pubClientId=2310
        self.siteId=1
        #self.pv=1
        self.visible=False
        self.expctdPayout   =0.1
        self.cityDimId="10316"
        self.osDimId="196063"
        self.devTypeDimId="196047"
        s="amit"
        self.ctxIds=[s]
        self.meItmIds=[0]

        def make_adlog(itmClmbLId, crtd):
            adlog=AdImprLog(itmClmbLId, crtd)        
            return adlog

class AdNotifyLog(object):
    imprId="impressionproduced"
    adSltDimId="234567"
    pv=1
    crtd=None

    def __init__(self, crtd,num):
        self.imprId="impressionproduced{0}".format(num)
        self.pv=1
        self.adSltDimId="76767676"
        seconds_since_epoch=int(time.mktime(crtd.timetuple()) * 1000)
        self.crtd=seconds_since_epoch

        def make_adlog(crtd):
            adlog=AdNotifyLog(crtd)
            return adlog

class AdLog(object):
    adLogType=0
    adImprLog=None
    adClickLog=None
    adNotifyLog=None

    # The class "constructor" - It's actually an initializer 
    def __init__(self, adLogType, adImprLog, adClickLog, adNotifyLog):
        self.adLogType=adLogType
        self.adImprLog=adImprLog
        self.adClickLog=adClickLog
        self.adNotifyLog=adNotifyLog

        def make_adlog(adLogType, adImprLog, adClickLog, adNotifyLog):
            adlog=AdLog(adImprLog, adClickLog, adNotifyLog)
            return adlog

num = int(sys.argv[1])
print sys.argv[1]
def clickandnotifylog(num):
    for b in range(0,num):
        clickcid=23232323
        click=AdClickLog(clickcid,datetime.now(),b)
        obj=AdLog(2,None,click,None)
        objString=json.dumps(obj, default=lambda o: o.__dict__)
        print objString
        producer.send_messages(producerTopic, objString)
        notify=AdNotifyLog(datetime.now(),b)
        obj=AdLog(3,None,None,notify)
        objString=json.dumps(obj, default=lambda o: o.__dict__)
        print objString
        producer.send_messages(producerTopic, objString)
        time.sleep(15*60)
for a in range(0,num):
    cid=[23232323]
    impr=AdImprLog(cid,datetime.now(),a)
    obj=AdLog(1, impr, None, None)
    objString=json.dumps(obj, default=lambda o: o.__dict__)
    # objString=json.dump(obj, default=json_util.default)
    print objString
    producer.send_messages(producerTopic, objString)

clickandnotifylog(num)
