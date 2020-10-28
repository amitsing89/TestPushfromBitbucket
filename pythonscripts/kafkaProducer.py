# pip install kafka-python
import datetime
import time
# from kafka import KafkaConsumer
from kafka import SimpleProducer, KafkaClient
import json

producerServer = 'localhost:9092'
producerTopic='AdServe'

consumerServer = 'localhost:9092'
consumerTopic='AdServe'
consumerGroup='AdServe-Streamer'

# Kafka Producer Config
kafkaProducerClient=KafkaClient(producerServer)
producer=SimpleProducer(kafkaProducerClient, async=True, req_acks=SimpleProducer.ACK_NOT_REQUIRED)

linesinfile=[]
f=open('/home/amit_scripts_python/IMP','r')
for line in f:
    linesinfile.append(line)

linesofnotify=[]
f = open('/home/amit_scripts_python/Notify','r')
for line in f:
    linesofnotify.append(line)


class AdClickLog(object):
    imprId="asdadasdasda"	
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
        self.imprId="asdadasdasda{0}".format(num)
        self.clmbUserId="iproeado"
        self.adSltDimId="234567"
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
    imprId="asdadasdasda"    
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
    ctxCatIds=[]
    meItmIds=[0]
    

    # The class "constructor" - It's actually an initializer 

    def __init__(self, itmClmbLId, crtd,num,imprid,adSltDimId,itmIds,algo,advclientidlist,pubClientId,geoDimId,siteId,visible,paidlist,pv):
        self.imprId=imprid
        self.spend=[0.0]
        self.cpa=[0.0]
        self.auds="au,er,rj"
        self.clmbUserId="iproeado"
        self.adSltDimId=adSltDimId
        self.position="au,er,rj"
        self.itmIds=itmIds
        self.algo=map(int,algo)
        self.itmClmbLIds=itmClmbLId
        self.tmpltId=321
        self.refUrl="google.com"
        self.geoDimId=geoDimId
        #self.clickBid=3
        self.ip="192.168.33.192"
        self.section="0"
        self.position="0"
        self.paid=map(int,paidlist)
        seconds_since_epoch=int(time.mktime(crtd.timetuple()) * 1000)
        self.crtd=seconds_since_epoch
        self.advClientIds=map(int,advclientidlist)
        self.pubClientId=pubClientId
        self.siteId=siteId
        self.pv=pv
        self.visible=visible
        self.expctdPayout   =0.1
        self.cityDimId="10316"
        self.osDimId="196063"
        self.devTypeDimId="196047"
        s="amit"
        self.ctxCatIds=[s]
        self.meItmIds=[0]
        
        def make_adlog(itmClmbLId, crtd):
            adlog=AdImprLog(itmClmbLId, crtd)        
            return adlog

class AdNotifyLog(object):
    imprId="asdadasdasda"
    adSltDimId="234567"
    pv=1
    crtd=None

    def __init__(self, crtd,num,imprid,adSltDimId,pv):
        self.imprId=imprid
        self.pv=pv
        self.adSltDimId=adSltDimId
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


def notifylog():
    b=0
    global num21
    global num22
    global num23
    global num24
    global num25
    while b<num25:
        #clickcid=11111
        #click=AdClickLog(clickcid,datetime.datetime.now(),b)
        #obj=AdLog(2,None,click,None)
        #objString=json.dumps(obj, default=lambda o: o.__dict__)
        #print objString
        #producer.send_messages(producerTopic, objString)
        imprid=str(linesofnotify[num21]).rstrip()
        adSltDimId=str(linesofnotify[num22]).rstrip()
        pv=int(linesofnotify[num23])
        notify=AdNotifyLog(datetime.datetime.now(),b,imprid,adSltDimId,pv)
        obj=AdLog(3,None,None,notify)
        objString=json.dumps(obj, default=lambda o: o.__dict__)
        print objString
        producer.send_messages(producerTopic, objString)
        num21=num21+3
        num22=num22+3
        num23=num23+3
        b=b+1
        if b%7==0:
            time.sleep(15*60)
            imprlog()
            



num1=0
num2=1
num3=2
num4=3
num5=4
num6=5
num7=6
num8=7
num9=8
num10=9
num11=10
num12=len(linesinfile)
num13=num12/11
num21=0
num22=1
num23=2
num24=len(linesofnotify)
num25=num24/3
def imprlog():
    global num1
    global num2
    global num3
    global num4
    global num5
    global num6
    global num7
    global num8
    global num9
    global num10
    global num11
    global num12
    global num13
    a=0
    while a<num13:
        imprid=str(linesinfile[num1]).rstrip()
        adSltDimId=str(linesinfile[num2]).rstrip()
        itmIds=[str(linesinfile[num3]).rstrip()]
        algo=linesinfile[num4].rstrip('\n').rstrip('\r').split(',')
        advclientidlist=linesinfile[num5].rstrip('\n').rstrip('\r').split(',')
        pubClientId=int(linesinfile[num6])
        geoDimId=str(linesinfile[num7]).rstrip()
        siteId=int(linesinfile[num8])
        visible=str(linesinfile[num9]).rstrip()
        paidlist=linesinfile[num10].rstrip('\n').rstrip('\r').split(',')
        pv=int(linesinfile[num11])
        cid=[11111]
        impr=AdImprLog(cid,datetime.datetime.now(),a,imprid,adSltDimId,itmIds,algo,advclientidlist,pubClientId,geoDimId,siteId,visible,paidlist,pv)
        obj=AdLog(1, impr, None, None)
        objString=json.dumps(obj, default=lambda o: o.__dict__)
        # objString=json.dump(obj, default=json_util.default)
        print objString
        producer.send_messages(producerTopic, objString)
        num1=num1+11
        num2=num2+11
        num3=num3+11
        num4=num4+11
        num5=num5+11
        num6=num6+11
        num7=num7+11
        num8=num8+11
        num9=num9+11
        num10=num10+11
        num11=num11+11
        a=a+1
        if a%20==0:
            notifylog()
            
    
imprlog()
