import urllib, json
import time
import datetime


def urlTesting(limit):
    url = "http://qa.ade.clmbtech.com/cde/data/v4.htm?adUnitId=129103~1~0&pv=1&auds=all&_u=http%3A//timesofindia.indiatimes.com/international-home&_t=2&_c=jsonCallback&exc=null&_v=0"
    imprId = []
    clickUrl = []
    notifyUrl = []
    pubClientid = []
    crtd = []
    lid = []
    for i in range(0, limit):
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        print "DATA",data
        for j in data:
            print "J",j
            imprId.append(j['imprId'])
            notifyUrl.append(j['imprUrl'])
            print j
            if "oItems" in j:
                imprItems = (j['items'])
                organicItems = (j['oItems'])
                for k in imprItems:
                    clickUrl.append(k['url'])
                for l in organicItems:
                    clickUrl.append(l['url'])
            else:
                clickUrl.append(j['items'][0]['url'])
                lid.append(j['items'][0]['lId'])
        print lid
        print "Url Opened for the Time:-", i

    a = 0
    while a < len(clickUrl):
        print "while started for time", a
        r = urllib.urlopen(clickUrl[a])
        re = urllib.urlopen(notifyUrl[a])
        print clickUrl[a]
        print notifyUrl[a]
        print "Url Open for the time    ", a
        print "Click count increased for the time", a
        time.sleep(15 * 60)
        a = a + 1


inp = input('Enter A Number for Opening URL for total time \n')
urlTesting(inp)
