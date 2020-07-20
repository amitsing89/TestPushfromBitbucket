import servercredentials
import sys
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import pytz
import time

sessionadlog = servercredentials.sessionReturn()
sessionadlog.set_keyspace('adlog')
sessionadlog.execute('use adlog')

red = servercredentials.redisconnection()


def clickBidCheck(year, month, dateVar):
    convDateVar = year + "-" + month + "-" + dateVar
    listpubcl = []
    listadvcl = []
    for hour in range(0, 24):
        for minutes in range(0, 60):
            query = "select * from adclicklog{0}{1}{2} WHERE date ='{3} {4}:{5}:00'".format(year, month, dateVar,
                                                                                            convDateVar, hour, minutes)
            rows = sessionadlog.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
            # print query
            for a in rows:
                listpubcl.append(a.pubclientid)
                listadvcl.append(a.advclientid)
                print a
            # if a.advclientid==int(sys.argv[4]):
            # if a.clickbid==0:
            # convertedDate = (a.time.replace(tzinfo = pytz.utc)).astimezone(pytz.timezone('Asia/Calcutta'))
            # print convertedDate
            # print a
    # print set(listpubcl)
    # print set(listadvcl)
    setpubcl = set(listpubcl)
    setadvcl = set(listadvcl)
    for i in setpubcl:
        strkey = "PUB:{0}".format(i)
        val = red.get(strkey)
        if val is None:
            print "Invalid Pub Clients--- :", i
    for j in setadvcl:
        strkey = "CL:{0}{1}{2}".format(sys.argv[1], sys.argv[2], sys.argv[3])
        val = red.hget(strkey, j)
        if val is None:
            print "Invalid Adv Clients--- :", j


clickBidCheck(sys.argv[1], sys.argv[2], sys.argv[3])
