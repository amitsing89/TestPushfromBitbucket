import urllib
import json
import MySQLdb
import time
import datetime
import sys

database = MySQLdb.connect(host="localhost145", user="expresso145", passwd="expresso@145", db="til_expresso_db")
cur = database.cursor()
sqltab = """create table if not exists spark_executors (id varchar(255), diskUsed int, totalShuffleWrite varchar(255),totalInputBytes varchar(255),rddBlocks varchar(255),maxMemory varchar(255),totalShuffleRead varchar(255),totalTasks varchar(255),activeTasks varchar(255),failedTasks varchar(255),completedTasks varchar(255),hostPort varchar(255),memoryUsed varchar(255),totalDuration varchar(255), date varchar(255)) """
cur.execute(sqltab)
url = "http://localhost237:4040/api/v1/applications/RealTimeAggregatorV2/executors"


# url = sys.args[1]
# open_url = urllib.urlopen(url)
# jsondata = json.load(open_url)

def sparkDataInsertion():
    while True:
        try:
            open_url = urllib.urlopen(url)
        except:
            continue
        jsondata = json.load(open_url)
        convertedTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for data in jsondata:
            diskUsed = data['diskUsed']
            totalShuffleWrite = data['totalShuffleWrite']
            totalInputBytes = data['totalInputBytes']
            rddBlocks = data['rddBlocks']
            maxMemory = data['maxMemory']
            totalShuffleRead = data['totalShuffleRead']
            totalTasks = data['totalTasks']
            activeTasks = data['activeTasks']
            failedTasks = data['failedTasks']
            completedTasks = data['completedTasks']
            hostPort = data['hostPort']
            memoryUsed = data['memoryUsed']
            ids = data['id']
            totalDuration = data['totalDuration']
            convertedTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            insertQuery = "insert into spark_executors (id,diskUsed,totalShuffleWrite,totalInputBytes,rddBlocks,maxMemory,totalShuffleRead,totalTasks,activeTasks,failedTasks,completedTasks,hostPort,memoryUsed,totalDuration,date) values ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},\"{11}\",{12},{13},\"{14}\")".format(
                ids, diskUsed, str(totalShuffleWrite), str(totalInputBytes), str(rddBlocks), str(maxMemory),
                str(totalShuffleRead), str(totalTasks), str(activeTasks), str(failedTasks), str(completedTasks),
                str(hostPort), str(memoryUsed), str(totalDuration), str(convertedTime))
            try:
                cur.execute(insertQuery)
                database.commit()
            except:
                database.rollback()
        time.sleep(15 * 60)


try:
    sparkDataInsertion()
except Exception as e:
    print "Error Message-", e
