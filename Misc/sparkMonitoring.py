#!/usr/bin/env python
import bs4, requests
import pymysql

pymysql.install_as_MySQLdb()
import MySQLdb
import time
import datetime

database = MySQLdb.connect(host="localhost", user="expresso145", passwd="expresso@145", db="til_expresso_db")
cur = database.cursor()
sql = """create table if not exists spark_batchdetails (clusterType varchar(30),lastBatchTime varchar(30) primary 
key, lastBatchSize varchar(30), lastProcessingDelay varchar(30), lastProcessingTime varchar(30)) """
cur.execute(sql)


def getSoup(url):
    response = requests.get(url, timeout=300)
    return bs4.BeautifulSoup(response.text, "html5lib")


def checkWorkers(workers):
    for w in workers:
        ip = w.select('td')[1].text.strip().split(':')[0]
        state = w.select('td')[2].text.strip()
        if not 'ALIVE' == state:
            deadExecutors.append(ip)
        allExecutors[ip] = allExecutors[ip] + 1
        print ip


def checkExecutors(executors):
    print "ip, failedTasks, inputSize"
    for i in executors:
        memoryUsed = i.select('td')[3].text.strip()
        idx = i.select('td')[0].text.strip()
        ip = i.select('td')[1].text.strip().split(':')[0]
        if not 'driver' == idx:
            failedTasks = i.select('td')[4].text.strip()
            inputSize = i.select('td')[6].text.strip()
            allExecutors[ip] = allExecutors[ip] + 1
            print idx, ip, inputSize, failedTasks
        else:
            allExecutors[ip] = allExecutors[ip] + 2
    # Subtracting 1 for driver
    totalExecutors = len(executors) - 1
    print "Executors:", totalExecutors


def checkMaster(master):
    print "checking for " + master
    print "----------------------"
    soup = getSoup("http://" + masterIps[master] + ":8080/")
    workers = soup.select('table.table')[0].select('tbody tr')
    checkWorkers(workers)
    soup = getSoup("http://" + masterIps[master] + ":4040/executors/")
    executors = soup.select('table.table tbody tr')
    checkExecutors(executors)
    soup = getSoup("http://" + masterIps[master] + ":4040/streaming")
    activeBatches = soup.select('table#active-batches-table tbody tr')
    completedBatches = soup.select('table#completed-batches-table tbody tr')
    activeBatchesCount = len(activeBatches)
    completedBatchesCount = len(completedBatches)
    print "Active", activeBatchesCount,
    print "Completed", completedBatchesCount
    lastCompleted = completedBatches[0]
    lastBatchTime = long(lastCompleted.select('td')[0].attrs.get('sorttable_customkey'))
    lastBatchSize = long(lastCompleted.select('td')[1].attrs.get('sorttable_customkey'))
    lastProcessingDelay = float(lastCompleted.select('td')[2].attrs.get('sorttable_customkey')) / 60000
    lastProcessingTime = float(lastCompleted.select('td')[3].attrs.get('sorttable_customkey')) / 60000
    print "lastBatchTime, lastBatchSize, lastProcessingDelay, lastProcessingTime"
    print lastBatchTime, lastBatchSize, lastProcessingDelay, lastProcessingTime
    convertedTime = datetime.datetime.fromtimestamp(lastBatchTime / 1000).strftime('%Y-%m-%d %H:%M:%S')
    if master == 'Budgeting':
        insertQuery = """insert into spark_batchdetails (clusterType,lastBatchTime, lastBatchSize, 
        lastProcessingDelay, lastProcessingTime) values (\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\") ON DUPLICATE KEY 
        UPDATE clusterType=values(clusterType),lastBatchTime=values(lastBatchTime), lastBatchSize=values(
        lastBatchSize), lastProcessingDelay=values(lastProcessingDelay), lastProcessingTime=values(
        lastProcessingTime) """
        dataInsertion = insertQuery.format(master, convertedTime, lastBatchSize, lastProcessingDelay * 60,
                                           lastProcessingTime * 60)
        print dataInsertion
        cur.execute(dataInsertion)
        database.commit()
    elif master == 'Dashboard':
        insertQuery = """insert into spark_batchdetails (clusterType,lastBatchTime, lastBatchSize, 
        lastProcessingDelay, lastProcessingTime) values (\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\") ON DUPLICATE KEY 
        UPDATE clusterType=values(clusterType),lastBatchTime=values(lastBatchTime), lastBatchSize=values(
        lastBatchSize), lastProcessingDelay=values(lastProcessingDelay), lastProcessingTime=values(
        lastProcessingTime) """
        dataInsertion = insertQuery.format(master, convertedTime, lastBatchSize, lastProcessingDelay,
                                           lastProcessingTime)
        print dataInsertion
        cur.execute(dataInsertion)
        database.commit()


masterIps = {"Dashboard": "localhost237", "Budgeting": "localhost100"}
allExecutors = {}
deadExecutors = []


def runJob():
    global allExecutors
    allExecutors = {'localhost80': 0, 'localhost81': 0, 'localhost89': 0,
                    'localhost95': 0, 'localhost96': 0, 'localhost105': 0,
                    'localhost121': 0, 'localhost130': 0, 'localhost137': 0,
                    'localhost237': 0, 'localhost248': 0, 'localhost252': 0,
                    'localhost96': 0, 'localhost101': 0, 'localhost102': 0, 'localhost103': 0,
                    'localhost104': 0, 'localhost100': 0}
    global deadExecutors
    deadExecutors = []
    for master in masterIps:
        print master
        checkMaster(master)
        print "--------------------------------------------------------------"
    print "allExecutors:", allExecutors
    print "deadExecutors:", deadExecutors


while True:
    try:
        runJob()
        time.sleep(15 * 60)
    except Exception as inst:
        print "Error", inst
