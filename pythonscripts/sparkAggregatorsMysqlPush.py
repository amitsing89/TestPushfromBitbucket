#!/usr/bin/env python
import bs4, requests
import MySQLdb
import time
import datetime
import urllib
import json
import sys

database = MySQLdb.connect(host="localhost", user="expresso145", passwd="expresso@145", db="til_expresso_db")
cur = database.cursor()
sql = """create table if not exists spark_tableau_data (clusterType varchar(30),lastBatchTime varchar(30), lastBatchSize varchar(30), lastProcessingDelay varchar(30),lastProcessingTime varchar(30),ids varchar(30), diskUsed int, totalShuffleWrite varchar(30),totalInputBytes varchar(30),rddBlocks varchar(30),maxMemory varchar(30),totalShuffleRead varchar(30),totalTasks varchar(30),activeTasks varchar(30),failedTasks varchar(30),completedTasks varchar(30),hostPort varchar(30),memoryUsed varchar(30),totalDuration varchar(30), primary key (clusterType,lastBatchTime,hostPort)) """
url = "http://localhost:4040/api/v1/applications/RealTimeAggregatorV2/executors"
urlBudgeting = "http://localhost100:4040/api/v1/applications/RealTimeAggregatorV2/executors"
cur.execute(sql)


def getSoup(url):
    response = requests.get(url)
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
    open_url = urllib.urlopen(url)
    jsondata = json.load(open_url)
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
        hostPort = str(data['hostPort'])
        memoryUsed = data['memoryUsed']
        ids = data['id']
        totalDuration = data['totalDuration']
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
        for batches in completedBatches:
            print "Active", activeBatchesCount,
            print "Completed", completedBatchesCount
            lastCompleted = batches
            lastBatchTime = long(lastCompleted.select('td')[0].attrs.get('sorttable_customkey'))
            lastBatchSize = long(lastCompleted.select('td')[1].attrs.get('sorttable_customkey'))
            lastProcessingDelay = float(lastCompleted.select('td')[2].attrs.get('sorttable_customkey')) / 60000
            lastProcessingTime = float(lastCompleted.select('td')[3].attrs.get('sorttable_customkey')) / 60000
            print "lastBatchTime, lastBatchSize, lastProcessingDelay, lastProcessingTime"
            print lastBatchTime, lastBatchSize, lastProcessingDelay, lastProcessingTime
            convertedTime = datetime.datetime.fromtimestamp(lastBatchTime / 1000).strftime('%Y-%m-%d %H:%M:%S')
            # print convertedTime
            # print hostPort
            if 'Dashboard' in master and (
                                                                'localhost80' in hostPort or 'localhost81' in hostPort or 'localhost89' in hostPort or 'localhost95' in hostPort or 'localhost96' in hostPort or 'localhost105' in hostPort or 'localhost121' in hostPort or 'localhost130' in hostPort or 'localhost137' in hostPort or 'localhost' in hostPort or 'localhost248' in hostPort or 'localhost252' in hostPort or 'localhost96 ' in hostPort):
                if 'localhost' in hostPort:
                    insertQuery = """insert into spark_tableau_data(clusterType,lastBatchTime,lastBatchSize, lastProcessingDelay,lastProcessingTime,ids, diskUsed,totalShuffleWrite,totalInputBytes,rddBlocks,maxMemory,totalShuffleRead,totalTasks,activeTasks,failedTasks,completedTasks,hostPort,memoryUsed,totalDuration) values(\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\"{9}\",\"{10}\",\"{11}\",\"{12}\",\"{13}\",\"{14}\",\"{15}\",\"{16}\",\"{17}\",\"{18}\") ON DUPLICATE KEY UPDATE clusterType=values(clusterType),lastBatchTime=values(lastBatchTime),lastBatchSize=values(lastBatchSize), lastProcessingDelay=values(lastProcessingDelay),lastProcessingTime=values(lastProcessingTime),ids=values(ids),diskUsed=values(diskUsed),totalShuffleWrite=values(totalShuffleWrite),totalInputBytes=values(totalInputBytes),rddBlocks=values(rddBlocks),maxMemory=values(maxMemory),totalShuffleRead=values(totalShuffleRead),totalTasks=values(totalTasks),activeTasks=values(activeTasks),failedTasks=values(failedTasks),completedTasks=values(completedTasks),hostPort=values(hostPort),memoryUsed=values(memoryUsed),totalDuration=values(totalDuration)"""
                    dataInsertionDashboard = insertQuery.format(master, convertedTime, lastBatchSize,
                                                                lastProcessingDelay, lastProcessingTime, ids, diskUsed,
                                                                totalShuffleWrite, totalInputBytes, rddBlocks,
                                                                maxMemory, totalShuffleRead, totalTasks, activeTasks,
                                                                failedTasks, completedTasks, hostPort, memoryUsed,
                                                                totalDuration)
                    # print dataInsertionDashboard
                    # print master
                    cur.execute(dataInsertionDashboard)
                    database.commit()
                else:
                    insertQuery = """insert into spark_tableau_data(clusterType,lastBatchTime,lastBatchSize, lastProcessingDelay,lastProcessingTime,ids, diskUsed,totalShuffleWrite,totalInputBytes,rddBlocks,maxMemory,totalShuffleRead,totalTasks,activeTasks,failedTasks,completedTasks,hostPort,memoryUsed,totalDuration) values(\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\"{9}\",\"{10}\",\"{11}\",\"{12}\",\"{13}\",\"{14}\",\"{15}\",\"{16}\",\"{17}\",\"{18}\") ON DUPLICATE KEY UPDATE clusterType=values(clusterType),lastBatchTime=values(lastBatchTime),lastBatchSize=values(lastBatchSize), lastProcessingDelay=values(lastProcessingDelay),lastProcessingTime=values(lastProcessingTime),ids=values(ids),diskUsed=values(diskUsed),totalShuffleWrite=values(totalShuffleWrite),totalInputBytes=values(totalInputBytes),rddBlocks=values(rddBlocks),maxMemory=values(maxMemory),totalShuffleRead=values(totalShuffleRead),totalTasks=values(totalTasks),activeTasks=values(activeTasks),failedTasks=values(failedTasks),completedTasks=values(completedTasks),hostPort=values(hostPort),memoryUsed=values(memoryUsed),totalDuration=values(totalDuration)"""
                    dataInsertionDashboard = insertQuery.format(master, convertedTime, 0, lastProcessingDelay,
                                                                lastProcessingTime, ids, diskUsed, totalShuffleWrite,
                                                                totalInputBytes, rddBlocks, maxMemory, totalShuffleRead,
                                                                totalTasks, activeTasks, failedTasks, completedTasks,
                                                                hostPort, memoryUsed, totalDuration)
                    # print dataInsertionDashboard
                    # print masteri
                    cur.execute(dataInsertionDashboard)
                    database.commit()


def checkMasterBudgeting(master):
    open_url = urllib.urlopen(urlBudgeting)
    jsondata = json.load(open_url)
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
        hostPort = str(data['hostPort'])
        memoryUsed = data['memoryUsed']
        ids = data['id']
        totalDuration = data['totalDuration']
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
        for batches in completedBatches:
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
            # print convertedTime
            # print hostPort
            if 'Budgeting' in master and (
                                'localhost100' in hostPort or 'localhost101' in hostPort or 'localhost102' in hostPort or 'localhost103' in hostPort or 'localhost104' in hostPort):
                if 'localhost100' in hostPort:
                    # print convertedTime
                    lastProcessTime = lastProcessingTime * 60
                    lastProcessDelay = lastProcessingDelay * 60
                    insertQuery = """insert into spark_tableau_data(clusterType,lastBatchTime,lastBatchSize, lastProcessingDelay,lastProcessingTime,ids, diskUsed,totalShuffleWrite,totalInputBytes,rddBlocks,maxMemory,totalShuffleRead,totalTasks,activeTasks,failedTasks,completedTasks,hostPort,memoryUsed,totalDuration) values(\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\"{9}\",\"{10}\",\"{11}\",\"{12}\",\"{13}\",\"{14}\",\"{15}\",\"{16}\",\"{17}\",\"{18}\") ON DUPLICATE KEY UPDATE clusterType=values(clusterType),lastBatchTime=values(lastBatchTime),lastBatchSize=values(lastBatchSize), lastProcessingDelay=values(lastProcessingDelay),lastProcessingTime=values(lastProcessingTime),ids=values(ids),diskUsed=values(diskUsed),totalShuffleWrite=values(totalShuffleWrite),totalInputBytes=values(totalInputBytes),rddBlocks=values(rddBlocks),maxMemory=values(maxMemory),totalShuffleRead=values(totalShuffleRead),totalTasks=values(totalTasks),activeTasks=values(activeTasks),failedTasks=values(failedTasks),completedTasks=values(completedTasks),hostPort=values(hostPort),memoryUsed=values(memoryUsed),totalDuration=values(totalDuration)"""
                    dataInsertionBudgeting = insertQuery.format(master, convertedTime, lastBatchSize,
                                                                lastProcessingDelay, lastProcessingTime, ids, diskUsed,
                                                                totalShuffleWrite, totalInputBytes, rddBlocks,
                                                                maxMemory, totalShuffleRead, totalTasks, activeTasks,
                                                                failedTasks, completedTasks, hostPort, memoryUsed,
                                                                totalDuration)
                    # print dataInsertionBudgeting
                    # print master
                    cur.execute(dataInsertionBudgeting)
                    database.commit()
                else:
                    # print convertedTime
                    lastProcessTime = lastProcessingTime * 60
                    lastProcessDelay = lastProcessingDelay * 60
                    insertQuery = """insert into spark_tableau_data(clusterType,lastBatchTime,lastBatchSize, lastProcessingDelay,lastProcessingTime,ids, diskUsed,totalShuffleWrite,totalInputBytes,rddBlocks,maxMemory,totalShuffleRead,totalTasks,activeTasks,failedTasks,completedTasks,hostPort,memoryUsed,totalDuration) values(\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\"{9}\",\"{10}\",\"{11}\",\"{12}\",\"{13}\",\"{14}\",\"{15}\",\"{16}\",\"{17}\",\"{18}\") ON DUPLICATE KEY UPDATE clusterType=values(clusterType),lastBatchTime=values(lastBatchTime),lastBatchSize=values(lastBatchSize), lastProcessingDelay=values(lastProcessingDelay),lastProcessingTime=values(lastProcessingTime),ids=values(ids),diskUsed=values(diskUsed),totalShuffleWrite=values(totalShuffleWrite),totalInputBytes=values(totalInputBytes),rddBlocks=values(rddBlocks),maxMemory=values(maxMemory),totalShuffleRead=values(totalShuffleRead),totalTasks=values(totalTasks),activeTasks=values(activeTasks),failedTasks=values(failedTasks),completedTasks=values(completedTasks),hostPort=values(hostPort),memoryUsed=values(memoryUsed),totalDuration=values(totalDuration)"""
                    dataInsertionBudgeting = insertQuery.format(master, convertedTime, '0', lastProcessingDelay,
                                                                lastProcessingTime, ids, diskUsed, totalShuffleWrite,
                                                                totalInputBytes, rddBlocks, maxMemory, totalShuffleRead,
                                                                totalTasks, activeTasks, failedTasks, completedTasks,
                                                                hostPort, memoryUsed, totalDuration)
                    # print dataInsertionBudgeting
                    # print master
                    cur.execute(dataInsertionBudgeting)
                    database.commit()


masterIps = {"Dashboard": "localhost", "Budgeting": "localhost100"}
allExecutors = {}
deadExecutors = []


def runJob():
    global allExecutors
    allExecutors = {'localhost80': 0, 'localhost81': 0, 'localhost89': 0,
                    'localhost95': 0, 'localhost96': 0, 'localhost105': 0,
                    'localhost121': 0, 'localhost130': 0, 'localhost137': 0,
                    'localhost': 0, 'localhost248': 0, 'localhost252': 0,
                    'localhost96': 0, 'localhost101': 0, 'localhost102': 0, 'localhost103': 0,
                    'localhost104': 0, 'localhost100': 0}
    global deadExecutors
    deadExecutors = []
    for master in masterIps:
        print master
        checkMaster(master)
        checkMasterBudgeting(master)
        print "--------------------------------------------------------------"
    print "allExecutors:", allExecutors
    print "deadExecutors:", deadExecutors


while True:
    try:
        runJob()
        time.sleep(15 * 60)
    except Exception as e:
        print "Exception Thrown -", e
