import sys
from mailer import Mailer
import csv
from mailer import Message
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import servercredentials
import logging
import pytz
import threading
from threading import Thread
from thread import start_new_thread
import time
from multiprocessing import Process, Manager
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import sys
import redis
import json
import datetime
from datetime import date, timedelta
import sys
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from mailer import Mailer
from mailer import Message
import smtplib
import MySQLdb
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import servercredentials
import logging
from tabulate import tabulate

filename = open('dmpclient.txt', 'r')
clientlist = []
for lines in filename:
    clientlist.append(str(lines).replace("\n", ""))


def algoBilAttributionDetails(yearvar, monthvar, datevar, i):
    modval = []
    for val in range(0, 1000):
        modval.append(int(val))
    strs = "select * from behaviorlog{0}{1}{2} where clientid='{3}' and uuidmod in {4}".format(yearvar, monthvar,
                                                                                               datevar, i,
                                                                                               tuple(modval))
    # print strs
    rows = servercredentials.cassandraDictRowfinderDmp(strs)
    for j in rows:
        json_output = {}
        json_output['clientid'] = j['clientid']
        # json_output['uuidmod'] = j['uuidmod']
        json_output['bhtype'] = j['bhtype']
        json_output['uuid'] = j['uuid']
        json_output['creationdate'] = j['creationdate']
        json_output['behavior'] = j['behavior']
        json_output['cbhlist'] = j['cbhlist']
        # json_output['insertiondate'] = j['insertiondate']
        js = json.dumps(json_output)
        print js


try:
    for adv in clientlist:
        threaddmpvals = threading.Thread(target=algoBilAttributionDetails,
                                         kwargs={'yearvar': sys.argv[1], 'monthvar': sys.argv[2],
                                                 'datevar': sys.argv[3], 'i': adv})
        threaddmpvals.start()
except Exception as e:
    print e
