from KafkaTaasProducer import KafkaTaasProducer

producer = KafkaTaasProducer()
producer.kafkaProducer()

# from utility import Utility
#
# u = Utility()
# reading = u.readFromFile()
# # print type(reading),dir(reading)
# print reading.values


# from cassandra.cluster import Cluster
#
# # cluster = Cluster(contact_points=['127.0.0.1'],protocol_version=3)
# cluster = Cluster(contact_points=['192.168.36.70'], protocol_version=3)
# session = cluster.connect()
# session.set_keyspace('adlog')
# session.execute('use adlog')
#
# rows = session.execute("select * from adtracker")
# for i in rows:
#     print i
# import json
#
# f = open('taasapp.log','r')
# for lines in f:
#     line = json.loads(lines)
#     for key in line.keys():
#         print line[key]