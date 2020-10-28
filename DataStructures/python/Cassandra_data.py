from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

qccluster = Cluster(contact_points=['localhost'], protocol_version=3)
session = qccluster.connect()
session.set_keyspace('adlog')
session.execute('USE adlog')

query = "select * from adtracker limit 10"
rows = session.execute(query=query)
for i in rows:
    print i
