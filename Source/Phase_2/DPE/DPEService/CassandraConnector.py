from cassandra.cluster import Cluster

cluster = Cluster(contact_points=['192.168.75.128'], protocol_version=3)
# cluster = Cluster(contact_points=['192.168.33.55'], protocol_version=3)
session = cluster.connect()
session.set_keyspace('adlog')
session.execute('USE adlog')


def cassandraConnection():
    return session
