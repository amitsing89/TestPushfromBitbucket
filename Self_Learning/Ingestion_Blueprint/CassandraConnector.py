from cassandra.cluster import Cluster


class CassandraConnector():
    def __init__(self):
        self.cluster = Cluster(contact_points=['127.0.0.1'], protocol_version=3)
        # cluster = Cluster(contact_points=['192.168.33.55'], protocol_version=3)
        self.session = self.cluster.connect()
        self.session.set_keyspace('adlog')
        self.session.execute('USE adlog')

    def cassandraConnection(self):
        return self.session
