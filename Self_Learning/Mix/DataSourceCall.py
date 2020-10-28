from ClientRelated.MultipleDatasource import ConnectionInstances as ce


def connectionReturn(name):
    if 'mysql' in name:
        mysqlconnection = ce.mySqlConnection()
        return mysqlconnection
    elif 'cassandra' in name:
        cassconnection = ce.cassandraConnector()
        return cassconnection
    elif 'redis' in name:
        redisconnection = ce.redisConnection()
        return redisconnection
