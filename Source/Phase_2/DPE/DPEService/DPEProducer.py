import ConfigParser
import json
import sys
import Connections
from PolicyMetaDataReader import redisMetaData
from PolicyMetadataObject import PolicyMetadataObject

cfg = ConfigParser.RawConfigParser()
cfg.read('parameters.cfg')

from Utility import Utility
util = Utility()


class DPEProducer:
    def __init__(self):
        # Redis Connection
        self.redis = Connections.redisConnection()
        # Connection to kafka server as a producer
        self.producer = Connections.kafkaProducerConnection()
        # Topic of kafka
        self.topic = cfg.get('KAFKA', 'topic')

    # Producer Method for producing data
    def kafkaProducer(self):
        policyKey = sys.argv[2]
        headers = util.readHeaderFromFile()
        fields = cfg.get('REDIS', 'fields').split(',')
        # get columns field for rule-col mapping
        columns = redisMetaData().readFromRedis(policyKey, fields[0].strip())
        # get metadata to read last_executed_time for creating partition key
        metadata = redisMetaData().readMetadatafromRedis(policyKey, fields[1].strip())
        new_columns = dict()
        # update metadata with index:rule
        for col in columns:
            if col in headers:
                col_index = headers.index(col)
                new_columns[col_index] = columns[col]
        # separate optional params from metadata
        optional_params = cfg.get('REQUEST', 'optional_fields').split(',')
        extra = dict()
        for param in optional_params:
            if param in columns:
                extra[param] = columns[param]
        batch_size = int(cfg.get('KAFKA', 'batch_size'))
        while True:
            data = util.readFromFile(batch_size)
            if len(data) > 0:                         
                policy_metadata_object = PolicyMetadataObject(data, new_columns, metadata, **extra)
                policy_metadata_format, partition_key = policy_metadata_object.get_policy_metadata()
                policy_metadata_str = json.dumps(policy_metadata_format, default=lambda o: o.__dict__)
                print 'Producing messages ===>', policy_metadata_str
                self.producer.send(self.topic, key=partition_key, value=policy_metadata_str)
                self.producer.flush()
            else:
                break


executor = DPEProducer()
executor.kafkaProducer()
