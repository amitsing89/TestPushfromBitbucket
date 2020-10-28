from datetime import datetime
import ConfigParser

cfg = ConfigParser.RawConfigParser()
cfg.read('parameters.cfg')


class PolicyMetadataObject:
    def __init__(self, data=[], columns={}, metadata={}, **kwargs):
        self.__data = data
        self.__columns = columns
        self.__metadata = metadata
        self.__extra = dict()

        # get extra params
        if kwargs is not None:
            for k, v in kwargs.iteritems():
                # print k, v
                self.__extra[k] = v

    def get_data(self):
        data_dict = dict()
        if len(self.__data) != 0:
            data_dict['row'] = self.__data
        return data_dict

    def get_rule(self):
        rule_col_dict = dict()
        if self.__columns is not None:
            rule_col_dict['rule'] = self.__columns

        return rule_col_dict

    def get_partition_key(self):
        # get partition key
        partition_key = None
        if self.__metadata is not None:
            if partition_key is None:
                partition_date = datetime.strptime(self.__metadata[cfg.get('KAFKA', 'key_field')],
                                                   cfg.get('KAFKA', 'date_time_format'))
                partition_key = '%02d-%02d' % (partition_date.month, partition_date.day)
                print 'partition key generated ==>', partition_key
        return partition_key

    def get_optional_params(self):
        if self.__extra is not None:
            return self.__extra

    def get_policy_metadata(self):
        metadata_str = dict(self.get_data().items() + self.get_rule().items() + self.get_optional_params().items())
        key = self.get_partition_key()
        return metadata_str, key
