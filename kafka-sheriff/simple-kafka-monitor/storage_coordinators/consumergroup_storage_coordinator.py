from connectors.storage_connector import StorageConnector

class ConsumerGroupStorageCoordinator():
    """
    This class connects to the storage and gets the information about the consumergroup from the storage
    """
    def __init__(self, config):
        self.cache = StorageConnector(config).connect()

    def set_val(self, key, value, ttl):
        if ttl is None:
            self.cache.set(key, value)
        else:
            self.cache.setex(key, value, ttl)

    def push_val(self, key, value, ts, limit, ttl):
        if ttl is None:
            self.cache.push(key, value, ts, limit)
        else:
            self.cache.pushex(key, value, ts, limit, ttl)

    def set_topics(self, group_id, value, ttl=None):
        topics_key = "{}-topics".format(group_id)
        self.set_val(topics_key, value, ttl)

    def get_topics(self, group_id):
        topics_key = "{}-topics".format(group_id)
        return self.cache.get(topics_key)

    def get_partitions_count(self, group_id, topic):
        partitions_key = "{}-{}-partitions".format(group_id, topic)
        return self.cache.get(partitions_key)

    def set_partitions_count(self, group_id, topic, value, ttl=None):
        partitions_key = "{}-{}-partitions".format(group_id, topic)
        self.set_val(partitions_key, value, ttl)

    def get_headoffset(self, group_id, topic, partition):
        headoffsets_key = "{}-{}-{}-headoffsets".format(group_id, topic, partition)
        return self.cache.get(headoffsets_key)

    def set_headoffset(self, group_id, topic, partition, value, ts, limit, ttl=None):
        headoffsets_key = "{}-{}-{}-headoffsets".format(group_id, topic, partition)
        self.push_val(headoffsets_key, value, ts, limit, ttl)

    def get_committed(self, group_id, topic, partition):
        committed_key = "{}-{}-{}-committed".format(group_id, topic, partition)
        return self.cache.get(committed_key)

    def set_committed(self, group_id, topic, partition, value, ts, limit, ttl=None):
        committed_key = "{}-{}-{}-committed".format(group_id, topic, partition)
        self.push_val(committed_key, value, ts, limit, ttl)

    def get_currentlag(self, group_id, topic, partition):
        lag_key = "{}-{}-{}-currentlag".format(group_id, topic, partition)
        return self.cache.get(lag_key)

    def set_currentlag(self, group_id, topic, partition, value, ts, limit, ttl=None):
        lag_key = "{}-{}-{}-currentlag".format(group_id, topic, partition)
        self.push_val(lag_key, value, ts, limit, ttl)