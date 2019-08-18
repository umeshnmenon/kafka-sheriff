from storage.storage import Storage
from autologging import logged
import logging
@logged(logging.getLogger("kafka.monitor.log"))
class StorageConnector():
    """
    Class establishes connection to Storage
    """
    def __init__(self, config):
        self.config = config

    def connect(self):
        """
        Connects to a storage
        :return:
        """
        storage_type = self.config.get('cache', 'type')
        self.__log.info("Creating the storage cache of type {}".format(storage_type))
        cache = Storage(storage_type, self.config) #.cache
        self.__log.info("Connected to cache")
        return cache