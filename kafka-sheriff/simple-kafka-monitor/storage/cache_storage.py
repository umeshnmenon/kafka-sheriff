from autologging import logged
import logging
from cacheclient.cache_client import CacheConnection
@logged(logging.getLogger("kafka.monitor.log"))
class CacheStorage:
    def __init__(self, host, db):
        """
        Connects to internal cache. A wrapper class.
        :param host:
        :param db:
        """
        self.__log.info("Internal Cache connection string - host: {}, port: {}".format(host, port))
        self.cache = CacheConnection(host, db)

    def __call__(self, *args, **kwargs):
        return self.cache