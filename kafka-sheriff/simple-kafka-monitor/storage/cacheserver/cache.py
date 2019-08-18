import datetime
from autologging import logged
import logging

def singleton(cls, *args):
    instances = {}
    def getinstance(*args):
        if cls not in instances:
            instances[cls] = cls(*args)
        return instances[cls]
    return getinstance

class Singleton(object):

    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = object.__new__(cls, *args, **kwargs)
        return cls._instances[cls]

@logged(logging.getLogger("kafka.monitor.log"))
#@singleton
class Cache(Singleton):
    """
    A Dictionary based simple internal cache
    """
    CONFIG_KEY = 'cache'

    def __init__(self, max_size):
        """Constructor"""
        self.cache = {}
        #self.max_cache_size = config.get(self.CONFIG_KEY, 'internal_cache_max_size')
        self.max_cache_size = max_size
        self.__log.debug("Id of Cache is {}".format(id(self)))

    def __contains__(self, key):
        """
        Returns True or False depending on whether or not the key is in the
        cache
        """
        return key in self.cache

    def update(self, key, value):
        """
        Update the cache dictionary and optionally remove the oldest item
        """
        if key not in self.cache and len(self.cache) >= self.max_cache_size:
            self.remove_oldest()
        self.__log.info("Updating the cache for key {}".format(key))
        try:
            self.cache[key] = {'date_accessed': datetime.datetime.now(),
                           'value': value}
        except Exception as e:
            self.__log.error("Error while setting the value for key {}".format(str(e)))

    def remove_oldest(self):
        """
        Remove the entry that has the oldest accessed date
        """
        oldest_entry = None
        for key in self.cache:
            if oldest_entry is None:
                oldest_entry = key
            elif self.cache[key]['date_accessed'] < self.cache[oldest_entry][
                'date_accessed']:
                oldest_entry = key
        self.__log.info("Removing the oldest entry from cache for key {}".format(key))
        self.cache.pop(oldest_entry)

    @property
    def size(self):
        """
        Return the size of the cache
        """
        return len(self.cache)

    def set(self, key, value):
        self.update(key, value)

    def get(self, key):
        try:
            if self._has_expired(key):
                return None
            else:
                return self.cache[key]['value'] if self.__contains__(key) else None
        except Exception as e:
            self.__log.error("Error while getting the value for key {} from storage cache. Error: {}".format(key, str(e)))
            return None

    def _has_expired(self, key):
        ex_key = key + "_ttl"
        if ex_key in self.cache.keys():
            ttl = int(self.cache[ex_key]['value'])
            #date_accessed = datetime.datetime.strptime(self.cache[key]['date_accessed'], '%Y-%m-%d %H:%M:%S.%f')
            date_accessed = self.cache[key]['date_accessed']
            if datetime.datetime.now() >  date_accessed + datetime.timedelta(seconds=ttl):
                self.__log.debug("The %s has expired and will be removed from cache", key)
                return True
        return False