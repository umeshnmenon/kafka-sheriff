from helpers.utils import Utils
import redis
from autologging import logged
import logging
@logged(logging.getLogger("kafka.monitor.log"))
class RedisStorage:
    """
    Connects to a Redis cache
    """

    def __init__(self, host, port, db):
        """

        :param host:
        :param port:
        :param database:
        """

        self.__log.info("Redis connection string - host: {}, port: {}, db: {}".format(host, port, db))
        self.connect_redis(host, port, db)

    def connect_redis(self, host, port, db):
        try:
            self.__log.info("Connecting to Redis")
            pool = redis.ConnectionPool(host=host, port=port, db=db)
            self.redis = redis.Redis(connection_pool=pool)
        except Exception, e:
            self.__log.error("Error while connecting to Redis. Error: {}".format(str(e)))

    def __call__(self, *args, **kwargs):
        return self.redis