from autologging import logged
import logging
import time, copy
from threading import Thread, Event
from connectors.kafka_connector import Kafka_Consumer
from kafka import TopicPartition
#from connectors.storage_connector import StorageConnector
from storage_coordinators.consumergroup_storage_coordinator import ConsumerGroupStorageCoordinator
from gssapi.raw.misc import GSSError # to catch the kerberos/ssl related error

@logged(logging.getLogger("kafka.monitor.log"))
class ConsumerMonitor(Thread):
    """
    Periodically fetches the list of topics, list of group, offset, lags etc and stores it in Storage for use by Evaluator
    """

    def __init__(self, config, *args):
        self.__log.info("Instantiating Consumer Monitor")
        self.config = config
        Thread.__init__(self)
        name = "consumer-monitor"
        self.setName(name)
        self.setDaemon(True)
        self._stop = Event()
        self.__log.info("Successfully instantiated Consumer Monitor")

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        self.__log.info("Starting Consumer Monitor...")
        kafka_key = 'kafka'
        group_id = self.config.get(kafka_key, "group_id")
        topic = self.config.get(kafka_key, "topic")
        topics = topic.split(",")
        group_ids = group_id.split(",")
        #self.cache = StorageConnector(self.config)
        self.storage_coordinator = ConsumerGroupStorageCoordinator(self.config)
        consumers = self.get_consumers(group_ids, topics)
        ttl = self.config.get('cache', 'cache_expiration_time.seconds')
        limit = self.config.get('cache', 'sliding_window')
        self.__log.debug("Cache expiration is set as : {}".format(ttl))
        self.__log.debug("Length of the sliding window is set as :{}".format(limit))
        interval = int(self.config.get('cache', 'interval'))
        while not self.stopped():
            for i in range(0, len(topics)):
                group_id = group_ids[i]
                topic = topics[i]
                consumer = consumers[i]
                try:
                    self.store_topics(group_id, consumer, ttl)
                    self.store_partition_count(group_id, topic, consumer, ttl)
                    self.store_offsets(group_id, topic, consumer, ttl, limit)
                except Exception as e:
                    self.__log.error("Error while storing Consumer metrics. Error: {}".format(str(e)))
                    return False
            time.sleep(interval)

    def store_topics(self, group_id, consumer, ttl):
        try:
            topics = self.get_topics(consumer)
            self.storage_coordinator.set_topics(group_id, topics, ttl)
        except Exception as e:
            self.__log.error("Error while fetching topics. Error: {}".format(str(e)))

    def store_partition_count(self, group_id, topic, consumer, ttl):
        try:
            partition_ids = consumer.partitions_for_topic(topic)
            if partition_ids is None:
                self.__log.error("No partitions found for topic {}".format(topic))
                return None
            num_partitions = len(partition_ids)
            self.__log.debug("Number of partitions for topic {}: {}".format(topic, num_partitions))
            # This is needed for the evaluator to the run the loop for number of partitions
            self.storage_coordinator.set_partitions_count(group_id, topic, num_partitions, ttl)
        except Exception as e:
            self.__log.error("Error while fetching number of partitions for topic {}. Error: {}".format(topic, str(e)))

    def store_offsets(self, group_id, topic, consumer, ttl, limit):
        """
        Fetches the Committed offsets and current lag
        :return:
        """

        if group_id is None:
            group_id = self.group_id

        if topic is None:
            topic = self.topic

        self.__log.debug("Fetching offset info for group_id = {} and topic = {}".format(group_id, topic))
        partition_ids = consumer.partitions_for_topic(topic)
        if partition_ids is None:
            self.__log.error("No partitions found for topic {}".format(topic))
            return None
        num_partitions = len(partition_ids)
        #self.__log.debug("Number of partitions for topic {}: {}".format(topic, num_partitions))

        ts = time.time()  # str(time.time())  # datetime.datetime.now()
        for p in partition_ids:
            self.__log.debug("Fetching info for partition {}".format(p))
            try:
                last_offset, committed, current_lag = self.get_offsets(consumer, topic, p)
                # store the values in storage
                self.storage_coordinator.set_headoffset(group_id, topic, p, last_offset, ts, limit, ttl)
                self.storage_coordinator.set_committed(group_id, topic, p, committed, ts, limit, ttl)
                self.storage_coordinator.set_currentlag(group_id, topic, p, lag, ts, limit, ttl)
                #headoffsets_key = "{}-{}-{}-headoffsets".format(group_id, topic, p)
                #committed_key = "{}-{}-{}-committed".format(group_id, topic, p)
                #lags_key = "{}-{}-{}-lags".format(group_id, topic, p)
                #self.cache.pushex(headoffsets_key, last_offset, ts, limit, ttl)
                #self.cache.pushex(committed_key, committed, ts, limit, ttl)
                #self.cache.pushex(lags_key, lag, ts, limit, ttl)
            except Exception, e:
                self.__log.error("Error while fetching consumer metric from Kafka. Error: {}".format(str(e)))
                # raise

    def get_offsets(self, consumer, topic, p):
        """
        Returns head offset, committed and lag
        :param consumer:
        :param partition:
        :return:
        """
        self.__log.debug("Fetching info for partition {}".format(p))
        try:
            tp = TopicPartition(topic, p)
            consumer.assign([tp])
            committed = consumer.committed(tp)
            committed = committed if committed is not None else 0
            consumer.seek_to_end(tp)
            last_offset = consumer.position(tp)
            # last_offset = consumer.end_offsets([tp]).values()[0]
            # last_offset = consumer.highwater(tp)
            last_offset = last_offset if last_offset is not None else 0
            current_lag = (last_offset - committed)
            return (last_offset, committed, current_lag)
        except Exception, e:
            self.__log.error("Error while fetching consumer metric from Kafka. Error: {}".format(str(e)))

    def get_current_timestamp_millis(self):
        #dt = datetime.now()
        #return dt.microsecond
        return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)


    def create_consumer(self, group_id=None, topic=None):
        consumer = None
        try:
            # copy the config
            lconfig = copy.copy(self.config)
            if group_id is not None:
                lconfig.set("kafka", "group_id", group_id)
            if topic is not None:
                lconfig.set("kafka", "topic", topic)
            consumer = Kafka_Consumer(lconfig).connect()
        except GSSError as ge:
            self.__log.error("Error while connecting to Kafka. This could either be because of invalid kerberos ticket or "
                             "of invalid ssl certificates.")
            self.__log.error("Error: {}".format(str(ge)))
            raise
        except Exception as e:
            self.__log.error("Error while connecting to Kafka. Error: {}".format(str(e)))
            raise
        return consumer

    def get_consumers(self, group_ids, topics):
        consumers = []
        for i in range(0, len(topics)):
            group_id = group_ids[i]
            topic = topics[i]
            consumer = self.create_consumer(group_id, topic)
            consumers.append(consumer)
        return consumers

    def get_topics(self, consumer):
        """Return list of kafka topics.
        """
        return list(consumer.topics())

    def __is_assigned(self, tp, consumer):
        if tp in consumer.assignment():
            return True
        return False