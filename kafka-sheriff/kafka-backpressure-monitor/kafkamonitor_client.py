from requests import get
import logging, json
from autologging import logged

@logged(logging.getLogger("kafka.backpressure"))
class KafkaMonitorClient:
    """
    A client that makes http request to simple-kafka-monitor server
    """

    def __init__(self, host, port, context_root):
        self.host = host
        self.port = port
        self.url = "http://{}:{}/{}".format(host, port, context_root)
        self.api = ""
        self.filter_topic = None
        self.filter_consumer = None

        self._clusters = None
        self._cluster_details = {}
        self._consumers = {}
        self._topics = {}
        self._topic_details = {}
        self._consumer_details = {}
        self._consumer_status = {}

    def address(self, *paths):
        """
        Get the full address for the specified path components
        Args:
            *paths(list): A list of path components
        Returns:
            str
        """
        address_list = [self.url] # , self.api
        address_list += list(paths)
        return '/'.join(address_list)

    def request(self, *paths):
        """
        Send the HTTP request to the URL specified by path components
        Args:
            *paths(str): A list of path component strings
        Returns:
            dict
        """
        # catch the error and just log and do nothing because the server might not have been ready yet
        data = None
        try:
            address = self.address(*paths)
            self.__log.info("GET {}".format(address))
            #if self.debug:
            #    self.output("=> GET: {address}".format(address=address))
            response = get(address)
            data = None
            if response is not None:
                data = response.json()
            self.__log.debug("Response is {}".format(data))
        except Exception as e:
            self.__log.error(str(e))
        return data

    def match_topic(self, topic):
        """
        Check if the topic name passes filter or there is not topic filter defined.
        Args:
            topic(str): Topic name
        Returns:
            bool
        """
        if not self.filter_topic:
            return True
        if not topic:
            return False
        return bool(match(self.filter_topic, topic))

    def match_consumer(self, consumer):
        """
        Check if the consumer group name passes filter or there is not consumer group filter defined.
        Args:
            consumer(str): Consumer group name
        Returns:
            bool
        """
        if not self.filter_consumer:
            return True
        if not consumer:
            return False
        return bool(match(self.filter_consumer, consumer))

    def get_total_lag(self, group_id, topic):
        # For this version directly get the total lag
        data = None
        total_lag = 0
        self.__log.info("Getting total_lag")
        try:
            data = self.request('consumergroup', group_id, "topics", topic)
            if data is not None:
                total_lag = data["total_lag"]
        except Exception as e:
            self.__log.error("Error while getting total_lag. Error is {}".format(str(e)))
        return total_lag
