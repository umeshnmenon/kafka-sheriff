from requests import get
import argparse
from re import match
from sys import stdout

class BurrowClient():
    """
    A client that queries a Burrow Server
    """
    def __init__(self, url='http://127.0.0.1:8000', api='v3/kafka'):
        self.url = url
        self.api = api
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
        address_list = [self.url, self.api]
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
        address = self.address(*paths)
        #if self.debug:
        #    self.output("=> GET: {address}".format(address=address))
        response = get(address)
        data = response.json()
        return data

    @property
    def clusters(self):
        """
        Get the list of configured clusters names
        Returns:
            list
        """
        if self._clusters:
            return self._clusters
        data = self.request()
        clusters = data.get('clusters', [])
        if clusters:
            self._clusters = clusters
        return clusters

    def consumers(self, cluster):
        """
        Get a list of consumer group names for the specified cluster
        Args:
            cluster(str): Cluster name
        Returns:
            list
        """
        key = cluster
        if key in self._consumers:
            return self._consumers[key]
        data = self.request(cluster, 'consumer')
        consumers = data.get('consumers', [])
        if consumers:
            self._consumers[key] = consumers
        return consumers

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

    def consumer_details(self, cluster, consumer):
        """
        Get details about the specified consumer group in a cluster
        Args:
            cluster(sr): Cluster name
            consumer(srt): Consumer group name
        Returns:
            dict
        """
        key = "{cluster}-{consumer}".format(
            cluster=cluster,
            consumer=consumer,
        )
        if key in self._consumer_details:
            return self._consumer_details[key]
        data = self.request(cluster, 'consumer', consumer)
        consumer_details = data.get('topics', {})
        if consumer_details:
            self._consumer_details[key] = consumer_details
        return consumer_details

    def consumer_status(self, cluster, consumer):
        """
        Get the status of the specified consumer group in a cluster
        Args:
            cluster(sr): Cluster name
            consumer(srt): Consumer group name
        Returns:
            dict
        """
        key = "{cluster}-{consumer}".format(
            cluster=cluster,
            consumer=consumer,
        )
        if key in self._consumer_status:
            return self._consumer_status[key]
        data = self.request(cluster, 'consumer', consumer, 'lag')
        consumer_status = data.get('status', {})
        if consumer_status:
            self._consumer_status[key] = consumer_status
        return consumer_status

    def get_total_lag(self):
        """
        Gets the total lag
        Args:
           cluster(str): Cluster name
           consumer(str): Consumer Group name
           topic(str): Topic name
        Returns:
           integer
        """
        lag = None
        for cluster in self.clusters:
            for consumer in self.consumers(cluster):
                if not self.match_consumer(consumer):
                    continue
                consumer_status = self.consumer_status(cluster, consumer)
                total_lag = consumer_status.get('totallag', 0)
                #consumer_details = self.consumer_details(cluster, consumer)
                #for topic, details in consumer_details.items():
                #    if not self.match_topic(topic):
                #        continue
                #    for detail in details:
                #        cur_lag = detail.get('current-lag', '?')
                #        cur_lag = self.format_number(cur_lag)

        return total_lag

    @staticmethod
    def output(message):
        """
        Output test message
        Args:
            message(str): text to output
        Returns:
            None
        """
        if not isinstance(message, str):
            message = str(message)
        if not message.endswith("\n"):
            message += "\n"
        stdout.write(message)


# Uncomment this if we want to run the client alone
# TODO: Make client also easily callable independently. Lowest priority
#if __name__ == "__main__":
#    arg_parser = argparse.ArgumentParser(
#        description='The Burrow API CLI tool'
#    )

#    #arg_parser.add_argument(
#    #    "-T", "--filter_topic",
#    #    default=None,
#    #    help="Filter stats by topic name regular expression"
#    #)

#    arg_parser.add_argument(
#        "-C", "--filter_consumer",
#        default=None,
#        help="Filter report output by consumer group name regular expression"
#    )

#    args = arg_parser.parse_args()

#    bc = BurrowClient() #BurrowClient(url=args.url, api=args.api)

#    #bc.filter_topic = args.filter_topic
#    bc.filter_consumer = args.filter_consumer
#    bc.output(bc.get_total_lag())