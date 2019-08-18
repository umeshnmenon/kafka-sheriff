import ConfigParser
from autologging import logged
from threading import Thread
import argparse, os, sys
from log import *
from const import *
from burrow_client import BurrowClient
from cloudwatch_metric_writer import CloudWatchMetricWriter
from kafkamonitor_client import KafkaMonitorClient

@logged(logging.getLogger("kafka.backpressure"))
class LagMonitor(Thread):
    """ Monitors the lag in a Kafka queue"""
    def __init__(self, env = None):
        self.__log.info("Instantiating LagMonitor")
        Thread.__init__(self)
        self.setName(THREAD_NAME)
        self.setDaemon(True)
        self.env = env
        self.setvars()

    def _get_config(self):
        """Gets the config"""
        self.CONFIG = ConfigParser.ConfigParser()
        self.config_file = self._get_config_file()
        try:
            self.CONFIG.read(self.config_file)
        except Exception as e:
            self.__log.error("Error while trying to read config file. Error: {}".format(str(e)))
            raise(e)

    def _get_config_file(self):
        config_file = "settings_{}.ini"
        #location = '/etc/kafka-backpressure-monitor/'
        root_folder = os.path.dirname(os.path.realpath(__file__))
        if self.env is None:
            self.env = os.getenv("env", "sandbox")

        config_file = config_file.format(self.env)
        #self.config_file = location + config_file
        self.config_file = root_folder + '/' + config_file
        return self.config_file

    def _get_env_var(self, key, default):
        if key in os.environ:
            self.__log.info("Loading {} from ENV variables".format(key))
        else:
            self.__log.info("Setting {} from config file".format(key, default))
            #self.__log.info("{} does not exist in ENV. Setting {} from config file".format(key, default))

        val = os.getenv(key, default)
        return val

    def setvars(self):
        """Sets the variables from config if not set by command line args"""
        self.__log.info("Setting the variables from config if not set by command line args or env variables")
        self._get_config()
        self.interval = int(self.CONFIG.get(APP, "poll_interval"))

        # DEPRECATED: We are not using Burrow because of sasl_ssl support with gssapi is not available
        #self.burrow_host = self.CONFIG.get(BURROW, "burrow.host")
        #self.burrow_port = self.CONFIG.get(BURROW, "burrow.port")
        #self.burrow_url = self.CONFIG.get(BURROW, "burrow.url")
        #self.burrow_api = self.CONFIG.get(BURROW, "burrow.api")

        self.km_host = self.CONFIG.get(KAFKAMONITOR, "host")
        self.km_port = self.CONFIG.get(KAFKAMONITOR, "port")
        self.km_context_root = self.CONFIG.get(KAFKAMONITOR, "context_root")

        self.lag_threshold = self.CONFIG.get(KAFKA, "lag.threshold")
        cw_region_key = "cloudwatch_region"
        namespace_key = "metric_namespace"
        consumergroup_key = "consumer.group"
        topic_key = "topic"
        self.region = self.CONFIG.get(CLOUDWATCH, cw_region_key) #self._get_env_var(cw_region_key, self.CONFIG.get(CLOUDWATCH, cw_region_key))
        self.namespace = self.CONFIG.get(CLOUDWATCH, namespace_key) #self._get_env_var(namespace_key, self.CONFIG.get(CLOUDWATCH, namespace_key))
        self.total_lag_metric_name = self.CONFIG.get(CLOUDWATCH, "total_lag_metric_name")

        self.filter_topic = None
        self.filter_consumer = None
        self.consumergroup = self.filter_consumer or self.CONFIG.get(KAFKA, consumergroup_key) #self._get_env_var(consumergroup_key, self.CONFIG.get(KAFKA, consumergroup_key))
        self.topic = self.filter_topic or self.CONFIG.get(KAFKA, topic_key) #self._get_env_var(topic_key, self.CONFIG.get(KAFKA, topic_key))

    def run(self):
        """Main method"""
        gen_logger.info("Starting Backpressure Monitor....")
        gen_logger.debug("Starting the daemon")
        total_lag = None
        # DEPRECATED: We are not using Burrow because of sasl_ssl support with gssapi is not available
        # bc = BurrowClient(self.url)
        km = KafkaMonitorClient(self.km_host, self.km_port, self.km_context_root)
        self.consumergroup = self.filter_consumer or self.consumergroup
        self.topic = self.filter_topic or self.topic
        while True:
            try:
                total_lag = km.get_total_lag(self.consumergroup, self.topic)
                gen_logger.info("total_lag is {}".format(total_lag))
                #total_lag = bc.get_total_lag()
                #if total_lag > self.lag_threshold:
                #    self.put_clouwatch_metric(total_lag)
                #    gen_logger.info("Total lag exceeded, updating the CloudWatch metric....")
                self.put_cloudwatch_metric(total_lag)
            except Exception as e:
                # logging as info
                gen_logger.info("Error while getting the total_lag. Error is {}".format(str(e)))
                pass
            time.sleep(self.interval)


    def put_cloudwatch_metric(self, total_lag):
        """Writes the metric to CloudWatch"""
        #metrics = {self.total_lag_metric_name : total_lag}
        dimension_name = CW_METRIC_DIMENSION_CONSUMERGROUP
        dimension_val = self.consumergroup
        unit = 'Count' # TODO: Change the unit here
        try:
            writer = CloudWatchMetricWriter(self.region)
            writer.put_metric(self.namespace, self.total_lag_metric_name, total_lag, dimension_name,
                              dimension_val, unit)
            # writer.put_metric(self.namespace, metrics, dimension_name, dimension_val, unit)
        except Exception as e:
            gen_logger.error("Error while getting the put metrics. Error is {}".format(str(e)))

def main():

    arg_parser = argparse.ArgumentParser(
        description='The Simple Lag Monitor'
    )

    arg_parser.add_argument(
        "-C", "--filter_consumer",
        default=None,
        help="Filter report output by consumer group id regular expression"
    )
    arg_parser.add_argument(
        "-E", "--env",
        default=None,
        help="Environment name to pick the configurarion file"
    )
    #arg_parser.add_argument(
    #    "-u", "--url",
    #    default="http://127.0.0.1:8000",
    #    help="Burrow server root URL",
    #)
    # arg_parser.add_argument(
    #    "-h", "--host",
    #    default="http://127.0.0.1",
    #    help="Burrow server host URL",
    # )
    # arg_parser.add_argument(
    #    "-p", "--port",
    #    default="8000",
    #    help="Burrow server port",
    # )
    #arg_parser.add_argument(
    #    "-a", "--api",
    #    default='v3/kafka',
    #    help="Burrow API prefix after the root URL"
    #)
    arg_parser.add_argument(
        "-T", "--filter_topic",
        default=None,
        help="Filter report output by topic name regular expression"
    )

    args = arg_parser.parse_args()

    lag_monitor = LagMonitor(args.env)
    lag_monitor.filter_consumer = args.filter_consumer
    lag_monitor.filter_topic = args.filter_topic

    tasks = [
        lag_monitor
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

    #for task in tasks:
    #    task.stop()

    for task in tasks:
        task.join()

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyboardInterrupt, EOFError):
        print("\nAborting ... Keyboard Interrupt.")
        sys.exit(1)