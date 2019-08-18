from kafkaclient.kafka_channel import KafkaChannel
from httpserver.server import HTTPServer
from storage.cacheserver.cache_server import CacheServer
import ConfigParser, sys, argparse, os, time
from log import *
from autologging import logged
from helpers.kerberos_util import KerberosUtil
from threading import Thread
from security import *
from multi_thread import *
import socket
from storage import *

@logged(logging.getLogger("kafka.monitor.log"))
class SimpleKafkaMonitor:
    """
    This class starts the monitoring by starting a Kafka Channel and HTTP Server
    """
    def __init__(self, config_file = None):
        self.__log.info("Initializing Simple Kafka Monitor")
        self.__log.info("Reading config file")
        if config_file is None:
            config_file = self._get_config_file()
        self.config = self._get_config(config_file)
        self.env = env

    def _get_config_file(self, env = None):
        config_file = "settings_{}.ini"
        root_folder = os.path.dirname(os.path.realpath(__file__))
        if env is None:
            env = os.getenv("env", "sandbox")

        config_file = config_file.format(env)

        self.config_file = root_folder + '/' + config_file
        self.__log.info("Config file to use: {}".format(self.config_file))
        return self.config_file

    def _get_config(self, config_file):
        lconfig = ConfigParser.ConfigParser()
        lconfig.read(config_file)
        return lconfig

    def start(self):
        self.__log.info("Starting Simple Kafka Monitor......")
        # Four threads to execute
        # Kafka monitor, HTTP Server, Cache Server and a kinit refresher
        try:
            threads = []

            security_protocol = self.config.get('kafka', "protocol")
            if security_protocol == "SASL_SSL":
                # Make sure to set up stuffs for ssl and kerberos
                security = Security(self.config)
                security.setup_security(self.env)
                threads.append(Thread(target=security.kinit_periodic_refresh, name='kinit-periodic-refresh'))

            storage_type = self.config.get('cache', 'type')
            if storage_type == "internal":
                # start the Cache Storage server
                threads.append(CacheServer(self.config))

            threads.append(KafkaChannel(self.config))
            threads.append(HTTPServer(self.config))

            for i in range(0, len(threads)):
                thread = threads[i]
                err = thread.start()
                if err:
                    # if erred, stop the monitors in the reverse order
                    for j in range(len(threads), i):
                        prev_thread = threads[j]
                        prev_thread.stop()

            time.sleep(10)

            for thread in threads:
                thread.join()
        except Exception, e:
            self.__log.error("Error while running threads. Error: {}".format(str(e)))


def parse_args():
    arg_parser = argparse.ArgumentParser(
        description='Simple Kafka Monitor'
    )

    arg_parser.add_argument(
        "-C", "--config_file",
        default=None,
        help="Configuration file for Simple Kafka Monitor"
    )

    args = arg_parser.parse_args()
    return args

@logged(logging.getLogger("kafka.monitor.log"))
def get_config_file():
    config_file = "app.cfg"
    root_folder = os.path.dirname(os.path.realpath(__file__))
    config_file_abs_path = root_folder + '/' + config_file
    get_config_file._log.info("Config file used is: {}".format(config_file_abs_path))
    return config_file_abs_path

@logged(logging.getLogger("kafka.monitor.log"))
def main():
    args = parse_args()

    if args.config_file is None:
        config_file = get_config_file()

    else:
        config_file = args.config_file
    main._log.info("Loaded config file from {}".format(config_file))
    main._log.info("Starting Simple Kafka Monitor")
    kafka_monitor = SimpleKafkaMonitor(config_file)
    kafka_monitor.start()

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyboardInterrupt, EOFError):
        print("\nAborting ... Keyboard Interrupt.")
        sys.exit(1)