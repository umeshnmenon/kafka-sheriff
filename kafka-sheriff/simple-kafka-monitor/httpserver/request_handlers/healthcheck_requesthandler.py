import logging, json
from autologging import logged
from httpserver.response_codes import SimpleHTTPStatusOK, SimpleHTTPErrorInternalError
import threading
from generic_requesthandler import GenericRequestHandler
@logged(logging.getLogger("kafka.monitor.log"))
class HealthcheckRequestHandler(GenericRequestHandler):
    """
    A simple healthcheck request handler.
    """
    # def __init__(self, config=None):
    #     self.config = config

    def index(self):
        """
        Serve the HTTP Request
        :return:
        """

        # first check if kafka is working or not
        # check if the following threads are alive
        return self.check_health()

    def check_health(self):

        #check if kafka channel is running or not
        self.__log.info("Checking if kafka channel is working or not...")
        if not self.is_thread_alive('kafka-channel'):
            raise Exception(str(SimpleHTTPErrorInternalError(description = "Kafka channel is not running. Please check the logs from CloudWatch for troubleshooting.")))

        #check if Cache server is running
        storage_type = self.config.get('cache', 'type')
        if storage_type == "internal":
            self.__log.info("Checking if Cache server is working...")
            if not self.is_thread_alive('cache-server'):
                raise Exception(str(SimpleHTTPErrorInternalError(
                    description="Cache Server is not running. Please check the logs from CloudWatch for troubleshooting")))

        #check if consumer monitor is running
        self.__log.info("Checking if consumer monitor is running...")
        if not self.is_thread_alive("consumer-monitor"):
            raise Exception(str(SimpleHTTPErrorInternalError(description = "Kafka Monitor for group id {} is not running. Please check the logs from CloudWatch for troubleshooting.".format(group_id))))

        # else
        # returns nothing but a 200 status
        return 200, SimpleHTTPStatusOK

    def is_thread_alive(self, thread_name):
        #isalive = lambda tname: any((i.name == tname) and i.is_alive() for i in threading.enumerate())
        isalive = False
        for t in threading.enumerate():
            if t.name == thread_name:
                if t.is_alive():
                    isalive = True
                else:
                    isalive = False
        self.__log.debug("Is thread {} alive: {}".format(thread_name, isalive))
        return isalive