# first of all import the socket library
import socket
import json
from cache import Cache
from autologging import logged
import logging
from threading import Thread, Event
import datetime

@logged(logging.getLogger("kafka.monitor.log"))
class CacheServer(Thread):
    """
    Creates an internal cache server that listens on a specified port
    """
    def __init__(self, config):
        self.__log.info("Instantiating Cache Server")
        if config is None:
            raise ValueError("Config is not provided")
        Thread.__init__(self)
        THREAD_NAME = 'cache-server'
        self.setName(THREAD_NAME)
        self._stop = Event()
        self.setDaemon(True)
        self.host = config.get('internal_cache', 'cache_server_host')
        self.port = config.get('internal_cache', 'cache_server_port')
        self.cache_max_size = config.get('internal_cache', 'cache_max_size')

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        server_socket = None
        try:
            # create a socket object
            server_socket = socket.socket()
            self.__log.info("Created socket for Cache Server")
            server_socket.bind((self.host, int(self.port)))
            self.__log.info("Socket is bound to to {}:{}".format(self.host, self.port))
            # put the socket into listening mode
            server_socket.listen(5)
            self.__log.info("Cache Server socket is listening")
        except OSError as e:
            self.__log.error("Error while creating socket for Cache Server. Error: {}".format(str(e)))
            return False

        if server_socket is not None:
            # create the global cache object
            self.__log.info("Creating Global Cache")
            self.cache = Cache(self.cache_max_size)
            # a forever loop until we interrupt it or an error occurs
            while not self.stopped():
                try:
                    # Establish connection with client.
                    conn, addr = server_socket.accept()
                    self.__log.info("Connection received from {}".format(addr))
                    request_data = conn.recv(10240)
                    self.__log.info("Preparing response")
                    response = self._handle_request(request_data)
                    # send the response back to client
                    conn.send(response)
                    # Close the connection with the client
                    conn.close()
                except Exception as e:
                    self.__log.error("Error while processing request. Error: {}".format(str(e)))
                    conn.close()
                    return False # return a False so that the main thread will exit
                finally:
                    conn.close()

    def _handle_request(self, request):
        self.__log.info("Request received")
        json_data = None
        try:
            json_data = json.loads(request, 'utf-8')
        except Exception as e:
            self.__log.error("Error while loading the json request. Error: {}".format(str(e)))
        else:
            action = json_data["action"]
            data = json_data["data"]
            key = data["key"]
            self.__log.debug("Request data: action: {}, key: {}, data: {}".format(action, key, data))
            if action == "set":
                value = data["value"] #json.loads(data["value"])
                self.cache.set(key, value)
                if "ttl" in data.keys():
                    ex_key = key + "_ttl"
                    ttl = int(data["ttl"])
                    self.cache.set(ex_key, ttl)
                #self.__log.debug("Testing if the value is set: {}".format(self.cache.get(key)))
        return json.dumps(self.cache.get(key))