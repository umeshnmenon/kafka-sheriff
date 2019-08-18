import socket
import logging
from autologging import logged
import json

@logged(logging.getLogger("kafka.monitor.log"))
class CacheConnection:
    """
    A simple client to connect to Cache Server
    """
    def __init__(self, host, port):
        #self.connect(host, port)
        self.host = host
        self.port = port

    def connect(self, host, port):
        try:
            # Create a socket object
            self.sock = socket.socket()
            self.__log.info("Created client socket to Cache Server")
            # connect to the internal cache server in the local host
            self.__log.info("Connecting to local Cache Server {}:{}".format(host, port))
            self.sock.connect((host, port))
        except OSError as e:
            self.__log.error("Error while creating socket for Cache Server. Error: {}".format(str(e)))

    def _send(self, request):
        self.connect(self.host, self.port)
        response = None
        try:
            if self.sock is not None:
                self.__log.info("Sending the request")
                # send the message
                self.sock.send(request)
                # receive data from the server
                response = self.sock.recv(10240)
        except Exception as e:
            self.__log.error("Error while sending request. Error: {}".format(str(e)))
        finally:
            self.close()
        return response

    def close(self):
        # close the connection
        self.sock.close()

    def _prepare_get_request(self, key):
        request_json = {"action": "get", "data": {"key": key}}
        request_str = json.dumps(request_json)
        return request_str

    def _prepare_set_request(self, key, value, expiry=None):
        if expiry is None:
            request_json = {"action": "set", "data": {"key": key, "value": value}}
        else:
            request_json = {"action": "set", "data": {"key": key, "value": value, "ttl": expiry}}
        request_str = json.dumps(request_json)
        return request_str

    def get(self, key):
        request = self._prepare_get_request(key)
        self.__log.debug("Prepared get request: {}".format(request))
        response = self._send(request)
        return response

    def set(self, key, value):
        # here the value is a string, so convert it to a json first
        value_json = json.loads(value, 'utf-8')
        request = self._prepare_set_request(key, value_json)
        self.__log.debug("Prepared set request: {}".format(request))
        response = self._send(request)


    #def setex(self, key, value, expiry):
    def setex(self, name, value, time):
        # here the value is a string, so convert it to a json first
        value_json = json.loads(value, 'utf-8')
        request = self._prepare_set_request(name, value_json, time)
        self.__log.debug("Prepared set request: {}".format(request))
        response = self._send(request)