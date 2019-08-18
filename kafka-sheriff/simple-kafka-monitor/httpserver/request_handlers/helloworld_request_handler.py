import logging
from autologging import logged
from httpserver.response_codes import SimpleHTTPStatusOK, SimpleHTTPErrorInternalError
import threading
from generic_request_handler import GenericRequestHandler
@logged(logging.getLogger("http.server.log"))
class HelloworldRequestHandler(GenericRequestHandler):
    """
    A Hellow World request handler
    """
    # def __init__(self, config=None):
    #     self.config = config

    def hello_world(self, args):
        """
        Serve the HTTP Request
        :return:
        """
        return 200, "Hello World! {}".format(args['test_name'])