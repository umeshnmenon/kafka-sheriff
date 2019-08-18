import SimpleHTTPServer #, BaseHTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from urlparse import urlparse, parse_qs
from helpers.utils import Utils
from response_codes import *
from log import *
from router import *
from autologging import logged
import logging
import re
import json
@logged(logging.getLogger("kafka.monitor.log"))
#class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
class RequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    """
    Request Handler class. This calls works in mostly a REST way. It creates the controller class automatically from the
    second element from the path (for e.g. creates controller class for consumergroup from http://localhost/consumergroup/)
    """
    err_msg = ''
    MODULE_PATH = "httpserver.request_handlers.{}_requesthandler.{}RequestHandler" # Change here if the module path changes

    def __init__(self, config, *args):
        self.__log.info("Instantiating RequestHandler class")
        self.config = config
        self.__routes = Router()
        #self.log_folder = config.get('log', 'location')
        #self.log_folder = "/var/log/" if self.log_folder is None else self.log_folder
        #self._setup_logger()
        #BaseHTTPRequestHandler.__init__(self, *args)
        #super(RequestHandler, self).__init__(self, *args)
        SimpleHTTPServer.SimpleHTTPRequestHandler.__init__(self, *args)


    def _set_headers(self, status_code, content_type='text/json'):
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.end_headers()

    def do_GET(self):
        self.err_msg = ''
        # We can do a simple check on the parsed.path to get the healthcheck url. However to make it consistent we can add
        # a handler class for healthcheck
        access_logger.info("Processing GET request")
        access_logger.debug("Access url: {}".format(str(self.path)))
        header, body = self.process_request()
        self._set_headers(header)
        access_logger.debug("Response Header is {}".format(header))
        access_logger.debug("Response Body is {}".format(body))
        self.wfile.write(body)

    def do_HEAD(self):
        self._set_headers()


    def do_POST(self):
        # TODO: Not tested. Change the function similar to do_GET
        access_logger.info("Processing POST request")
        access_logger.debug("Access url: {}".format(str(self.path)))
        rewrite_path = self.rewrite_path()
        self._set_headers()
        content_length = int(self.headers['Content-Length'] if 'Content-Length' in self.headers else 0)
        if content_length > 0:
            self.data_string = self.rfile.read(content_length)
            data = json.loads(self.data_string)
            req_handler = self.get_request_handler()
            if req_handler is not None:
                try:
                    response = req_handler.serve_request(self.request, rewrite_path)
                    access_logger.info("Status OK")
                    self._set_headers(200)
                except Exception as e:
                    access_logger.info("Error while processing request")
                    self._set_headers(500)
                    response = str(e)
            else:
                error_logger.error("Error while processing GET request")
                error_logger.error(self.err_msg)
                self._set_headers(500)
                response = self.err_msg

        else:
            error_logger.error("Error while processing POST request")
            response = SimpleHTTPErrorZeroContentLength()
            error_logger.error(response)

        access_logger.debug("Response is {}".format(response))
        self.wfile.write(response)

    def get_request_handler(self):
        """
        Creates the HTTP controller instance from path string
        :return:
        """
        argpath = self.get_argpath()
        args = argpath.split('/')
        # TODO: Do a proper way of request path handling
        # Now we have context_root in place
        if len(args) == 0 or argpath == '/':
            #self.err_msg = SimpleHTTPStatusOK()
            handler = None
        else:
            try:
                handler_name = Utils.strip_char('/', args[1])
                self.__log.info("Handler Name is {}".format(handler_name))
                handler_module = self._get_module_path(handler_name)
                self.__log.info("Creating HTTP Controller class for {}".format(handler_module))
                handler = Utils.create_instance(handler_module, self.config)
            except Exception as e:
                self.err_msg =  SimpleHTTPErrorControllerNotExists()
                self.__log.error("Error while creating HTTP Controller instance")
                self.__log.error(self.err_msg)
                self.__log.error(str(e))
                handler = None
                #raise
        return handler

    def rewrite_path(self):
        rewrite_rule = "/{}/".format(config.get('http', 'context_root'))
        replace_str = "/"
        self.__log.info("Original path is {}".format(self.path))
        new_path = self.path.replace(rewrite_rule, replace_str)
        # remove the trailing slash
        new_path = new_path.rstrip("/")
        self.__log.info("Rewritten path is {}".format(new_path))
        return new_path

    def _get_module_path(self, handler_name):
        #return "httpserver.request_handlers." + coord_name + "_controller." + coord_name.title() + "HTTPController"
        return self.MODULE_PATH.format(handler_name, handler_name.title())

    def log_message(self, format, *args):
        # To supress console stdout
        return

    def process_request(self):

        req_handler = self.get_request_handler()
        if req_handler is not None:
            try:
                func, args = self.load_args_from_path()
                print(args)
                #header, body = getattr(req_handler, func)(args)  # apply(self.__dict__[handler], (args,))
                header, body = getattr(req_handler, 'process_request')(func, args)
                access_logger.info("Status OK")
            except Exception as e:
                access_logger.info("Error while processing request")
                header = 500
                body = str(e)
        else:
            error_logger.error("Error while processing request")
            error_logger.error(self.err_msg)
            header = 500
            body = self.err_msg
        return header, body

    def get_argpath(self):
        path = self.rewrite_path()
        parsed = urlparse(path)
        argpath = parsed.path #parsed['path']
        return argpath

    def load_args_from_path(self):
        args = None
        argpath = self.get_argpath()
        for urlpath, fn in self.__routes():
            #urlpath = self.make_regexed(urlpath)
            #self.__log.debug("path: {}, func: {}".format(urlpath, handler))
            if re.search(urlpath, argpath):
                self.__log.debug("Matched path and func for the req: {}, {}, {}".format(urlpath, fn, argpath))
                m = re.match(urlpath, argpath)
                args = m.groupdict()
                func = fn
                break
        self.__log.debug("func: {}, args: {}".format(func, args))
        return func, args

    def make_regexed(self, path):
        return path
#    def _setup_logger(self):
#        self._setup_access_logger()
#        self._setup_error_logger()

#    def _setup_access_logger(self):
#        # HTTP Access logger
#        access_log_file = self.log_folder + "http.server.access.log"
#        self.access_logger = logging.getLogger("http.server.access.log")
#        self.access_logger.setLevel(logging.DEBUG)
#        formatter = logging.Formatter(
#            '%(asctime)s:%(levelname)s:process %(process)d:process name %(processName)s:%(funcName)s():line %(lineno)d:%(message)s')
#        # stdout_handler = logging.FileHandler(LOGCONFIG.get("locations", "kafka_backpressure_log"), mode='a')
#        stdout_handler_access_log = logging.FileHandler(access_log_file, mode='a')
#        stdout_handler_access_log.setLevel(logging.DEBUG)
#        stdout_handler_access_log.setFormatter(formatter)
#        self.access_logger.addHandler(stdout_handler_access_log)

#    def _setup_error_logger(self):
#        # HTTP Error logger
#        error_log_file = self.log_folder + "http.server.error.log"
#        self.error_logger = logging.getLogger("http.server.error.log")
#        self.error_logger.setLevel(logging.DEBUG)
#        formatter = logging.Formatter(
#            '%(asctime)s:%(levelname)s:process %(process)d:process name %(processName)s:%(funcName)s():line %(lineno)d:%(message)s')
#        # stdout_handler = logging.FileHandler(LOGCONFIG.get("locations", "kafka_backpressure_log"), mode='a')
#        stdout_handler_error_log = logging.FileHandler(error_log_file, mode='a')
#        stdout_handler_error_log.setLevel(logging.DEBUG)
#        stdout_handler_error_log.setFormatter(formatter)
#        self.error_logger.addHandler(stdout_handler_error_log)