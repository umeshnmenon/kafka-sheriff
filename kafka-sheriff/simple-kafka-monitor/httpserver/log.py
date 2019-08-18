import logging, sys, os
import ConfigParser
env = os.getenv("env", "sandbox")
config_file = "/etc/simple-kafka-monitor/settings_{}.ini".format(env)
config = ConfigParser.ConfigParser()
config.read(config_file)
log_level_key = 'consumer_log_level'
consumer_log_level = config.get('log', log_level_key)
consumer_log_level = os.getenv(log_level_key, consumer_log_level).upper()
# or we can just use getattr() instead as commented below
#log_level = getattr(logging, consumer_log_level)
if consumer_log_level == 'CRITICAL':
    log_level = logging.CRITICAL
elif consumer_log_level == 'ERROR':
    log_level = logging.ERROR
elif consumer_log_level == 'WARNING':
    log_level = logging.WARNING
elif consumer_log_level == 'INFO':
    log_level = logging.INFO
else:
    log_level = logging.DEBUG

out_type = config.get('log', 'out_type')
log_folder = config.get('log', 'location')

formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:process %(process)d:process name %(processName)s:%(funcName)s():line %(lineno)d:%(message)s')

# HTTP Access logger
access_log_file = log_folder + "http.server.access.log"
access_logger = logging.getLogger("http.server.access.log")
access_logger.setLevel(log_level)
if out_type == "file":
    # stdout_handler = logging.FileHandler(LOGCONFIG.get("locations", "kafka_backpressure_log"), mode='a')
    stdout_handler_access_log = logging.FileHandler(access_log_file, mode='a')
else:
    stdout_handler_access_log = logging.StreamHandler(sys.stdout)

stdout_handler_access_log.setLevel(log_level)
stdout_handler_access_log.setFormatter(formatter)
access_logger.addHandler(stdout_handler_access_log)

# HTTP Error logger
error_log_file = log_folder + "http.server.error.log"
error_logger = logging.getLogger("http.server.error.log")
error_logger.setLevel(log_level)
if out_type == "file":
    # stdout_handler = logging.FileHandler(LOGCONFIG.get("locations", "kafka_backpressure_log"), mode='a')
    stdout_handler_error_log = logging.FileHandler(error_log_file, mode='a')
else:
    stdout_handler_error_log = logging.StreamHandler(sys.stdout)

stdout_handler_error_log.setLevel(log_level)
stdout_handler_error_log.setFormatter(formatter)
error_logger.addHandler(stdout_handler_error_log)