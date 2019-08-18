import logging, sys, os #, time, logging.config
import ConfigParser
# All the logging bootstrapping is done here
env = os.getenv("env", "sandbox")
config_file = "/etc/simple-kafka-monitor/settings_{}.ini".format(env)
config = ConfigParser.ConfigParser()
config.read(config_file)
log_level_key = 'consumer_log_level'
consumer_log_level = config.get('log', log_level_key)
consumer_log_level = os.getenv(log_level_key, consumer_log_level).upper()
# or we can just use getattr() instead as commented below
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
log_location = config.get('log', 'location')
log_file = log_location + "kafka_monitor.log"

# Handles all the loggers to use across the project
km_logger = logging.getLogger("kafka.monitor.log")
#log_level = getattr(km_logger, consumer_log_level)
km_logger.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:process %(process)d:process name %(processName)s:%(funcName)s():line %(lineno)d:%(message)s')
if out_type == "file":
    #stdout_handler = logging.FileHandler(LOGCONFIG.get("locations", "kafka_monitor_log"), mode='a')
    stdout_handler = logging.FileHandler(log_file, mode='a')
else:
    stdout_handler = logging.StreamHandler(sys.stdout)

stdout_handler.setLevel(log_level)
stdout_handler.setFormatter(formatter)
km_logger.addHandler(stdout_handler)

# HTTP server loggers (both access and error) are defined in httpserver/log.py