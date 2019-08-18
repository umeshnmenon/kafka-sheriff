from kafka import KafkaConsumer, KafkaProducer
from autologging import logged  # , traced
import time, sys, logging, json, ssl
from helpers.kerberos_util import KerberosUtil
from helpers.s3_util import S3Util
from helpers.utils import Utils
from gssapi.raw.misc import GSSError # to catch the kerberos/ssl related error

@logged(logging.getLogger("kafka.monitor.log"))
class Kafka_Connection(object):
    """
    Establishes connection to Kafka
    """
    def __init__(self, config, group_id = None, topic = None):
        self.__log.info("Starting up Kafka Connection")
        self._set_vars(config, group_id, topic)

    def _set_vars(self, config, group_id = None, topic = None):
        self.__log.info("Validating Kafka Configuration File")
        KEY = "kafka"
        # TODO put in test for checking to see if it is a list
        self.bootstrap_servers = config.get(KEY, "bootstrap_servers")
        if not self.bootstrap_servers:
            raise ValueError('Please provide a valid list of servers.')
        #self.__log.info("Bootstrap Servers are configured as type {0}".format(
        #    type(json.loads(self.bootstrap_servers))))
        self.group_id = group_id if group_id is not None else config.get(KEY, "group_id")
        self.topic = topic if topic is not None else config.get(KEY, "topic")
        self.security_protocol = config.get(KEY, "protocol")
        self.auto_offset_reset = config.get(KEY, "auto_offset_reset")
        # SSL related configs
        self.ssl_cafile = config.get(KEY, "ssl_cafile")
        self.ssl_certfile = config.get(KEY, "ssl_certfile")
        self.ssl_keyfile = config.get(KEY, "ssl_keyfile")
        self.ssl_password = config.get(KEY, "ssl_password")
        self.sasl_mechanism = config.get(KEY, "sasl_mechanism")
        self.keytab_retry_limit = int(config.get(KEY, 'keytab_retry_limit'))
        self.keytab_retry_interval = int(config.get(KEY, 'keytab_retry_interval'))

    def do_ssl_certs_exist(self):
        """
        Checks if ssl related files exist in the system
        :return:
        """
        ssl_files = [self.ssl_cafile, self.ssl_certfile, self.ssl_keyfile]
        return Utils.do_files_exist(ssl_files, self.__log)

@logged(logging.getLogger("kafka.monitor.log"))
class Kafka_Consumer(Kafka_Connection):
    def __init__(self, config):
        self.__log.info("Instantiating Kafka Consumer")
        super(Kafka_Consumer, self).__init__(config)
        self.__log.info("Kafka Consumer instantiated")

    def connect(self):
        self.__log.info("Kafka Consumer Connecting to Kafka Topic")

        if self.security_protocol == "PLAINTEXT":
            #return KafkaConsumer(self.inbound_topic, group_id=self.group_id,
            #                     bootstrap_servers=json.loads(self.bootstrap_servers))
            return KafkaConsumer(group_id=self.group_id, bootstrap_servers=json.loads(self.bootstrap_servers),
                                 auto_offset_reset=self.auto_offset_reset, enable_auto_commit=False)
        elif self.security_protocol == "SASL_SSL":
            self.__log.info("Connecting over sasl_ssl")
            # check if ssl certificates exist
            if not self.do_ssl_certs_exist():
                self.__log.error("Required ssl certficate files do not exist.")
                raise ValueError("Required ssl certficate files do not exist.")
            # create a new context using system defaults, disable all but TLS1.2
            context = ssl.create_default_context()
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(self.ssl_cafile)
            context.load_cert_chain(certfile=self.ssl_certfile, keyfile=self.ssl_keyfile)
            context.options &= ssl.OP_NO_TLSv1
            context.options &= ssl.OP_NO_TLSv1_1

            return KafkaConsumer(group_id=self.group_id, bootstrap_servers=json.loads(self.bootstrap_servers),
                                 auto_offset_reset=self.auto_offset_reset,
                                 # consumer_timeout_ms=self.consumer_timeout_ms,
                                 security_protocol=self.security_protocol,
                                 sasl_mechanism=self.sasl_mechanism,
                                 ssl_context=context,
                                 api_version=(0, 10),
                                 enable_auto_commit=False
                                 ) # ssl_certfile=ssl_certfile, ssl_keyfile=ssl_keyfile, ssl_password=ssl_password


    def refresh(self):
        """
        This function only tries to reconnect in case of a GSSError. There's another thread that runs to do the kinit
        periodically
        :return:
        """
        keytab_retry_count = 1
        while keytab_retry_count < keytab_retry_limit:
            try:
                self.__log.info("Trying to connect. Attempt: {}".format(keytab_retry_count))
                return self.connect()
            except GSSError as e:  # this means we know that it is kerberos/ssl related issue, so retry again
                keytab_retry_count += 1
                time.sleep(keytab_retry_interval)
                if keytab_retry_count >= keytab_retry_limit:
                    self.__log.error("Error is : {}".format(str(e)))
                    self.__log.error(
                        "Connection not succeeded after {} times. Exiting...".format(keytab_retry_count))
                    break
            except Exception as e1:
                self.__log.error("There's some problem while establishing a connection to Kafka. Error: {}".format(str(e1)))
                pass
            #else:  # log and pass
                #pass


@logged(logging.getLogger("kafka.monitor.log"))
class Kafka_Producer(Kafka_Connection):
    def __init__(self, config):
        super(Kafka_Producer, self).__init__(config)


    def connect(self):
        self.__log.info("Kafka Producer Connecting to Kafka Topic")
        if self.security_protocol == "PLAINTEXT":
            self.broker = KafkaProducer(bootstrap_servers=json.loads(self.bootstrap_servers), retries=5)
            self.__log.info("Status of the connection is {0}".format(self.broker.metrics()))
        elif self.security_protocol == "SASL_SSL":
            self.__log.info("Connecting over sasl_ssl")
            # SSL
            # create a new context using system defaults, disable all but TLS1.2
            context = ssl.create_default_context()
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(self.ssl_cafile)
            context.load_cert_chain(certfile=self.ssl_certfile, keyfile=self.ssl_keyfile)
            context.options &= ssl.OP_NO_TLSv1
            context.options &= ssl.OP_NO_TLSv1_1

            self.broker = KafkaProducer(bootstrap_servers=json.loads(self.bootstrap_servers),
                                        # consumer_timeout_ms=self.consumer_timeout_ms,
                                        security_protocol=self.security_protocol,
                                        sasl_mechanism=self.sasl_mechanism,
                                        ssl_context=context,
                                        api_version=(0, 10)
                                        )

    def send(self, msg):
        self.__log.info("Kafka Producer Send Function")
        self.broker.send(self.outbound_topic, b"{0}".format(msg))
        return 1

    def close(self):
        self.broker.close()