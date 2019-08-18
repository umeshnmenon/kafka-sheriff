"""
Test the Kerberos KeyTab is valid, if not download the update keytab. Download the sasl_ssl certificate for the Burrow to use
"""
from autologging import logged
import sys, time, argparse, os
import ConfigParser
import json
from log import *
from helpers.kerberos_util import KerberosUtil
from helpers.s3_util import S3Util
from helpers.utils import Utils

@logged(logging.getLogger("kafka.monitor.log"))
class Security:
    """
        This class sets up all the ssl and kerberos related needs
    """

    def __init__(self, config):
        self.__log.info("Initializing Security")
        self.config = config

    def copy_certs(self, bucket, files):
        """
        Copies the certificate
        :param bucket:
        :param files:
        :return:
        """
        try:
            for cert in files:
                src = cert['src']
                dst = cert['dst']
                self.__log.info("Copying from s3 bucket {}/{} to {}".format(bucket, src, dst))
                if S3Util.copy_to_local(bucket, src, dst):
                    self.__log.info("Copying sucessfully completed")
                else:
                    self.__log.error("Copying failed for {}".format(dst))
        except:
            self.__log.error("Could not download to {} from s3 {}/{}".format(dst, bucket, src))
            #raise
            return False
        return True

    @logged(logging.getLogger("kafka.monitor.log"))
    def copy_ssl_certs(self):
        """
        Copies ssl certificates from s3 to a location location so that other services/apps can use
        :return:
        """
        SSL = 'ssl'
        bucket_key = 'ssl_bucket'
        certs_key = 'ssl_certs'
        # Copy the security certificates from s3
        self.__log.info("Pulling SSL certs from s3")
        bucket = self.config.get(SSL, bucket_key)
        #bucket = Utils.get_env_var(bucket_key, bucket, self.__log)
        certs = self.config.get(SSL, certs_key)
        #certs = Utils.get_env_var(certs_key, certs, self.__log)
        files = json.loads(certs)
        self.__log.debug("Loaded SSL properties are: {}: {}, {}: {}".format(bucket_key, bucket, certs_key, certs))
        ret = self.copy_certs(bucket, files)
        self.__log.info("Copied certificates")
        return ret


    @logged(logging.getLogger("kafka.monitor.log"))
    def init_kerberos(self):
        """
        Authenticates using Kerberos
        :return:
        """
        KERBEROS = 'kerberos'
        principal_key = 'kerberos_principal'
        bucket_key = 'keytab_s3_bucket'
        keytab_key = 'keytab_s3_key'
        principal = self.config.get(KERBEROS, principal_key)
        #principal = Utils.get_env_var(principal_key, principal, self.__log)
        bucket = self.config.get(KERBEROS, bucket_key)
        #bucket = Utils.get_env_var(bucket_key, bucket, self.__log)
        keytab_s3_key = self.config.get(KERBEROS, keytab_key)
        #keytab_s3_key = Utils.get_env_var(keytab_key, keytab_s3_key, self.__log)
        login_retry = int(self.config.get(KERBEROS, 'login_retry'))
        login_retry_poll = int(self.config.get(KERBEROS, 'login_retry_poll'))
        keytab_path = self.config.get(KERBEROS, 'keytab_local_path')
        self.__log.debug("Loaded Kerberos properties are: {}: {}, {}: {}, {}: {}, login_retry: {}, "
                                    "login_retry_poll: {}, keytab_local_path: {}".format(principal_key, principal, bucket_key, bucket,
                                                                                   keytab_key, keytab_s3_key, login_retry,
                                                                                   login_retry_poll, keytab_path))
        self.__log.info("Checking if keytab file exists in the path {}".format(keytab_path))
        if not Utils.does_file_exist(keytab_path):
            self.__log.info("Downloading keytab file from s3")
            KerberosUtil.download_keytab(s3_bucket, s3_key, keytab_path)

        # Check if cached credentials are still valid
        self.__log.info("Check if cached credentials are still valid")
        valid_cache = KerberosUtil.validate_ticket()
        if not valid_cache:
            self.__log.info("Cached credentials are not valid. Initializing ticket using kerberos credentials.")
            auth = KerberosUtil.kinit(login_retry, login_retry_poll, principal, keytab_path,
                                      keytab_s3_key, bucket)
            if not auth:
                self.__log.error("Failed to authenticate using kerberos")
                #raise ValueError('Failed to authenticate using kerberos')
            self.__log.info("Successfully initialized kerberos ticket")
            return auth
        else:
            self.__log.info("Successfully authenticated using cached credentials")

        return valid_cache

    @logged(logging.getLogger("kafka.monitor.log"))
    def kinit_periodic_refresh(self):
        KERBEROS = 'kerberos'
        kinit_refresh_interval = self.config.get(KERBEROS, 'kinit_refresh_interval')
        kerberos_principal = self.config.get(KERBEROS, 'kerberos_principal')
        keytab_s3_bucket = self.config.get(KERBEROS, 'keytab_s3_bucket')
        keytab_s3_key = self.config.get(KERBEROS, 'keytab_s3_key')
        login_retry = int(self.config.get(KERBEROS, 'login_retry'))
        login_retry_poll = self.config.get(KERBEROS, 'login_retry_poll')
        keytab_local_path = self.config.get(KERBEROS, 'keytab_local_path')
        self.__log.debug(
            "Loaded kerberos configuration: kerberos_principal: {}, keytab_s3_bucket: {}, keytab_s3_key: {}, login_retry: {}, "
            "login_retry_poll: {}, keytab_path: {}".format(
                kerberos_principal, keytab_s3_bucket, keytab_s3_key, login_retry, login_retry_poll,
                keytab_local_path))

        while True:
            # Refreshing the Kerberos Ticket
            self.__log.info("Refreshing the Kerberos Ticket")
            if not KerberosUtil.refresh_ticket(kerberos_principal):
                # kinit -R returned error. Validating the Ticket
                self.__log.info("kinit -R returned error. Validating the Ticket")
                if not KerberosUtil.validate_ticket():
                    # Kerberos Ticket is not valid. Initializing the ticket cache using kinit
                    self.__log.info("Kerberos Ticket is not valid. Initializing the ticket cache using kinit")
                    if KerberosUtil.kinit(login_retry, login_retry_poll, kerberos_principal, keytab_local_path,
                                          keytab_s3_key, keytab_s3_bucket):
                        self.__log.info("New Ticket cache has been created")
                    else:
                        self.__log.warning("kinit returned error. Trying again after {} seconds...".format(kinit_refresh_interval))
            else:
                self.__log.info("Ticket cache has been refreshed")
            time.sleep(int(kinit_refresh_interval))

    def rename_krb5conf_file(self, env=None):
        krb5conf_file_new_name = "krb5.conf"
        krb5conf_file = "krb5_{}.conf"
        if env is None:
            env = os.getenv("env", "sandbox")

        krb5conf_file = krb5conf_file.format(env)
        conf_folder = '/etc/'
        conf_file_path_old = conf_folder + krb5conf_file
        conf_file_path_new = conf_folder + krb5conf_file_new_name
        #if Utils.does_file_exist(conf_file_path_new):
        #    self.__log.info("krb5.conf already exists. Skipping renaming..")
        #    return True
        #else:
        self.__log.info("Renaming krb5_{}.conf to krb5.conf for {} environment".format(env, env))
        if Utils.does_file_exist(conf_file_path_old):
            try:
                os.rename(conf_file_path_old, conf_file_path_new)
            except Exception as e:
                self.__log.error("Error while renaming {}: {}".format(conf_file_path_old, e))
                # raise
                return False
            else:
                self.__log.info("Renamed {} file to {}".format(conf_file_path_old, conf_file_path_new))
                return True
        else:
            self.__log.error("{} file doesn't exist".format(conf_file_path_old))

        if not Utils.does_file_exist(conf_file_path_new):
            self.__log.error("{} file doesn't exist".format(conf_file_path_new))
            return False

    def setup_security(self, env=None):
        # rename the krb.conf file
        self.rename_krb5conf_file(env)
        # do a kinit
        self.init_kerberos()
        # copy ssl certificates
        self.copy_ssl_certs()