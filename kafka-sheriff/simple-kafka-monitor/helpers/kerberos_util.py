import subprocess
import time
from autologging import logged
from s3_util import S3Util
from log import *

@logged(logging.getLogger("kafka.monitor.log"))
class KerberosUtil:
    @staticmethod
    def kinit(retry_count, retry_poll, principal, keytabs_path, s3_key=None, s3_bucket=None):
        """
        Retry_poll and retry_count can not be less than 1
        set s3_bucket and s3_key values if you want to pull the latest keytab from s3 in the event of a failure
        """

        try:
            retry = 0
            while retry < retry_count:
                KerberosUtil.__log.info("Executing kinit with following arguments")
                KerberosUtil.__log.debug("keytabs_path: {}, principal: {}".format(keytabs_path, principal))
                sh = subprocess.Popen(['kinit', '-kt', keytabs_path, principal],
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)
                stderr_result = sh.stderr.readlines()
                stdout_result = sh.stdout.readlines()
                if stderr_result != []:
                    for error in stderr_result:
                        #print(error)
                        KerberosUtil.__log.error(error)
                        KerberosUtil.download_keytab(s3_bucket, s3_key, keytabs_path)
                    retry += 1
                    if retry != retry_poll:
                        time.sleep(float(retry_poll))
                else:
                    KerberosUtil.__log.info("Done kinit successfully")
                    return True
            return False
        except Exception, e:
            KerberosUtil.__log.error('Error in kinit :' + str(e))
            raise

    @staticmethod
    def validate_ticket():
        try:
            KerberosUtil.__log.info("Executing klist")
            status = subprocess.check_output('klist -s | echo $?', shell=True)
            if status ==  0:
                KerberosUtil.__log.info("klist returned results")
                return True
            else:
                KerberosUtil.__log.info("klist returned an error")
                return False
        except Exception, e:
            KerberosUtil.__log.error('Error in klist :' + str(e))
            raise

    @staticmethod
    def refresh_ticket(principal):
        try:
            KerberosUtil.__log.info("Executing kinit refresh")
            sh = subprocess.Popen(['kinit', '-R'],
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            stderr_result = sh.stderr.readlines()
            stdout_result = sh.stdout.readlines()
            if stderr_result:
                KerberosUtil.__log.info("kinit refresh erred")
                return False
            return True
        except Exception, e:
            KerberosUtil.__log.error('Error in kinit while refreshing the ticket :' + str(e))
            raise

    @staticmethod
    def download_keytab(s3_bucket, s3_key, keytab_path):
        if s3_bucket != None and s3_key != None:
            KerberosUtil.__log.info("Copying keytab from s3 to local")
            KerberosUtil.__log.debug("s3_bucket: {}, s3_key: {}, local_keytab_path: {}".format(
                s3_bucket, s3_key, keytab_path))
            if S3Util.copy_to_local(s3_bucket, s3_key, keytab_path):
                KerberosUtil.__log.info("Keytab downloaded from s3")
                return True
            else:
                KerberosUtil.__log.error("Failed to download Keytab from s3")
                return False