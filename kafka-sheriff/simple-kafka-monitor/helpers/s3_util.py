import boto3
from botocore.client import ClientError
from log import *
from autologging import logged

@logged(logging.getLogger("kafka.monitor.log"))
class S3Util:
    """This class handles all the s3 related tasks"""

    @staticmethod
    def copy_to_local(source_bucket, source_key, local_path):
        """This function download a file from s3 """
        try:
            S3Util.__log.debug("S3 copy source: {}/{}".format(source_bucket, source_key))
            S3Util.__log.debug("S3 copy destination: {}".format(local_path))
            client = boto3.client('s3')
            client.download_file(source_bucket, source_key, local_path)
            S3Util.__log.debug("Successfully copied s3 files to local system")
        except Exception as e:
            #raise
            S3Util.__log.error('Error while copying file from s3: ' + str(e))
            return False
        return True