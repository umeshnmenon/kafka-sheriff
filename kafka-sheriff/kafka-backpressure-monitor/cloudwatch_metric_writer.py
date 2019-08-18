import boto3
from log import *
from autologging import logged
from datetime import datetime


@logged(logging.getLogger("kafka.backpressure"))
class CloudWatchMetricWriter:
    """Updates the CloudWatch init script. This will update the metric to a Cloudwatch"""

    def __init__(self, region):
        self.__log.info("Instantiating CloudWatch Metric Writer")
        self.region = region
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)

    def put_metric(self, namespace, metric_name, metric_val, dimension_key, dimension_val, unit):
        """Puts the metric"""
        # sending only one dimension for the time being. TODO: change it for multiple dimensions if required
        # Put custom metrics
        gen_logger.info("Updating the CloudWatch Metric...")
        gen_logger.info(
            "Time: {3}, Metric: {0}, Name: {1}, Value: {2}".format(namespace, metric_name, metric_val, time.time()))
        response = None
        try:
            response = self.cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=[
                    {
                        'MetricName': metric_name,
                        'Dimensions': [
                            {
                                'Name': dimension_key,
                                'Value': dimension_val
                            },
                        ],
                        'Value': metric_val,
                        'Unit': unit,
                        'StorageResolution': 1
                    },
                ])
        except Exception as e:
            gen_logger.error("Error while getting the put metrics. Error is {}".format(str(e)))

        gen_logger.info("CloudWatch Metric successfully updated")
        gen_logger.debug("Response: {0}".format(response))

    # def put_metric(self, namespace, metrics, dimension_key, dimension_val, unit):
    #    self.cloudwatch.put_metric_data(namespace, metrics.keys(), metrics.values(),
    #                               dimensions={dimension_key:dimension_val, }, unit=unit)

    #   put-metric-data
    #   --namespace <value>
    #   [--metric-data <value>]
    #   [--metric-name <value>]
    #   [--timestamp <value>]
    #   [--unit <value>]
    #   [--value <value>]
    #   [--dimensions <value>]
    #   [--statistic-values <value>]
    #   [--cli-input-json <value>]
    #   [--generate-cli-skeleton <value>]
