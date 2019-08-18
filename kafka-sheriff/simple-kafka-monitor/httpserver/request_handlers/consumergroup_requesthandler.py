from storage.storage import Storage
from urlparse import urlparse, parse_qs
from helpers.utils import Utils
import logging, json
from autologging import logged
from evaluator.consumergroup_evaluator import ConsumerGroupEvaluator
from generic_requesthandler import GenericRequestHandler
@logged(logging.getLogger("kafka.monitor.log"))
class ConsumergroupRequestHandler(GenericRequestHandler):
    """
    Takes care all the request handling for the ConsumerGroup
    """

    def get_status(self, args):
        self.__log.info("Executing get_status")
        response = ConsumerGroupEvaluator(self.config).get_status(args['group_id'], args['topic'])
        return json.dumps(response)

    def get_total_lag(self, args):
        self.__log.info("Executing get_total_lag")
        response = ConsumerGroupEvaluator(self.config).get_total_lag(args['group_id'], args['topic'])
        return json.dumps(response)

    def get_topics(self, args):
        self.__log.info("Executing get_topics")
        response = ConsumerGroupEvaluator(self.config).get_topics(args['group_id'])
        return json.dumps(response)
