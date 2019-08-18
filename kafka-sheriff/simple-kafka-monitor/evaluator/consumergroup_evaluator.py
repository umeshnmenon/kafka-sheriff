from storage.storage import Storage
from autologging import logged
import logging, time
from collections import OrderedDict
from storage.fixed_size_dict import FixedSizeDict
from storage_coordinators.consumergroup_storage_coordinator import ConsumerGroupStorageCoordinator

@logged(logging.getLogger("kafka.monitor.log"))
class ConsumerGroupEvaluator():
    """
    This class fetches the information from Storage based on the query given
    """
    def __init__(self, config):
        self.config = config
        self.storage_coordinator = ConsumerGroupStorageCoordinator(self.config)

    def get_status(self, group_id, topic):
        # Connect to Storage
        #cache = self.get_storage()
        # first get the number of partitions to loop through
        #partitions_key = "{}-{}-partitions".format(group_id, topic)
        #num_partitions = int(cache.get(partitions_key))
        num_partitions = int(self.storage_coordinator.get_partitions_count(group_id, topic))
        self.__log.debug("Number of partitions for topic: {}".format(num_partitions))
        complete_partitions = 0
        partitions_status = {}
        partition_status = {}
        partitions_status["error"] = ""
        for p in range(0, num_partitions - 1):
            # first get the committed offsets
            #committed_key = "{}-{}-{}-committed".format(group_id, topic, p)
            #offsets = self.cast_as_ordered_dict_from_cache(committed_key, cache)
            offsets = self.storage_coordinator.get_committed(group_id, topic, p)
            offsets = self.cast_as_ordered_dict(offsets)
            self.__log.debug("Offsets: {}".format(offsets))
            #lags_key = "{}-{}-{}-lags".format(group_id, topic, p)
            #lags = self.cast_as_ordered_dict_from_cache(lags_key, cache)
            lags = self.storage_coordinator.get_currentlag(group_id, topic, p)
            lags = self.cast_as_ordered_dict(lags)
            self.__log.debug("Lags: {}".format(lags))
            #headoffsets_key = "{}-{}-{}-headoffsets".format(group_id, topic, p)
            #headoffsets = self.cast_as_ordered_dict_from_cache(headoffsets_key, cache)
            headoffsets = self.storage_coordinator.get_headoffset(group_id, topic, p)
            headoffsets = self.cast_as_ordered_dict(headoffsets)
            self.__log.debug("HEADOffsets: {}".format(headoffsets))
            partition = {"offsets": offsets, "lags": lags, "headoffsets": headoffsets}
            status = self.evaluate_partition_status(partition)
            if status.status == ConsumerStatusOK().status:
                complete_partitions = complete_partitions + 1
            partition_status[p] = {"status": status.status, "committed": self.get_current_offset(partition),
            "headoffset": self.get_current_headoffset(partition), "current_lag": self.get_current_lag(partition)}
        partitions_status["partitions"] = partition_status
        partitions_status["status_complete"] = float(complete_partitions) * 100 / float(num_partitions)
        self.__log.debug(partitions_status)
        return partitions_status

    def get_storage(self):
        """
        Connects to a storage
        :return:
        """
        storage_type = self.config.get('cache', 'type')
        self.__log.info("Creating the storage cache of type {}".format(storage_type))
        cache = Storage(storage_type, self.config).cache
        self.__log.info("Connected to cache")
        return cache

    def cast_as_ordered_dict(self, val):
        self.__log.info("Casting the values as a sorted dictionary")
        limit = self.config.get('cache', 'sliding_window')
        fs_dict = FixedSizeDict(size=limit)
        if val is not None:
            fs_dict = fs_dict.from_json(val)
        sorted_dict = fs_dict.cast_to_ordered_dict()
        self.__log.debug(sorted_dict)
        return sorted_dict
    #
    # def get_status(self):
    #     # Connect to Storage
    #     storage_type = self.config.get('cache', 'type')
    #     self.__log.info("Creating the storage cache of type {}".format(storage_type))
    #     self.cache = Storage(storage_type, self.config).cache
    #     self.__log.info("Connected to cache")
    #     key = "{}-{}-window-offsets".format(group_id, topic)
    #     val = self.cache.get(key)
    #     dict = FixedSizeDict(size=10)
    #     self.__log.debug("Value is {}: ".format(val))
    #     if val is not None:
    #         dict = dict.from_json(val)
    #
    #     partitions = {}
    #     stats = []
    #     for key in dict:
    #         ts = key
    #         val = dict[key]
    #         for stat in val:
    #             partition = dict[stat]["partition"]
    #             stats = partitions[partition]
    #             stats.append({"timestamp": ts, "committed": dict[val]["committed"], "current_lag": dict[val]["current_lag"]})
    #             partitions[partition] = stats
    #
    #     self.__log.debug(partitions)
    # def cast_as_ordered_dict_from_cache(self, key, cache):
    #     self.__log.info("Casting the values as a sorted dictionary from cache")
    #     limit = self.config.get('cache', 'sliding_window')
    #     vals = cache.get(key)
    #     dict_vals = FixedSizeDict(size=limit)
    #     if vals is not None:
    #         dict_vals = dict_vals.from_json(vals)
    #     sorted_vals = OrderedDict(sorted(dict_vals.items()))
    #     self.__log.debug(sorted_vals)
    #     return sorted_vals

    def evaluate_partition_status(self, partition):
        """
        Run all the rules
        :param partition:
        :return:
        """
        self.__log.info("Evaluating Partition status")
        self.__log.debug(len(partition.get("offsets")))
        if (len(partition.get("offsets")) == 0) or (len(partition.get("lags")) == 0) \
                or (len(partition.get("headoffsets")) == 0):
            self.__log.info("Length of any is zero")
            return ConsumerStatusOK()
        self.__log.info("Starts checking Rules")
        if self.get_current_lag(partition) > 0:
            if self.check_if_offsets_stopped(partition) and (not self.check_if_recent_lag_zero(partition)):
                return ConsumerStatusStop()
            if self.is_lag_always_not_zero(partition):
                if self.check_if_offsets_rewind(partition):
                    return ConsumerStatusRewind()
                if self.check_if_offsets_stalled(partition):
                    return ConsumerStatusStall()
                if self.check_if_lag_not_decreasing(partition):
                    return ConsumerStatusWarning()
        self.__log.info("Completed checking Rules")
        return ConsumerStatusOK()

    def cast_to_list(self, dict):
        lst = list(dict.values())
        return lst

    def get_last_item_from_list(self, dict):
        # the latest timestamp is the last element of the list
        lst = self.cast_to_list(dict)
        if (len(lst) == 0):
            return None
        return lst[len(lst) - 1]

    def get_current_lag(self, partition):
        self.__log.info("Getting current lag")
        # the latest timestamp is the last element of the list
        current_lag = self.get_last_item_from_list(partition.get("lags"))
        if current_lag is None:
            current_lag = 0
        return int(current_lag)

    def get_current_offset(self, partition):
        self.__log.info("Getting current offset")
        # the latest timestamp is the last element of the list
        current_offset = self.get_last_item_from_list(partition.get("offsets"))
        if current_offset is None:
            current_offset = 0
        return int(current_offset)

    def get_current_headoffset(self, partition):
        self.__log.info("Getting current head offset")
        # the latest timestamp is the last element of the list
        current_headoffset = self.get_last_item_from_list(partition.get("headoffsets"))
        if current_headoffset is None:
            current_headoffset = 0
        return int(current_headoffset)

    def check_if_current_lag_is_zero(self, partition):
        self.__log.info("Checking if the current lag is zero")
        # the latest timestamp is the last element of the list
        lags = self.cast_to_list(partition.get("lags"))
        if len(lags) == 0:
            return False
        if int(lags[len(lags) - 1]) > 0:
            return True
        else:
            return False

    def is_lag_always_not_zero(self, partition):
        self.__log.info("Checking if the lag is always not zero")
        lags = self.cast_to_list(partition.get("lags"))
        if len(lags) == 0:
            return True
        for i in range(0, len(lags)):
            if lags[i] == 0:
                return False
        return True

    def check_if_offsets_rewind(self, partition):
        self.__log.info("Checking if offsets rewind")
        offsets = self.cast_to_list(partition.get("offsets"))
        if len(offsets) > 0:
            for i in range(0, len(offsets)):
                if (int(offsets[i]) < int(offsets[i - 1])):
                    return True

        return False

    def check_if_offsets_stopped(self, partition):
        self.__log.info("Checking if offsets stopped")
        offsets = partition.get("offsets")
        if len(offsets) == 0:
            return True
        first_timestamp = float(offsets.keys()[0])
        last_timestamp = float(offsets.keys()[len(offsets) - 1])
        time_now = time.time()
        return ((time_now * 1000) - last_timestamp) > (last_timestamp - first_timestamp)

    def check_if_offsets_stalled(self, partition):
        self.__log.info("Checking if offsets stalled")
        offsets = self.cast_to_list(partition.get("offsets"))
        if len(offsets) > 0:
            for i in range(0, len(offsets)):
                if (int(offsets[i]) != int(offsets[i - 1])):
                    return False
        return True

    def check_if_lag_not_decreasing(self, partition):
        self.__log.info("Checking if lag is not decreasing")
        lags = self.cast_to_list(partition.get("lags"))
        for i in range(0, len(lags)):
            if (int(lags[i]) < int(lags[i - 1])):
                return False

        return True

    def check_if_recent_lag_zero(self, partition):
        self.__log.info("Checking if recent lag is zero")
        offsets = self.cast_to_list(partition.get("offsets"))
        last_offset = int(offsets[len(offsets) - 1])
        broker_offsets = self.cast_to_list(partition.get("headoffsets"))
        for i in range(0, len(broker_offsets)):
            if (int(broker_offsets[i]) <= last_offset):
                return True

        return False

    def get_total_lag(self, group_id, topic):
        num_partitions = self.storage_coordinator.get_partitions_count(group_id, topic)
        self.__log.debug("Number of partitions for topic {}: {}".format(topic, num_partitions))
        resp = {}
        resp["total_lag"] = 0
        resp["error"] = ""
        if num_partitions is None:
            resp["error"] = "No partitions found for topic {}. Either partitions are not found in Kafka or partition " \
                            "count is not saved in cache".format("topic")

        else:
            num_partitions = int(num_partitions)
            total_lag = 0
            for p in range(0, num_partitions - 1):
                current_lag = self.get_current_lag_for_partition(group_id, topic, p)
                total_lag = total_lag + current_lag
            resp["total_lag"] = total_lag
        self.__log.debug("Response from get_total_lag {}".format(resp))
        #return total_lag
        return resp

    def get_current_lag_for_partition(self, group_id, topic, partition):
        lags = self.storage_coordinator.get_currentlag(group_id, topic, partition)
        lags = self.cast_as_ordered_dict(lags)
        current_lag = self.get_last_item_from_list(lags)
        self.__log.debug("current_lag for partition{}: {}".format(partition, current_lag))
        if current_lag is None:
            current_lag = 0
        return int(current_lag)

    def get_topics(self, group_id, topic):
        topics = self.storage_coordinator.get_topics(group_id, topic)
        self.__log.debug("Number of topics: {}".format(len(topics)))
        topic_lst = []
        for tp in topics:
            num_partitions = self.storage_coordinator.get_partitions_count(group_id, tp)
            if num_partitions is None or num_partitions == 0:
                self.__log.error("No partitions found for topic {}".format(tp))
                continue
            self.__log.debug("Number of partitions for topic {}: {}".format(tp, num_partitions))
            topic_stats = {"topic": tp, "partitions": num_partitions}
            topic_lst.append(topic_stats)

        response = {"topics": topic_lst}
        #return json.dumps(response)
        return response


class ConsumerStatus():
    """Base class for all the Consumer Status"""
    ## Set the code, description, explanation (detail) in the sub classes
    # default set the code and description
    code = 200
    status = 'OK'
    description = 'Consumer is healthy'

    def __init__(self, status=None, description=None, json_formatter=None):
        # Only status, description can be customized not code
        if status is not None:
            self.status = status
        if description is not None:
            self.description = description
        # Use a different json_formatter if you wish to return a custom json other than the default one
        if json_formatter is not None:
            self._json_formatter = json_formatter

    def __str__(self):
        # detail =  '%s %s %s' % (self.code, self.description, self.details)
        # return str(detail)
        return (self.prepare())

    def _json_formatter(self, code, status, description):
        return {'code': code,
                'status': status,
                'description': description
                }

    def prepare(self):
        resp_json = self._json_formatter(self.code, self.status, self.description)
        return json.dumps(resp_json)

    def __call__(self, *args, **kwargs):
        return self.prepare()

class ConsumerStatusOK(ConsumerStatus):
    code = 200
    status = 'OK'

class ConsumerStatusNotFound(ConsumerStatus):
    code = 404
    status = 'Not Found'

class ConsumerStatusWarning(ConsumerStatus):
    code = 300
    status = 'Warning'

class ConsumerStatusError(ConsumerStatus):
    code = 500
    status = 'Error'

class ConsumerStatusStop(ConsumerStatus):
    code = 501
    status = 'Stopped'

class ConsumerStatusStall(ConsumerStatus):
    code = 502
    status = 'Stalled'

class ConsumerStatusRewind(ConsumerStatus):
    code = 503
    status = 'Rewind'