from threading import Thread
from log import *
from autologging import logged
import threading
import time

@logged(logging.getLogger("kafka.monitor.log"))
class MultiThread:
    """
    A simple class to execute multiple threads in parallel
    """
    def __init__(self, threads=None, strict=True, callback=None):
        """

        :param threads: Threads to be executed
        :param strict: If True, then exception is raised in case of an error. If False, then it continues just by logging the error
        :param callback: a callback function can be executed with exception as the argument so that we can do anything with the exception
                        because otherwise we will not be able to come out of thread execution
        """
        self.__log.info("Initializing multithread class")
        self.threads = []
        if threads is not None:
            for thread in threads:
                self.__log.info("Adding threads to execution list. Add: {}".format(thread.getName()))
                if not thread is None:
                    self.threads.append(thread)
                else:
                    self.__log.warning("Thread {} is None".format(thread.getName()))
        self.aggregate_exception = [] # a list to store the exceptions from the individual thread runs
        self.strict = strict # if True then throws the exception and halts the execution
        self.callback = callback

    def start(self):
        """
                Runs each thread. If any fails, the error is added to the exception list and the thread number is noted. The join
                will be called for only sucessful threads
                :return:
        """

        for thread in self.threads:
            try:
                self.__log.info("Starting thread: {}".format(thread.getName()))
                thread.start()
            except Exception, e:
                mte = MultiThreadException(thread.getName(), e)
                self.__log.error("Exception in thread start {}. Error: {}".format(thread.getName(), str(e)))
                self.aggregate_exception.append(mte)
                if self.callback:
                    self.callback(mte)
                if self.strict:
                    raise mte
                continue

    def join(self):
        """
                Runs each thread. If any fails, the error is added to the exception list and the thread number is noted. The join
                will be called for only sucessful threads
                :return:
        """

        for thread in self.threads:
            try:
                self.__log.info("Join thread: {}".format(thread.getName()))
                thread.join()
            except Exception, e:
                mte = MultiThreadException(thread.getName(), e)
                self.__log.error("Exception in thread join {}. Error: {}".format(thread.getName(), str(e)))
                self.aggregate_exception.append(mte)
                if self.callback:
                    self.callback(mte)
                if self.strict:
                    raise mte
                continue

    def run(self):
        """
        Runs each thread. If any fails, the error is added to the exception list and the thread number is noted. The join
        will be called for only sucessful threads
        :return:
        """
        self.__log.info("Starting threads one by one from the list....")
        self.start()
        self.__log.info("Starting threads completed.")

        self.__log.info("Checking if all the threads given in the list have started....")
        if len(self.aggregate_exception) > 0:
        #if not self.is_all_alive():
            self.__log.warning("Not all the threads in the execution list have started. Please check the preceding logs.")

        time.sleep(10)

        self.__log.info("Call thread.join one by one from the list....")
        self.join()
        self.__log.info("Call thread.join completed.")

    def print_errors(self):
        if len(self.aggregate_exception) > 0:
            for mte in self.aggregate_exception:
                self.__log.error("Exception in thread {}. Error: {}".format(mte.getThreadName(), mte.getException()))

    def get_exceptions(self):
        return self.aggregate_exception

    def is_all_alive(self):
        actives = threading.activeCount()
        given = len(self.threads)
        self.__log.debug("Given threads: {}".format(given))
        self.__log.debug("Active threads: {}".format(actives))
        if actives == given + 1: #1 for main thread
            return True
        else:
            return False

    def append(self, thread):
        self.__log.info("Adding threads to execution list. Add: {}".format(thread.getName()))
        self.threads.append(thread)

class MultiThreadException(Exception):
    """
    Handles exceptions for multi-threads
    """
    # def __init__(self, name, e=None, inner_e=None, *args, **kwargs):
    #     self.name = name
    #     self.e = e
    #     self.inner_e = inner_e
    #     Exception.__init__(self, *args, **kwargs)

    def __init__(self, name, e=None, inner_e=None):
        self.name = name
        self.e = e
        self.inner_e = inner_e

    def __str__(self):
        return repr(self.e)

    def getException(self):
        return self.e

    def getInnerException(self):
        return self.inner_e

    def getThreadName(self):
        return self.name