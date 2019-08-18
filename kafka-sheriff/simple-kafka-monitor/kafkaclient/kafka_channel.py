from autologging import logged
import logging, time
from threading import Thread
from kafkaclient.consumer_monitor import ConsumerMonitor

@logged(logging.getLogger("kafka.monitor.log"))
class KafkaChannel(Thread):
    """
    Sets up all the communiation channels to Kafka
    """
    def __init__(self, config, *args):
        self.__log.info("Instantiating Kafka Channel thread")
        Thread.__init__(self)
        THREAD_NAME = 'kafka-channel'
        self.setName(THREAD_NAME)
        self.setDaemon(True)
        self.config = config

    def run(self):
        self.__log.info("Staring Kafka Channel")
        self.__log.info("Starting Kafka Monitors in seperate threads....")
        monitors = []
        try:
            monitors.append(ConsumerMonitor(self.config))
        except Exception as e:
            self.__log.error("Error while instantiating ConsumerMonitor. Error: {}".format(str(e)))
            return False

        for i in range(0, len(monitors)):
            monitor = monitors[i]
            err = monitor.start()
            if err:
                # if erred, stop the monitors in the reverse order
                for j in range(len(monitors), i):
                    prev_monitor = monitors[j]
                    prev_monitor.stop()
                return False # return a False so that the main thread will exit

        time.sleep(10)

        for monitor in monitors:
             monitor.join()