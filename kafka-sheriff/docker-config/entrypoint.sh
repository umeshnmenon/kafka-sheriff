#!/bin/sh
set -e

echo "Starting Simple Kafka Monitor..."
# variable to hold the status
rc=1
python /etc/simple-kafka-monitor/kafka_monitor.py &
rc=$?
if [ $? -eq 1 ]; then
	echo "Kafka Monitor has some problems while starting. Aborting..."
	exit 1
fi
echo "Done"
# a delay to makre sure http server has started before the backpressure monitor makes a request
sleep 10
echo "Starting Backpressure monitor..."
cd /etc/kafka-backpressure-monitor/
rc=1
python /etc/kafka-backpressure-monitor/lag_monitor.py
rc=$?
if [ $? -eq 1 ]; then
	echo "Kafka Backpressure Monitor has some problems while starting. Aborting..."
	exit 1
fi
echo "Done"

# comment the below for production
#/bin/bash