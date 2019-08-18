# kafka-sheriff

KafkaSheriff a.k.a Simple Kafka Monitor is a simple monitoring tool for keeping track of consumer lag in Apache Kafka. It is designed to monitor a specified consumer group that is committing offsets to Kafka and to monitor the topic and partition consumed by the group.

It also provides HTTP request end points for getting information about the topics and consumer group. This can be useful for other applications and purposes where connecting to Kafka cluster is not possible.

This work is largely inspired from LinkedIn Burrow but this is written in Python and has some additional features such 
as option to store the metrics in either an inbuilt cache or to an external Redis cache, offers Kerberos based security 
also option to store the total lag in the consumer group using set of Amazon CloudWatch metrics and alarms to use for 
auto scaling purpose, etc.. 

### Features

KafkaSheriff is primarily designed to monitor only inbound and outbound topic partition status. The topic and 
group names are configurable. It provides the broker and consumer offsets, current lag for each partition and total lag.
It is also designed to fetch the consumer group status for all the consumer groups without having to specify the group 
and topic names. The new version also has an api end point that provides the consumer group status over a sliding window. 
A set of following rules is used to calculate the status of the consumer group:

1. If any lag within the window is zero, the status is considered to be OK.
2. If the consumer offset does not change over the window, and the lag is either fixed or increasing, the consumer is in an ERROR state, and the partition is marked as STALLED.
3. If the consumer offsets are increasing over the window, but the lag either stays the same or increases between every pair of offsets, the consumer is in a WARNING state. This means that the consumer is slow, and is falling behind.
4. If the difference between the time now and the time of the most recent offset is greater than the difference between the most recent offset and the oldest offset in the window, the consumer is in an ERROR state and the partition is marked as STOPPED. However, if the consumer offset and the current broker offset for the partition are equal, the partition is not considered to be in error.
5. If we don not have the offset yet for the partition, the status is considered to be OK.
      
**Auto Scaling**

The primary requirement is to auto scale the python kafka consumers, we are mainly interested in the lag of the consumer group for the inbound topic. We get the number of partitions for the inbound topic and calculate the ‘current lag’ in each partition of the consumer group. The current lag is calculated by the following formula:
```
 current lag = broker offset - committed offset
```
The broker offset is the HEAD offset of the partition and committed offset is the offset that the consumer consumed most recently. We calculate the ‘current lag’ for each partition in the group and add them up together to get the ‘total lag’ for the group.

We then use a simple threshold based approach to scale up and down the consumers using this total lag. For e.g. if total_lag > 50, add two consumers and if total_lag < 50, remove two consumers. 

### Solution Architecture

![High Level Design of Simple Kafka Monitor Service](images/kafka-monitor-arch.png?raw=true "High Level Design of Simple Kafka Monitor Service")

There are mainly two components, Simple Kafka Monitor and Back Pressure Monitor. 

**Simple Kafka Monitor** has a modular design that separates out the works to multiple components:

- **Kafka Client** module periodically fetches the broker offset and consumer offset, calculates the lag and stores the information to Storage
- **Storage module** can either be Internal Cache Server or be a Redis Server. This stores all of this information
- **Internal Cache Server** is a simple built in cache which can be used to store the info if Redis server is not available. This is an optional configuration.
- **HTTP Server module** provides an API interface to Simple Kafka Monitor for fetching information about consumer group and topics

**Back Pressure Monitor** is a client in Simple Kafka Monitor which periodically polls the total lag information for a consumer group and topic and pushes the value to a custom CloudWatch Metric named ‘TotalLag’.

### How it Works?
KafkaSheriff will run behind the scenes and stores the metrics in the cache. The BackPressure Monitor fetches the required
information from these metrics and return to the consumer in response to HTTP api calls.

**To use in Auto Scaling**

- The KafkaSheriff periodically gets the Total Lag for the given consumer group (inbound topic group in our case)
- KafkaSheriff then updates a custom CloudWatch Metric named TotalLag at specified time interval.
- A CloudWatch alarm is set up to alert if the TotalLag metric is above a threshold.
- Assuming your Kafka consumer is deployed as a Docker container and is registered in AWS ECS for the container 
management, add a policy in the Service definition of ECS to scale up and down the containers when the custom metric TotalLag breaches a threshold.

### Configuration

KafkaSheriff configuration follows INI format. The configuration is organized into several subheadings. The configuration 
file is in the root folder of the KafkaSheriff distribution. The config file follows the naming convention ‘settings_<<env>>.ini’ 
where env should be replaced by the environment we would plan to deploy. For e.g. sandbox, stage, prod, etc..

**Cloudwatch Metric Configuration**

KafkaSheriff updates the custom metric ‘TotalLag’ using ‘put_metric_data’ function every 10 seconds (configurable).

The Unit of ‘TotalLag’ is 'Count' and StorageResolution is 1 (1 second = high resolution)

The evaluation period is currently set to 1 min (1 out of 1 data point with a period of 1 minute), so the evaluation interval is 1 minute.


### HTTP API End Points

The HTTP Server in KafkaSheriff provides a convenient way to interact with both Kafka Monitor and Kafka clusters. The 
Requests are simple HTTP calls and all the responses are in JSON format. Appropriate error codes are returned in case of 
an error and for application specific errors, a detailed explanation of the error is provided in the JSON message.

In the current version following end points are available:

| Request | URL | Description |
| ------------- | ------------- | ------------- |
| Healthcheck | GET /bpmon/healthcheck | Healthcheck of KafkaSheriff |
| List topics | GET /bpmon/consumergroup/<<group_id>>/topics/ | List of topics that the consumer group consumes |
| Consumer Group Status | GET /bpmon/consumergroup/<<group_id>>/topics/<<topic>> | Current status of the consumer group based on the evaluation of all partitions that it consumes. It returns the current consumer offset (committed), broker offset, current lag for each partition and the total current lag for the group. |

### How to run

KafkaSheriff is implemented as a Docker container. If you are using AWS, you can use ECR registry to register your 
container. We can use the standard Docker run command to run the container.

Example run command:

Minimal Run
```sh
sudo docker run --rm -it <<docker file>>
```

With all arguments
```sh
sudo docker run –e env=sandbox –e consumer_log_level=DEBUG --rm -it <<docker file>>
```

