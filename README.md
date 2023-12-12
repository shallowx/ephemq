# Architecture

Meteor is a distributed messaging and streaming platform on based memory with low latency, high performance and
reliability, trillion-level capacity and flexible scalability

![image](https://github.com/shallowx/meteor/blob/main/doc/image/infra.png)

- Dotted lines are optional

# Example
- For better demonstration of the running effect, set the time interval to 1 second

- To demonstrate the effect, the setting of sending one message per second was implemented. However, this setting can be
omitted when deploying it in practice.
![image](https://github.com/shallowx/meteor/blob/main/doc/image/example.gif)

## Latency

![image](https://github.com/shallowx/meteor/blob/main/doc/image/partition.png)

## Message Protocol

![image](https://github.com/shallowx/meteor/blob/main/doc/image/message.png)

# Environment

- Zookeeper 3.5.x or higher
- Java 17
- -Dio.prometheus.client.export-protobuf.use-direct-buffers=true -Dio.netty.noKeySetOptimization=true
-

# Quickly Start

- start Zookeeper
- start Broker: -c /path/broker.properties

## Metrics configuration

```
0.0.0.0:9528/prometheus
```
