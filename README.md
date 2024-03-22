# Architecture

- Meteor is a distributed messaging and streaming platform on based memory with low latency, high performance and
  reliability, trillion-level capacity and flexible scalability
- Support high traffic, low latency, and meet subtle-level latency
- Supports connection to broker and proxy
- If only broker is required, set the broker address, and broker is suitable for applications that are highly sensitive
  to delays
- If proxy is required, set the proxy address,and 'proxy.upstream.servers' set the broker address, and proxy is suitable
  for low latency sensitivity, but proxy can use relatively low-configuration machine horizontal extension classes to
  support more client connections

![image](https://github.com/shallowx/meteor/blob/main/docs/image/infra.png)

# Example

- For better demonstration of the running effect, set the time interval to 1 second [TimeUnit.MILLISECONDS.sleep(1000)]
  for easy demo creation, but it actually supports high traffic and meets subtle-level latency
- To demonstrate the effect, the setting of sending one message per second was implemented. However, this
  setting[TimeUnit.MILLISECONDS.sleep(1000)] can be omitted when deploying it in practice
  ![image](https://github.com/shallowx/meteor/blob/main/docs/image/example.gif)

## Store And Buffers

![image](https://github.com/shallowx/meteor/blob/main/docs/image/partition.png)

# Environment

- Zookeeper Version: Zookeeper 3.5.x or higher
- Java Version: Java 17 or higher

## Metrics

- 0.0.0.0:9528/prometheus
- Support metrics and monitoring, data collection uses prometheus components

