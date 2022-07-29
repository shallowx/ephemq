# Shallow

Shallow is a distributed messaging and streaming platform with low latency, high performance and reliability, trillion-level capacity and flexible scalability

![image](https://github.com/shallow-rs/shallow/blob/main/doc/infra.png)

- Broker: accept command(eg: query、storage)、message storage、message push、pull message
- Client: producer & consumer
- Nameserver: broker register
- Metrics: CPU、JVM、message、semaphore

# Latency

![image](https://github.com/shallow-rs/shallow/blob/main/doc/latency.png)

# Environment

- Jdk version: Jdk17+
- Memory size: 64G (the better)
- CPU:  at least 16C (the better: 32C or 64C)
