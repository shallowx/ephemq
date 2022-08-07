# Architecture

Shallow is a distributed messaging and streaming platform on based memory with low latency, high performance and reliability, trillion-level capacity and flexible scalability

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/infra.png)

- Broker: Process command...
- Client: Producer & Consumer...
- Nameserver: Cluster management、Metadata management...
- Metrics & Monitoring: CPU、JVM、Thread、Cluster、Metadata、Network...

# Latency

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/latency.png)

# Asynchronous thread model

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/thread_model.png)

# Protocol
- Leader election protocol: Raft

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/metadata.png)

# Environment

- Jdk version: Jdk17+
- Memory size: 64G (the better)
- CPU: at least 16C (the better: 32C or 64C)
