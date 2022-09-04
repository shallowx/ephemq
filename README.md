# Architecture

Shallow is a distributed messaging and streaming platform on based memory with low latency, high performance and reliability, trillion-level capacity and flexible scalability

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/infra.png)

- Broker: Process command...
- Client: Producer & Consumer...
- Nameserver: Cluster management、Metadata management...
- Metrics & Monitoring: CPU、JVM、Thread、Cluster、Metadata、Network...

## Latency

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/latency.png)

## Message
![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/message.png)
- Type: pull or push or no

## Asynchronous thread model

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/thread_model.png)

## Message dispatch architecture

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/push_message.png)

## Metadata

- Architecture
![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/metadata.png)

- Leader election protocol: Raft

# Environment

- Jdk version: Jdk17+

# Quickstart
## CreateTopic
```
ClientConfig clientConfig = new ClientConfig();
clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
Client client = new Client("create-client", clientConfig);
client.start();

Promise<CreateTopicResponse> promise = client.getMetadataManager().createTopic(CREATE_TOPIC, "create", 3, 1);
CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);
```
## Subscribe
```
ClientConfig clientConfig = new ClientConfig();
clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

ConsumerConfig consumerConfig = new ConsumerConfig();
consumerConfig.setClientConfig(clientConfig);

org.shallow.consumer.push.PushConsumer messagePushConsumer = new MessagePushConsumer("example-consumer", consumerConfig);
messagePushConsumer.registerListener(new MessagePushListener() {
        @Override
        public void onMessage(Message message) {
            if (logger.isInfoEnabled()) {
                logger.info("Recive message:{}", message);
            }
        }
    });
messagePushConsumer.start();

Subscription subscribe = messagePushConsumer.subscribe("create", "message");
```
## Send
```
ProducerConfig producerConfig = new ProducerConfig();
producerConfig.setClientConfig(clientConfig);

Producer producer = new MessageProducer("async-producer", producerConfig);
producer.start();

Message message = new Message("create", "message", "message-test-send-async".getBytes(UTF_8), new Message.Extras());
MessagePreFilter filter = sendMessage -> sendMessage;

producer.sendAsync(message, filter, (sendResult, cause) -> {});
```
- For more, please check out module: shallow-example