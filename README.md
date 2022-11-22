# Architecture

Shallow is a distributed messaging and streaming platform on based memory with low latency, high performance and
reliability, trillion-level capacity and flexible scalability

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/infra.png)

- Dotted lines are optional

# Example

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/example.gif)

## Latency

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/partition.png)

## Message

![image](https://github.com/shallow-rs/shallow/blob/main/doc/image/message.png)

# Environment

- Java version: Jdk17+

# Quickstart

## Create Topic

```
ClientConfig clientConfig = new ClientConfig();
clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
Client client = new Client("create-client", clientConfig);
client.start();

Promise<CreateTopicResponse> promise = client.getMetadataManager().createTopic(CREATE_TOPIC, "create", 3, 1);
CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);
```

## Subscribe Message

```
ClientConfig clientConfig = new ClientConfig();
clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

ConsumerConfig consumerConfig = new ConsumerConfig();
consumerConfig.setClientConfig(clientConfig);

org.leopard.consumer.push.PushConsumer messagePushConsumer = new MessagePushConsumer("example-consumer", consumerConfig);
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

## Send Message

```
ProducerConfig producerConfig = new ProducerConfig();
producerConfig.setClientConfig(clientConfig);

Producer producer = new MessageProducer("async-producer", producerConfig);
producer.start();

Message message = new Message("create", "message", "message-test-send-async".getBytes(UTF_8), new Message.Extras());
MessagePreInterceptor interceptor = sendMessage -> sendMessage;

producer.sendAsync(message, filter, (sendResult, cause) -> {});
```