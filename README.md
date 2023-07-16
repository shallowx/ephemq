# Architecture

Ostara is a distributed messaging and streaming platform on based memory with low latency, high performance and
reliability, trillion-level capacity and flexible scalability

![image](https://github.com/shallow-rs/Ostara/blob/main/doc/image/infra.png)

- Dotted lines are optional

# Example
To demonstrate the effect, the setting of sending one message per second was implemented. However, this setting can be omitted when deploying it in practice.
![image](https://github.com/shallow-rs/Ostara/blob/main/doc/image/example.gif)

## Latency

![image](https://github.com/shallow-rs/Ostara/blob/main/doc/image/partition.png)

## Message

![image](https://github.com/shallow-rs/Ostara/blob/main/doc/image/message.png)

# Environment

- Zookeeper 3.5.x or higher
- Java 17
- -Dio.prometheus.client.export-protobuf.use-direct-buffers=true // prometheus directory memory
- 
# Quickly Start
- start Zookeeper
- start Broker: -c /path/broker.properties

## Create Topic
```
    public void createTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:8888");
            }
        });

        Client client = new Client("default", clientConfig, new ClientListener() {});
        client.start();

        client.createTopic("#test#default", 10, 1);
        new CountDownLatch(1).await(5000, TimeUnit.MILLISECONDS);
        client.close();
    }
```

## Subscribe Message And Consume Message
```
    public void attachOfReset() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:8888");
            }
        });
        clientConfig.setConnectionPoolCapacity(2);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer consumer = new Consumer("default", consumerConfig, new MessageListener() {
            @Override
            public void onMessage(String topic, String queue, MessageId messageId, ByteBuf message, Extras extras) {
                String msg = ByteBufUtils.buf2String(message, message.readableBytes());
                System.out.printf("messageId=%s topic=%s queue=%s message=%s%n", messageId, topic, queue, msg);
            }
        });
        consumer.start();

        String[] symbols = new String[]{"BTC-USDT"};
        for (String symbol : symbols) {
            consumer.attach("#test#default", symbol);
        }

        new CountDownLatch(1).await(10, TimeUnit.MINUTES);
        consumer.close();
    }
```

## Send Message
```
    public void continueSend() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>(){
            {add("127.0.0.1:8888");}
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                Producer producer = new Producer("default", producerConfig);
                producer.start();

                Random random = new Random();
                String[] symbols = new String[]{"BTC-USDT"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtils.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.send("#test#default", symbol, message, new Extras());
                        System.out.println(messageId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t){
                    t.printStackTrace();
                }
                producer.close();
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
    }
```
## Async Send Message

```
 public void continueAsyncSend() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>(){
            {add("127.0.0.1:8888");}
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                Producer producer = new Producer("default", producerConfig);
                producer.start();

                Random random = new Random();
                String[] symbols = new String[]{"BTC-USDT"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtils.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.sendAsync("#test#default", symbol, message, new Extras(), new SendCallback(){
                               public void onCompleted(MessageId messageId, Throwable t) {
                                    // todo 
                               }
                        });
                        System.out.println(messageId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t){
                    t.printStackTrace();
                }
                producer.close();
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
    }
```

## SendOneway Message

```
 public void continueSendOneway() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>(){
            {add("127.0.0.1:8888");}
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                Producer producer = new Producer("default", producerConfig);
                producer.start();

                Random random = new Random();
                String[] symbols = new String[]{"BTC-USDT"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtils.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.sendOneway("#test#default", symbol, message, new Extras());
                        System.out.println(messageId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t){
                    t.printStackTrace();
                }
                producer.close();
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
    }
```

## Metrics configuration
```
0.0.0.0:8889/prometheus
```
