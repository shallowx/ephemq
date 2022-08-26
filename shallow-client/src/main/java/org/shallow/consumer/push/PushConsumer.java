package org.shallow.consumer.push;

interface PushConsumer {
    Subscription subscribe(String topic, String queue);
    void subscribeAsync(String topic, String queue, SubscribeCallback callback);

    MessagePushListener getPushConsumerListener();
}
