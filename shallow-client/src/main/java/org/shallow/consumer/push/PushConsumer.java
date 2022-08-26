package org.shallow.consumer.push;

import org.shallow.internal.Listener;

public interface PushConsumer {
    Subscription subscribe(String topic, String queue);
    void subscribeAsync(String topic, String queue, SubscribeCallback callback);

    MessagePushListener getPushConsumerListener();
}
