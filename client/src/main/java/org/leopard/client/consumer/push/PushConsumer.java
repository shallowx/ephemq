package org.leopard.client.consumer.push;

import org.leopard.client.consumer.MessagePostInterceptor;

public interface PushConsumer {

    void start() throws Exception;

    void shutdownGracefully() throws Exception;
    Subscription subscribe(String topic, String queue, short version);
    void subscribeAsync(String topic, String queue, short version, SubscribeCallback callback);

    Subscription subscribe(String topic, String queue, short version, MessagePushListener listener);

    void subscribeAsync(String topic, String queue, short version, SubscribeCallback callback, MessagePushListener listener);

    Subscription subscribe(String topic, String queue);
    void subscribeAsync(String topic, String queue, SubscribeCallback callback);

    Subscription subscribe(String topic, String queue, MessagePushListener listener);

    void subscribeAsync(String topic, String queue, SubscribeCallback callback, MessagePushListener listener);

    Subscription subscribe(String topic, String queue, short version, int epoch, long index);

    void subscribeAsync(String topic, String queue, short version, int epoch, long index, SubscribeCallback callback);

    Subscription subscribe(String topic, String queue, short version, int epoch, long index, MessagePushListener listener);

    void subscribeAsync(String topic, String queue, short version, int epoch, long index, SubscribeCallback callback, MessagePushListener listener);

    Subscription subscribe(String topic, String queue, int epoch, long index);

    void subscribeAsync(String topic, String queue, int epoch, long index, SubscribeCallback callback);

    Subscription subscribe(String topic, String queue, int epoch, long index, MessagePushListener listener);

    void subscribeAsync(String topic, String queue, int epoch, long index, SubscribeCallback callback, MessagePushListener listener);

    boolean clean(String topic, String queue);

    void cleanAsync(String topic, String queue, CleanSubscribeCallback callback);

    void registerListener(MessagePushListener listener);
    void registerInterceptor(MessagePostInterceptor interceptor);
    MessagePushListener getListener();
}
