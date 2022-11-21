package org.leopard.client.consumer;

import org.leopard.common.metadata.Subscription;

public interface Consumer {

    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    Subscription subscribe(String topic, String queue, short version);

    void subscribeAsync(String topic, String queue, short version, SubscribeCallback callback);

    Subscription subscribe(String topic, String queue, short version, MessageListener listener);

    void subscribeAsync(String topic, String queue, short version, SubscribeCallback callback, MessageListener listener);

    Subscription subscribe(String topic, String queue);

    void subscribeAsync(String topic, String queue, SubscribeCallback callback);

    Subscription subscribe(String topic, String queue, MessageListener listener);

    void subscribeAsync(String topic, String queue, SubscribeCallback callback, MessageListener listener);

    Subscription subscribe(String topic, String queue, short version, int epoch, long index);

    void subscribeAsync(String topic, String queue, short version, int epoch, long index, SubscribeCallback callback);

    Subscription subscribe(String topic, String queue, short version, int epoch, long index, MessageListener listener);

    void subscribeAsync(String topic, String queue, short version, int epoch, long index, SubscribeCallback callback, MessageListener listener);

    Subscription subscribe(String topic, String queue, int epoch, long index);

    void subscribeAsync(String topic, String queue, int epoch, long index, SubscribeCallback callback);

    Subscription subscribe(String topic, String queue, int epoch, long index, MessageListener listener);

    void subscribeAsync(String topic, String queue, int epoch, long index, SubscribeCallback callback, MessageListener listener);

    boolean clean(String topic, String queue);

    void cleanAsync(String topic, String queue, CleanSubscribeCallback callback);

    void registerListener(MessageListener listener);

    void registerInterceptor(MessagePostInterceptor interceptor);

    MessageListener getListener();
}
