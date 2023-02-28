package org.ostara.client.producer;

import org.ostara.client.Message;

public interface Producer {

    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    void sendOneway(Message message);

    SendResult send(Message message) throws Exception;

    void sendAsync(Message message, SendCallback callback);

    void sendOneway(Message message, ProducerInterceptor interceptor);

    SendResult send(Message message, ProducerInterceptor interceptor) throws Exception;

    void sendAsync(Message message, ProducerInterceptor interceptor, SendCallback callback);
}
