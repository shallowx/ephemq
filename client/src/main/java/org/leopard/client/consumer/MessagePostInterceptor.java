package org.leopard.client.consumer;

import org.leopard.client.Message;

@FunctionalInterface
public interface MessagePostInterceptor {
    Message interceptor(Message message);
}
