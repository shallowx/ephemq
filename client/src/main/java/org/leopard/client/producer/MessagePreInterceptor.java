package org.leopard.client.producer;

import org.leopard.client.Message;

@FunctionalInterface
public interface MessagePreInterceptor {
    Message interceptor(Message message);
}
