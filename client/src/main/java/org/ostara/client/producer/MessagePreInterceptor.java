package org.ostara.client.producer;

import org.ostara.client.Message;

@FunctionalInterface
public interface MessagePreInterceptor {
    Message interceptor(Message message);
}
