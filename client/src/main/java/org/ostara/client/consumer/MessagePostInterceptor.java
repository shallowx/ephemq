package org.ostara.client.consumer;

import org.ostara.client.Message;

@FunctionalInterface
public interface MessagePostInterceptor {
    Message interceptor(Message message);
}
