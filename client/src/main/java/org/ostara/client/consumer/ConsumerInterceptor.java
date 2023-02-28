package org.ostara.client.consumer;

import org.ostara.client.Message;

@FunctionalInterface
public interface ConsumerInterceptor {
    Message interceptor(Message message);
}
