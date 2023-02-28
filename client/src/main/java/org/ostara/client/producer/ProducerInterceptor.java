package org.ostara.client.producer;

import org.ostara.client.Message;

@FunctionalInterface
public interface ProducerInterceptor {
    Message interceptor(Message message);
}
