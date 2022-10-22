package org.shallow.client.consumer;

import org.shallow.client.Message;

@FunctionalInterface
public interface MessagePostInterceptor {
    Message interceptor(Message message);
}
