package org.shallow.client.producer;

import org.shallow.client.Message;

@FunctionalInterface
public interface MessagePreInterceptor {
    Message interceptor(Message message);
}
