package org.shallow.consumer;

import org.shallow.Message;

@FunctionalInterface
public interface MessagePostInterceptor {
    Message interceptor(Message message);
}
