package org.shallow.producer;

import org.shallow.Message;

@FunctionalInterface
public interface MessagePreInterceptor {
    Message interceptor(Message message);
}
