package org.shallow.consumer.push;

import org.shallow.Message;

@FunctionalInterface
public interface MessagePostFilter {
    Message filter(Message message);
}
