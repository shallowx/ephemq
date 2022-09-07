package org.shallow.consumer;

import org.shallow.Message;

@FunctionalInterface
public interface MessagePostFilter {
    Message filter(Message message);
}
