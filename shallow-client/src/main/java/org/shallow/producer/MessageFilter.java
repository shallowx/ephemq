package org.shallow.producer;

import org.shallow.Message;

@FunctionalInterface
public interface MessageFilter {
    Message filter(Message message);
}
