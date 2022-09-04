package org.shallow.producer;

import org.shallow.Message;

@FunctionalInterface
public interface MessagePreFilter {
    Message filter(Message message);
}
