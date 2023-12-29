package org.meteor.client.producer;

import org.meteor.common.message.MessageId;

@FunctionalInterface
public interface SendCallback {
    void onCompleted(MessageId messageId, Throwable t);
}
