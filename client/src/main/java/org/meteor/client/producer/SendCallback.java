package org.meteor.client.producer;

import org.meteor.common.MessageId;

@FunctionalInterface
public interface SendCallback {
    void onCompleted(MessageId messageId, Throwable t);
}
