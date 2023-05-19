package org.ostara.client.producer;

import org.ostara.common.MessageId;

@FunctionalInterface
public interface SendCallback {
    void onCompleted(MessageId messageId, Throwable t);
}
