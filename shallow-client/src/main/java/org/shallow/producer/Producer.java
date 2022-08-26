package org.shallow.producer;

import org.shallow.Message;

public interface Producer {

    void start();
    void sendOneway(Message message);

    SendResult send(Message message) throws Exception;

    void sendAsync(Message message, SendCallback callback);

    void sendOneway(Message message, MessageFilter messageFilter);

    SendResult send(Message message, MessageFilter messageFilter) throws Exception;

    void sendAsync(Message message, MessageFilter messageFilter, SendCallback callback);
}
