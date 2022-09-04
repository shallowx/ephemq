package org.shallow.producer;

import org.shallow.Message;

public interface Producer {

    void start() throws Exception;

    void shutdownGracefully() throws Exception;
    void sendOneway(Message message);

    SendResult send(Message message) throws Exception;

    void sendAsync(Message message, SendCallback callback);

    void sendOneway(Message message, MessagePreFilter messageFilter);

    SendResult send(Message message, MessagePreFilter messageFilter) throws Exception;

    void sendAsync(Message message, MessagePreFilter messageFilter, SendCallback callback);
}
