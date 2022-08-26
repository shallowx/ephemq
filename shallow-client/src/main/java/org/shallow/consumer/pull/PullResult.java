package org.shallow.consumer.pull;

import org.shallow.Message;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public final class PullResult {

    private final int ledger;
    private final String topic;
    private final String queue;
    private final List<Message> message;

    public PullResult(int ledger, String topic, String queue, List<Message> message) {
        this.ledger = ledger;
        this.topic = topic;
        this.queue = queue;
        this.message = message;
    }

    public int getLedger() {
        return ledger;
    }

    public String getTopic() {
        return topic;
    }

    public String getQueue() {
        return queue;
    }

    public List<Message> getMessage() {
        return message;
    }
}
