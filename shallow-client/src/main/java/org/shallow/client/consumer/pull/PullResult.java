package org.shallow.client.consumer.pull;

import org.shallow.client.Message;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public final class PullResult {

    private final int ledger;
    private String topic;
    private final String queue;
    private final int startEpoch;
    private final long startIndex;
    private final int limit;
    private final List<Message> message;

    public PullResult(int ledger, String topic, String queue, int limit, int startEpoch, long startIndex , List<Message> message) {
        this.ledger = ledger;
        this.topic = topic;
        this.queue = queue;
        this.message = message;
        this.limit = limit;
        this.startEpoch = startEpoch;
        this.startIndex = startIndex;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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

    public int getStartEpoch() {
        return startEpoch;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public int getLimit() {
        return limit;
    }

    @Override
    public String toString() {
        return "PullResult{" +
                "ledger=" + ledger +
                ", topic='" + topic + '\'' +
                ", queue='" + queue + '\'' +
                ", startEpoch=" + startEpoch +
                ", startIndex=" + startIndex +
                ", limit=" + limit +
                ", message=" + message +
                '}';
    }
}
