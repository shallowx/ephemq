package org.shallow.consumer.pull;

import org.shallow.Message;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public final class PullResult {

    private final int ledger;
    private String topic;
    private final String queue;
    private final int endEpoch;
    private final long endIndex;
    private final int limit;
    private final List<Message> message;

    public PullResult(int ledger, String topic, String queue, int limit, int endEpoch, long endIndex , List<Message> message) {
        this.ledger = ledger;
        this.topic = topic;
        this.queue = queue;
        this.message = message;
        this.limit = limit;
        this.endEpoch = endEpoch;
        this.endIndex = endIndex;
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

    public int getEndEpoch() {
        return endEpoch;
    }

    public long getEndIndex() {
        return endIndex;
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
                ", endEpoch=" + endEpoch +
                ", endIndex=" + endIndex +
                ", limit=" + limit +
                ", message=" + message +
                '}';
    }
}
