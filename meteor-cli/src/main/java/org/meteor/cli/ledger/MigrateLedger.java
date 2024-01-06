package org.meteor.cli.ledger;

public class MigrateLedger {
    private String topic;
    private int partition;
    private String from;
    private String to;

    public MigrateLedger(String topic, int partition, String from, String to) {
        this.topic = topic;
        this.partition = partition;
        this.from = from;
        this.to = to;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "migrate_ledger{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                '}';
    }
}
