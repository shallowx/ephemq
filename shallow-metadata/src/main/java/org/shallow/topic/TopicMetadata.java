package org.shallow.topic;

public class TopicMetadata {
    private final String name;
    private Partition partition;

    public TopicMetadata(String topic) {
        this.name = topic;
    }

    public String name() {
        return name;
    }
}
