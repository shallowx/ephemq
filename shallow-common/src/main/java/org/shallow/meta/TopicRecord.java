package org.shallow.meta;

import java.util.Objects;

public class TopicRecord {
    private String name;
    private PartitionRecord partitionRecord;

    public TopicRecord(String name, PartitionRecord partitionRecord) {
        this.name = name;
        this.partitionRecord = partitionRecord;
    }

    public String getName() {
        return name;
    }

    public PartitionRecord getPartitionRecord() {
        return partitionRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicRecord)) return false;
        TopicRecord that = (TopicRecord) o;
        return getName().equals(that.getName()) && getPartitionRecord().equals(that.getPartitionRecord());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getPartitionRecord());
    }

    @Override
    public String toString() {
        return "TopicRecord{" +
                "name='" + name + '\'' +
                ", partitionRecord=" + partitionRecord +
                '}';
    }
}
