package org.shallow.meta;

import java.util.Objects;

public class TopicRecord {
    private String name;
    private int partitions;
    private int latencies;
    private PartitionRecord partitionRecord;

    public TopicRecord(String name, int partitions, int latencies) {
        this.name = name;
        this.partitions = partitions;
        this.latencies = latencies;
    }

    public TopicRecord(String name, int partitions, int latencies, PartitionRecord partitionRecord) {
        this.name = name;
        this.partitions = partitions;
        this.latencies = latencies;
        this.partitionRecord = partitionRecord;
    }

    public String getName() {
        return name;
    }

    public int getPartitions() {
        return partitions;
    }

    public int getLatencies() {
        return latencies;
    }

    public PartitionRecord getPartitionRecord() {
        return partitionRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicRecord that)) return false;
        return getPartitions() == that.getPartitions() &&
                getLatencies() == that.getLatencies() &&
                getName().equals(that.getName()) &&
                getPartitionRecord().equals(that.getPartitionRecord());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getPartitions(), getLatencies(), getPartitionRecord());
    }

    @Override
    public String toString() {
        return "TopicRecord{" +
                "name='" + name + '\'' +
                ", partitions=" + partitions +
                ", latencies=" + latencies +
                ", partitionRecord=" + partitionRecord +
                '}';
    }
}
