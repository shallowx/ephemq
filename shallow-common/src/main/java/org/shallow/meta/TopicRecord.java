package org.shallow.meta;

import java.util.Objects;
import java.util.Set;

public class TopicRecord {
    private final String name;
    private int partitions;
    private int latencies;
    private Set<PartitionRecord> partitionRecords;

    public TopicRecord(String name) {
        this.name = name;
    }

    public TopicRecord(String name, int partitions, int latencies) {
        this.name = name;
        this.partitions = partitions;
        this.latencies = latencies;
    }

    public TopicRecord(String name, int partitions, int latencies, Set<PartitionRecord> partitionRecords) {
        this.name = name;
        this.partitions = partitions;
        this.latencies = latencies;
        this.partitionRecords = partitionRecords;
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

    public Set<PartitionRecord> getPartitionRecords() {
        return partitionRecords;
    }

    public void setPartitionRecords(Set<PartitionRecord> partitionRecords) {
        this.partitionRecords = partitionRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicRecord that)) return false;
        return getPartitions() == that.getPartitions() &&
                getLatencies() == that.getLatencies() &&
                getName().equals(that.getName()) &&
                getPartitionRecords().equals(that.getPartitionRecords());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getPartitions(), getLatencies(), getPartitionRecords());
    }

    @Override
    public String toString() {
        return "TopicRecord{" +
                "name='" + name + '\'' +
                ", partitions=" + partitions +
                ", latencies=" + latencies +
                ", partitionRecords=" + partitionRecords +
                '}';
    }
}
