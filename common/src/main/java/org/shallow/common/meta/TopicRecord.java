package org.shallow.common.meta;

import java.util.Objects;
import java.util.Set;

public class TopicRecord {
    private String name;
    private int partitions;
    private int latencies;
    private Set<PartitionRecord> partitionRecords;

    public TopicRecord() {
        // unsupported
    }

    public static TopicBuilder newBuilder() {
        return new TopicBuilder();
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

    public void setName(String name) {
        this.name = name;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public void setLatencies(int latencies) {
        this.latencies = latencies;
    }

    public void setPartitionRecords(Set<PartitionRecord> partitionRecords) {
        this.partitionRecords = partitionRecords;
    }

    public static class TopicBuilder {
        private String name;
        private int partitions;
        private int latencies;
        private Set<PartitionRecord> partitionRecords;

        private TopicBuilder() {
        }

        public TopicBuilder name(String name) {
            this.name = name;
            return this;
        }

        public TopicBuilder partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        public TopicBuilder latencies(int latencies) {
            this.latencies = latencies;
            return this;
        }

        public TopicBuilder partitionRecords(Set<PartitionRecord> partitionRecords) {
            this.partitionRecords = partitionRecords;
            return this;
        }


        public TopicRecord build() {
            TopicRecord topicRecord = new TopicRecord();
            topicRecord.name = this.name;
            topicRecord.partitions = this.partitions;
            topicRecord.latencies = this.latencies;
            topicRecord.partitionRecords = this.partitionRecords;

            return topicRecord;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicRecord that)) return false;
        return getPartitions() == that.getPartitions() &&
                getName().equals(that.getName()) &&
                getPartitionRecords().equals(that.getPartitionRecords());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getPartitions(), getPartitionRecords());
    }

    @Override
    public String toString() {
        return "TopicRecord{" +
                "name='" + name + '\'' +
                ", partitions=" + partitions +
                ", partitionRecords=" + partitionRecords +
                '}';
    }
}
