package org.shallow.common.meta;

import java.util.List;
import java.util.Objects;

public class PartitionRecord {

    private int id;
    private int latency;
    private String leader;
    private List<String> latencies;

    private PartitionRecord() {
        // unsupported
    }

    public static PartitionBuilder newBuilder() {
        return new PartitionBuilder();
    }

    public int getId() {
        return id;
    }

    public int getLatency() {
        return latency;
    }

    public String getLeader() {
        return leader;
    }

    public List<String> getLatencies() {
        return latencies;
    }

    public static class PartitionBuilder {
        private int id;
        private int latency;
        private String leader;
        private List<String> latencies;

        private PartitionBuilder() {
        }

        public PartitionBuilder id(int id) {
            this.id = id;
            return this;
        }

        public PartitionBuilder latency(int latency) {
            this.latency = latency;
            return this;
        }

        public PartitionBuilder leader(String leader) {
            this.leader = leader;
            return this;
        }

        public PartitionBuilder latencies(List<String> latencies) {
            this.latencies = latencies;
            return this;
        }

        public PartitionRecord build() {
            PartitionRecord partitionRecord = new PartitionRecord();

            partitionRecord.id = this.id;
            partitionRecord.latencies = this.latencies;
            partitionRecord.leader = this.leader;
            partitionRecord.latency = this.latency;

            return partitionRecord;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionRecord partition)) return false;
        return getId() == partition.getId() &&
                getLatency() == partition.getLatency();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getLatency());
    }

    @Override
    public String toString() {
        return "Partition{" +
                "id=" + id +
                ", latency=" + latency +
                ", leader='" + leader + '\'' +
                ", latencies=" + latencies +
                '}';
    }
}
