package org.ostara.common.metadata;

import java.util.Objects;
import java.util.Set;

public class Topic {
    private String name;
    private int partitionLimit;
    private int replicateLimit;
    private Set<Partition> partitions;
    private int hash;

    public Topic() {
        // unsupported
    }

    public static TopicBuilder newBuilder() {
        return new TopicBuilder();
    }

    public String getName() {
        return name;
    }

    public int getPartitionLimit() {
        return partitionLimit;
    }

    public int getReplicateLimit() {
        return replicateLimit;
    }

    public Set<Partition> getPartitions() {
        return partitions;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPartitions(int partitionLimit) {
        this.partitionLimit = partitionLimit;
    }

    public void setReplicateLimit(int replicateLimit) {
        this.replicateLimit = replicateLimit;
    }

    public void setPartitions(Set<Partition> partitions) {
        this.partitions = partitions;
    }

    public static class TopicBuilder {
        private String name;
        private int partitionLimit;
        private int replicateLimit;
        private Set<Partition> partitions;
        private int hash;
        private TopicBuilder() {
        }

        public TopicBuilder name(String name) {
            this.name = name;
            return this;
        }

        public TopicBuilder partitionLimit(int partitionLimit) {
            this.partitionLimit = partitionLimit;
            return this;
        }

        public TopicBuilder replicateLimit(int replicateLimit) {
            this.replicateLimit = replicateLimit;
            return this;
        }

        public TopicBuilder partitions(Set<Partition> partitionRecords) {
            this.partitions = partitionRecords;
            return this;
        }


        public Topic build() {
            Topic record = new Topic();
            record.name = this.name;
            record.partitions = this.partitions;
            record.replicateLimit = this.replicateLimit;
            record.partitionLimit = this.partitionLimit;
            record.hash = Objects.hash(name, replicateLimit, partitions);
            return record;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Topic that)) return false;
        return getPartitionLimit() == that.getPartitionLimit() &&
                getName().equals(that.getName()) &&
                getPartitions().equals(that.getPartitions());
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "TopicRecord{" +
                "name='" + name + '\'' +
                ", partitionLimit=" + partitionLimit +
                ", partitions=" + partitions +
                '}';
    }
}
