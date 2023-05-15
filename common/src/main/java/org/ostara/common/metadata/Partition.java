package org.ostara.common.metadata;

import java.util.List;
import java.util.Objects;

public class Partition {

    private int id;
    private int ledgerId;
    private String leader;
    private List<String> replicates;
    private int epoch;
    private int hash;

    private Partition() {
        // unsupported
    }

    public static PartitionBuilder newBuilder() {
        return new PartitionBuilder();
    }

    public int getId() {
        return id;
    }

    public int getEpoch() {
        return epoch;
    }

    public int getLedgerId() {
        return ledgerId;
    }

    public String getLeader() {
        return leader;
    }

    public List<String> getReplicates() {
        return replicates;
    }

    public static class PartitionBuilder {
        private int id;
        private int ledgerId;
        private String leader;
        private List<String> replicates;
        private int epoch;
        private int hash;

        private PartitionBuilder() {
        }

        public PartitionBuilder epoch(int epoch) {
            this.epoch = epoch;
            return this;
        }

        public PartitionBuilder id(int id) {
            this.id = id;
            return this;
        }

        public PartitionBuilder ledgerId(int ledgerId) {
            this.ledgerId = ledgerId;
            return this;
        }

        public PartitionBuilder leader(String leader) {
            this.leader = leader;
            return this;
        }

        public PartitionBuilder replicates(List<String> replicates) {
            this.replicates = replicates;
            return this;
        }

        public Partition build() {
            Partition partition = new Partition();

            partition.id = this.id;
            partition.replicates = this.replicates;
            partition.leader = this.leader;
            partition.ledgerId = this.ledgerId;
            partition.epoch = this.epoch;
            partition.hash = Objects.hash(id, ledgerId);

            return partition;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Partition partition)) {
            return false;
        }
        return getId() == partition.getId() &&
                getLedgerId() == partition.getLedgerId();
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "id=" + id +
                ", ledgerId=" + ledgerId +
                ", leader='" + leader + '\'' +
                ", replicates=" + replicates +
                ", epoch=" + epoch +
                '}';
    }
}
