package org.shallow.meta;

import java.util.List;
import java.util.Objects;

public class PartitionRecord {

    private int id;
    private int latency;
    private String leader;
    private List<String> latencies;

    public PartitionRecord(int id, int latency, String leader, List<String> latencies) {
        this.id = id;
        this.latency = latency;
        this.leader = leader;
        this.latencies = latencies;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getLatency() {
        return latency;
    }

    public void setLatency(int latency) {
        this.latency = latency;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public List<String> getLatencies() {
        return latencies;
    }

    public void setLatencies(List<String> latencies) {
        this.latencies = latencies;
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
