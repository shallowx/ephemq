package org.shallow.meta;

import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;

public class Partition {

    private int id;
    private int partitions;
    private int latency;
    private String leader;
    private SocketAddress leaderAddress;
    private List<String> latencies;
    private List<SocketAddress> latenciesAddress;

    public Partition() {
    }

    public Partition(int id, int partitions, int latency, String leader, SocketAddress leaderAddress, List<String> latencies, List<SocketAddress> latenciesAddress) {
        this.id = id;
        this.partitions = partitions;
        this.latency = latency;
        this.leader = leader;
        this.leaderAddress = leaderAddress;
        this.latencies = latencies;
        this.latenciesAddress = latenciesAddress;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
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

    public SocketAddress getLeaderAddress() {
        return leaderAddress;
    }

    public void setLeaderAddress(SocketAddress leaderAddress) {
        this.leaderAddress = leaderAddress;
    }

    public List<String> getLatencies() {
        return latencies;
    }

    public void setLatencies(List<String> latencies) {
        this.latencies = latencies;
    }

    public List<SocketAddress> getLatenciesAddress() {
        return latenciesAddress;
    }

    public void setLatenciesAddress(List<SocketAddress> latenciesAddress) {
        this.latenciesAddress = latenciesAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Partition that)) return false;
        return getId() == that.getId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }

    @Override
    public String toString() {
        return "PartitionInfo{" +
                "id=" + id +
                ", partitions=" + partitions +
                ", latency=" + latency +
                ", leader='" + leader + '\'' +
                ", leaderAddress=" + leaderAddress +
                ", latencies=" + latencies +
                ", latenciesAddress=" + latenciesAddress +
                '}';
    }
}
