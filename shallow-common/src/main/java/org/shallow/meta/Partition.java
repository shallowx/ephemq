package org.shallow.meta;

import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;

public class Partition {

    private int id;
    private int latency;
    private String leader;
    private SocketAddress leaderAddress;
    private List<String> latencies;
    private List<SocketAddress> latenciesAddress;

    public Partition(int id, int latency, String leader, SocketAddress leaderAddress, List<String> latencies, List<SocketAddress> latenciesAddress) {
        this.id = id;
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
        if (!(o instanceof Partition)) return false;
        Partition partition = (Partition) o;
        return getId() == partition.getId() && getLatency() == partition.getLatency();
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
                ", leaderAddress=" + leaderAddress +
                ", latencies=" + latencies +
                ", latenciesAddress=" + latenciesAddress +
                '}';
    }
}
