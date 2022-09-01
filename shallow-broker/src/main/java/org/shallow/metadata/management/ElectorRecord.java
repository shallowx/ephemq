package org.shallow.metadata.management;

import java.util.List;

public class ElectorRecord {
    String leader;
    List<String> latencies;

    public ElectorRecord(String leader, List<String> latencies) {
        this.leader = leader;
        this.latencies = latencies;
    }

    public String getLeader() {
        return leader;
    }

    public List<String> getLatencies() {
        return latencies;
    }
}
