package org.shallow.metadata.snapshot;

import java.util.List;

public interface LeaderElection {

    String elect(String topic, int partitions, int latencies) throws Exception;

    List<String> assignLatencies(String topic, int partitions, int latencies) throws Exception;
}
