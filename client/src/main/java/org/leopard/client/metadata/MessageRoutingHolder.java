package org.leopard.client.metadata;

import java.net.SocketAddress;
import java.util.Set;

public record MessageRoutingHolder(String topic, int ledger, int partition, SocketAddress leader, Set<SocketAddress> latencies) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageRoutingHolder that)) return false;
        return ledger == that.ledger &&
                partition == that.partition &&
                topic.equals(that.topic) &&
                leader.equals(that.leader) &&
                latencies.equals(that.latencies);
    }

    @Override
    public String toString() {
        return "MessageRoutingHolder{" +
                "topic='" + topic + '\'' +
                ", ledger=" + ledger +
                ", partition=" + partition +
                ", leader=" + leader +
                ", latencies=" + latencies +
                '}';
    }
}
