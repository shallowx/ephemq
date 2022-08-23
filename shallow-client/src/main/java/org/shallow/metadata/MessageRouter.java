package org.shallow.metadata;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MessageRouter {
    private final String topic;
    private final int[] ledgerIds;
    private final Map<Integer, RouteHolder> holders;

    public MessageRouter(String topic, Map<Integer, RouteHolder> holders) {
        this.topic = topic;
        this.holders = holders;
        this.ledgerIds = holders.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
    }

    public RouteHolder allocRouteHolder(String queue) {
        int length = ledgerIds.length;

        if (length == 0) {
            return null;
        } else if (length == 1) {
            return holders.get(0);
        }

        int index = ((31 * topic.hashCode() + queue.hashCode()) & 0x7fffffff) % length;
        return holders.get(index);
    }

    public record RouteHolder(String topic, int ledger, int partition, SocketAddress leader, Set<SocketAddress> latencies) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RouteHolder that)) return false;
            return partition == that.partition &&
                    ledger == that.ledger &&
                    Objects.equals(topic, that.topic) &&
                    Objects.equals(leader, that.leader) &&
                    Objects.equals(latencies, that.latencies);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, ledger);
        }

        @Override
        public String toString() {
            return "RouteHolder{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", leader=" + leader +
                    ", latencies=" + latencies +
                    '}';
        }
    }
}
