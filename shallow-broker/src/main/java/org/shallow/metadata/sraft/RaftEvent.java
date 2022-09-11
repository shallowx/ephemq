package org.shallow.metadata.sraft;

import java.net.SocketAddress;
import java.util.List;

import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class RaftEvent {

    private String leader;
    private int term;
    private int version;
    private SocketAddress leaderAddress;
    private List<SocketAddress> quorumVoters;

    private RaftEvent() {
    }

    public String getLeader() {
        return leader;
    }

    public int getTerm() {
        return term;
    }

    public SocketAddress getLeaderAddress() {
        return leaderAddress;
    }

    public List<SocketAddress> getQuorumVoters() {
        return quorumVoters;
    }

    public int getVersion() {
        return version;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public void setLeaderAddress(SocketAddress leaderAddress) {
        this.leaderAddress = leaderAddress;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public static RaftEventBuilder newBuilder() {
        return new RaftEventBuilder();
    }

    public static class RaftEventBuilder {
        private String leader;
        private SocketAddress leaderAddress;
        private int version;
        private int term;
        private List<SocketAddress> quorumVoters;

        public RaftEventBuilder() {
        }

        public RaftEventBuilder leader(String leader) {
            this.leader = leader;
            return this;
        }

        public RaftEventBuilder address(String host, int port) {
            this.leaderAddress = switchSocketAddress(host, port);
            return this;
        }

        public RaftEventBuilder term(int term) {
            this.term = term;
            return this;
        }

        public RaftEventBuilder version(int version) {
            this.version = version;
            return this;
        }

        public RaftEventBuilder quorumVoters(List<SocketAddress> addresses) {
            this.quorumVoters = addresses;
            return this;
        }

        public RaftEvent build() {
            RaftEvent event = new RaftEvent();
            event.leader = leader;
            event.term = term;
            event.quorumVoters = quorumVoters;

            return event;
        }
    }
}
