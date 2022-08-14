package org.shallow.metadata.sraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.ClusterManager;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.Strategy;
import org.shallow.metadata.TopicManager;
import org.shallow.proto.elector.VoteResponse;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.shallow.util.NetworkUtil.*;

public class SRaftProcessController {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftProcessController.class);

    private final BrokerConfig config;
    private final SRaftHeartbeat heartbeat;
    private final SRaftLog<TopicRecord> topicSRaftLog;
    private final SRaftLog<NodeRecord> clusterSRaftLog;

    public SRaftProcessController(BrokerConfig config, MappedFileApi api) {
        this.config = config;
        this.heartbeat = new SRaftHeartbeat(config, this);
        this.topicSRaftLog = new TopicManager(toSocketAddressWithoutSelf(), config, api);
        this.clusterSRaftLog = new ClusterManager(toSocketAddressWithoutSelf(), config, api);
    }

    public void start() throws Exception {
        checkQuorumVoters();
        if (config.isStandAlone()) {
            if (logger.isInfoEnabled()) {
                logger.info("The model is stand alone, and the node<name={} host={} port={}> is elected as leader", config.getServerId(), config.getExposedHost(), config.getExposedPort());
            }
            return;
        }
        heartbeat.start();
    }

    private void checkQuorumVoters() {
        Set<SocketAddress> quorumVoters = toSocketAddress();
        if (config.isStandAlone()) {
            if (quorumVoters.size() != 1) {
                throw new IllegalArgumentException(String.format("The model is stand alone, and the quorum voters<shallow.controller.quorum.voters> value expected = 1, but actual = %d", quorumVoters.size()));
            }

            checkQuorumVoterRoles(quorumVoters);
            return;
        }

        if (quorumVoters.size() < 3) {
            throw new IllegalArgumentException(String.format("The quorum voters<shallow.controller.quorum.voters> value expected >= 3, but actual = %d", quorumVoters.size()));
        }

        if (quorumVoters.size() % 2 == 0) {
            throw new IllegalArgumentException("The quorum voters<shallow.controller.quorum.voters> value expected to be odd, but actually even, for example, it should be 3, not 4");
        }

        checkQuorumVoterRoles(quorumVoters);
    }

    private void checkQuorumVoterRoles(Set<SocketAddress> quorumVoters) {
        SocketAddress localAddress = switchSocketAddress(config.getExposedHost(), config.getExposedPort());
        if (quorumVoters.contains(localAddress) && !config.getProcessRoles().contains("controller")) {
            throw new IllegalArgumentException(String.format("The quorum voters<shallow.controller.quorum.voters> value with node<host=%s port=%d> role must be 'controller'", config.getExposedHost(), config.getExposedPort()));
        }
    }

    public void respondVote(int term, Promise<VoteResponse> promise) {
        SRaftQuorumVoter quorumVoter = heartbeat.getQuorumVoter();
        if (quorumVoter.getTerm() >= term) {
            VoteResponse response = VoteResponse.newBuilder().setAck(false).build();
            promise.trySuccess(response);

            return;
        }

        Promise<VoteResponse> respondVotePromise = newImmediatePromise();
        respondVotePromise.addListener(f -> {
            if (f.isSuccess()) {
                promise.trySuccess((VoteResponse) f.get());
                heartbeat.stopHeartbeat();
            }
        });

        quorumVoter.respondVote(respondVotePromise);
    }

    public void receiveHeartbeat(int term) {
        heartbeat.receiveHeartbeat(term);
    }

    public void prepareCommit(Strategy strategy, CommitRecord<?> record, Promise<MessageLite> promise) {
        switch (strategy) {
            case TOPIC -> {
                topicSRaftLog.prepareCommit((TopicRecord) record.getRecord(), record.getType(), promise);
            }

            case CLUSTER -> {
                clusterSRaftLog.prepareCommit((NodeRecord) record.getRecord(), record.getType(), promise);
            }
        }
    }

    public void postCommit(Strategy strategy, CommitRecord<?> record, Promise<MessageLite> promise) {
        switch (strategy) {
            case TOPIC -> {
                topicSRaftLog.postCommit((TopicRecord) record.getRecord(), record.getType());
            }
            case CLUSTER -> {
                clusterSRaftLog.postCommit((NodeRecord) record.getRecord(), record.getType());
            }
        }
    }

    public Set<SocketAddress> toSocketAddress() {
       String[] votersArray = config.getControllerQuorumVoters().split(",");
       return Stream.of(votersArray)
               .map(voters -> {
                   final int length = voters.length();
                   final String newVoters = voters.substring(voters.lastIndexOf("@") + 1, length);
                   return switchSocketAddress(newVoters);
               }).collect(Collectors.toSet());
    }

    public Set<SocketAddress> toSocketAddressWithoutSelf() {
        return toSocketAddress()
                .stream()
                .filter(f -> !Objects.equals(switchSocketAddress(config.getExposedHost(), config.getExposedPort()), f))
                .collect(Collectors.toSet());
    }

    public void shutdownGracefully() throws Exception {
        heartbeat.shutdownGracefully();
    }
}
