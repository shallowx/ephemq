package org.shallow.metadata.sraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.metadata.management.ClusterManager;
import org.shallow.metadata.Strategy;
import org.shallow.metadata.management.TopicManager;
import org.shallow.proto.elector.VoteResponse;

import javax.annotation.concurrent.ThreadSafe;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.shallow.util.NetworkUtil.*;

@ThreadSafe
public class SRaftProcessController {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftProcessController.class);

    private final BrokerConfig config;
    private SRaftHeartbeat heartbeat;
    private TopicManager topicManager;
    private ClusterManager clusterManager;
    private final DistributedAtomicInteger atomicValue;
    private SocketAddress metadataLeader;
    private final BrokerManager manager;
    public SRaftProcessController(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.manager = manager;

        this.atomicValue = new DistributedAtomicInteger();
    }

    public void start() throws Exception {
        checkQuorumVoters();

        this.LazyInitialize();
        topicManager.start();
        clusterManager.start();
        if (isQuorumLeader()) {
            if (logger.isInfoEnabled()) {
                logger.info("The model is stand alone, and the node<name={} host={} port={}> is elected as leader", config.getServerId(), config.getExposedHost(), config.getExposedPort());
            }
            return;
        }

        heartbeat.start();
    }

    private void LazyInitialize() {
        this.heartbeat = new SRaftHeartbeat(config, manager);
        this.topicManager = new TopicManager(toSocketAddress(true), config, manager);
        this.clusterManager = new ClusterManager(toSocketAddress(true), config, manager);
    }

    private void checkQuorumVoters() {
        Set<SocketAddress> quorumVoters = toSocketAddress(false);
        if (isQuorumLeader()) {
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

    public void receiveHeartbeat(int term, int distributedValue, SocketAddress metadataLeader) {
        this.setMetadataLeader(metadataLeader);
        heartbeat.receiveHeartbeat(term);
        atomicValue.trySet(distributedValue);
    }

    public void prepareCommit(Strategy strategy, CommitRecord<?> record, Promise<MessageLite> promise) {
        switch (strategy) {
            case TOPIC -> {
                topicManager.prepareCommit((TopicRecord) record.getRecord(), record.getType(), promise);
            }

            case CLUSTER -> {
                clusterManager.prepareCommit((NodeRecord) record.getRecord(), record.getType(), promise);
            }
        }
    }

    public void postCommit(Strategy strategy, CommitRecord<?> record, Promise<MessageLite> promise) {
        switch (strategy) {
            case TOPIC -> {
                topicManager.postCommit((TopicRecord) record.getRecord(), record.getType());
            }
            case CLUSTER -> {
                clusterManager.postCommit((NodeRecord) record.getRecord(), record.getType());
            }
        }
    }

    public Set<SocketAddress> toSocketAddress(boolean excludeSelf) {
       String[] votersArray = config.getControllerQuorumVoters().split(",");
       return Stream.of(votersArray)
               .map(voters -> {
                   int length = voters.length();
                   String newVoters = voters.substring(voters.lastIndexOf("@") + 1, length);
                   return switchSocketAddress(newVoters);
               }).filter(f -> {
                   if (!excludeSelf) {
                       return true;
                   }
                   return !Objects.equals(switchSocketAddress(config.getExposedHost(), config.getExposedPort()), f);
               }).collect(Collectors.toSet());
    }

    public void setMetadataLeader(SocketAddress metadataLeader) {
        this.metadataLeader = metadataLeader;
    }

    public SocketAddress getMetadataLeader() {
        return metadataLeader;
    }

    public boolean isQuorumLeader() {
        return config.isStandAlone() || metadataLeader.equals(switchSocketAddress(config.getExposedHost(), config.getExposedPort()));
    }

    public TopicManager getTopicManager() {
        return topicManager;
    }

    public DistributedAtomicInteger getAtomicValue() {
        return atomicValue;
    }

    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    public void shutdownGracefully() throws Exception {
        heartbeat.shutdownGracefully();
    }
}
