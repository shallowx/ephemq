package org.shallow.metadata.sraft;

import io.netty.util.concurrent.Promise;
import org.shallow.ClientConfig;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.metadata.MappingFileProcessor;
import org.shallow.metadata.snapshot.ClusterSnapshot;
import org.shallow.metadata.snapshot.TopicSnapshot;

import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.List;

public class RaftVoteProcessor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RaftVoteProcessor.class);

    private final MappingFileProcessor fileProcessor;
    private final BrokerConfig config;
    private final LeaderElector leaderElector;
    private final StandAloneProcessor standAloneProcessor;
    private final RaftQuorumClient client;
    private final List<String> quorumAddress;
    private final DistributedAtomicInteger atomicValue;
    private final ClusterSnapshot clusterSnapshot;
    private final TopicSnapshot topicSnapshot;

    public RaftVoteProcessor(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.atomicValue = new DistributedAtomicInteger();
        this.fileProcessor = new MappingFileProcessor(config);

        quorumAddress = switchSocketAddress();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(quorumAddress);
        this.client = new RaftQuorumClient("raft-vote-client", clientConfig);

        this.leaderElector = new LeaderElector(config, quorumAddress, client, atomicValue);
        this.standAloneProcessor = new StandAloneProcessor(quorumAddress, config);

        this.clusterSnapshot = new ClusterSnapshot(fileProcessor, config, this, client, manager);
        this.topicSnapshot = new TopicSnapshot(fileProcessor, config, atomicValue, this, manager, client);
    }

    private List<String> switchSocketAddress() {
        String controllerQuorumVoters = config.getControllerQuorumVoters();
        if (controllerQuorumVoters.length() == 0) {
            throw new IllegalArgumentException("");
        }

        List<String> addresses = new LinkedList<>();
        String[] voterArray = controllerQuorumVoters.split(",");
        for (String voter : voterArray) {
            int pre = voter.lastIndexOf("@");
            String address = voter.substring(pre + 1);

            addresses.add(address);
        }

        return addresses;
    }

    public void start() throws Exception {
        checkConfiguration();
        fileProcessor.start();

        client.start();
        standAloneProcessor.start(() -> {
            try {
                leaderElector.registerElector();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        clusterSnapshot.start();
        topicSnapshot.start();
    }

    public void handleVoteRequest(int term, int version, Promise<Boolean> promise) {
        int currentTerm = leaderElector.getTerm();
        int currentVersion = leaderElector.getVersion();

        if (term > currentTerm || version > currentVersion) {
            promise.trySuccess(true);
            leaderElector.setAllowedVote(false);
            return;
        }

        promise.trySuccess(false);
    }

    public void handleHeartbeatRequest(SocketAddress address, String leader, int version, int term, int distributedValue) {
        leaderElector.setLastKeepTime(System.currentTimeMillis());

        if (distributedValue != atomicValue.get().preValue()) {
            atomicValue.trySet(distributedValue);
        }

        int currentVersion = leaderElector.getVersion();
        if (version != currentVersion) {
            leaderElector.setLeader(leader);
            leaderElector.setTerm(term);
            leaderElector.setVersion(version);
            leaderElector.setAddress(address);

            clusterSnapshot.fetchFromQuorumLeader();
            topicSnapshot.fetchFromQuorumLeader();
        }
    }

    private void checkConfiguration() {
        int size = quorumAddress.size();
        if (size == 0) {
            throw new IllegalArgumentException("Quorum address size expect > 0");
        }
    }

    public LeaderElector getLeaderElector() {
        return leaderElector;
    }

    public ClusterSnapshot getClusterSnapshot() {
        return clusterSnapshot;
    }

    public TopicSnapshot getTopicSnapshot() {
        return topicSnapshot;
    }

    public void shutdownGracefully() throws Exception {
        clusterSnapshot.unRegisterNode();
        if (client != null) {
            client.shutdownGracefully();
        }
        leaderElector.shutdownGracefully();
    }
}
