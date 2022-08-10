package org.shallow.metadata.sraft;

import io.netty.util.concurrent.Promise;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.elector.VoteResponse;
import java.net.SocketAddress;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class SRaftProcessController {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftProcessController.class);

    private final BrokerConfig config;
    private final SRaftHeartbeat heartbeat;

    public SRaftProcessController(BrokerConfig config) {
        this.config = config;
        this.heartbeat = new SRaftHeartbeat(config, this);
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
        final Set<SocketAddress> quorumVoters = toSocketAddress();
        if (config.isStandAlone()) {
            if (quorumVoters.size() != 1) {
                throw new IllegalArgumentException(String.format("The model is stand alone, and the quorum voters<shallow.controller.quorum.voters> value expected = 1, but actual = %d", quorumVoters.size()));
            }
            return;
        }

        if (quorumVoters.size() < 3) {
            throw new IllegalArgumentException(String.format("The quorum voters<shallow.controller.quorum.voters> value expected >= 3, but actual = %d", quorumVoters.size()));
        }

        if (quorumVoters.size() % 2 == 0) {
            throw new IllegalArgumentException("The quorum voters<shallow.controller.quorum.voters> value expected to be odd, but actually even, for example, it should be 3, not 4");
        }
    }

    public void respondVote(int term, Promise<VoteResponse> promise) {
        final SRaftQuorumVoter quorumVoter = heartbeat.getQuorumVoter();
        if (quorumVoter.getTerm() >= term) {
            final VoteResponse response = VoteResponse.newBuilder().setAck(false).build();
            promise.trySuccess(response);

            return;
        }

        final Promise<VoteResponse> respondVotePromise = newImmediatePromise();
        respondVotePromise.addListener(f -> {
            if (f.isSuccess()) {
                promise.trySuccess((VoteResponse) f.get());
                heartbeat.stopHeartbeat();
            }
        });

        quorumVoter.respondVote(respondVotePromise);
    }

    public void receiveHeartbeat() {
        heartbeat.receiveHeartbeat();
    }

    public Set<SocketAddress> toSocketAddress() {
       final String[] votersArray = config.getControllerQuorumVoters().split(",");
       return Stream.of(votersArray)
               .map(voters -> {
                   final int length = voters.length();
                   final String newVoters = voters.substring(voters.lastIndexOf("@") + 1, length);
                   return switchSocketAddress(newVoters);
               }).collect(Collectors.toSet());
    }

    public void shutdownGracefully() throws Exception {
        heartbeat.shutdownGracefully();
    }
}
