package org.shallow.metadata.raft;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.ClientChannel;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.elector.VoteRequest;
import org.shallow.proto.elector.VoteResponse;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.Server.QUORUM_VOTE;
import static org.shallow.util.NetworkUtil.*;

@SuppressWarnings("all")
public class LeaderElector {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(HeartbeatProcessor.class);

    private final BrokerConfig config;
    private final HeartbeatProcessor heartbeatProcessor;
    private final EventExecutor electEventExecutor;
    private final List<String> quorumAddress;
    private final ShallowChannelPool pool;
    private RaftPeer peer = RaftPeer.FOLLOWER;
    private final RaftEvent event;
    private boolean isAllowedVote = true;
    private final DistributedAtomicInteger atomicValue;
    private long lastKeepTime = System.currentTimeMillis();

    public LeaderElector(BrokerConfig config, List<String> quorumAddress, RaftQuorumClient client, DistributedAtomicInteger atomicValue) {
        this.config = config;
        this.atomicValue = atomicValue;
        this.quorumAddress = quorumAddress;
        this.heartbeatProcessor = new HeartbeatProcessor(config, client, quorumAddress, this);
        this.pool = client.getChanelPool();
        this.event = RaftEvent.newBuilder().build();
        this.electEventExecutor = newEventExecutorGroup(1, "raft-leader-elect").next();
    }

    public void registerElector() {
        if (electEventExecutor == null || electEventExecutor.isShutdown()) {
            throw new RuntimeException("Leader elect executor is shutdown");
        }

        int size = quorumAddress.size();
        if (size <=1) {
            throw new IllegalArgumentException("Quorum address size <= 1, but expect = 1");
        }

        if (size % 2 == 0) {
            throw new IllegalArgumentException("Quorum address (size % 2 == 0) is true, but expect (size % 2 == 1) is true");
        }

        Promise<Void> promise = electEventExecutor.newPromise();
        promise.addListener(f -> {
            if (!f.isSuccess()) {
                electEventExecutor.scheduleWithFixedDelay(new LeaderElectionScheduleTask(quorumAddress, pool),  ThreadLocalRandom.current().nextInt(500), 500 ,  TimeUnit.MILLISECONDS);
            }
        });

        try {
            electEventExecutor.scheduleWithFixedDelay(new LeaderElectionScheduleTask(quorumAddress, pool),  ThreadLocalRandom.current().nextInt(500), 500 ,  TimeUnit.MILLISECONDS);
            promise.trySuccess(null);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private class LeaderElectionScheduleTask implements Runnable {

        private static final InternalLogger logger = InternalLoggerFactory.getLogger(LeaderElectionScheduleTask.class);

        private final List<String> quorumAddress;
        private final ShallowChannelPool pool;

        public LeaderElectionScheduleTask(List<String> quorumAddress, ShallowChannelPool pool) {
            this.quorumAddress = quorumAddress;
            this.pool = pool;
        }

        @Override
        public void run() {
            if (peer == RaftPeer.LEADER) {
                return;
            }

            if (peer == RaftPeer.CANDIDATE) {
                peer = RaftPeer.FOLLOWER;
            }

            if (!isAllowedVote) {
                return;
            }

            long now = System.currentTimeMillis();
            if (now - lastKeepTime < 500) {
                return;
            }

            peer = RaftPeer.CANDIDATE;

            for (String address : quorumAddress) {
                SocketAddress voteAddress = null;
                try {
                    voteAddress = switchSocketAddress(address);
                    sendVoteRequest(voteAddress);
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel<address={}> send vote request failure, and it will give up. error:{}", voteAddress.toString(), t);
                    }
                }
            }
        }

        private int votes = 1;
        private void sendVoteRequest(SocketAddress voteAddress) {
            VoteRequest.Builder builder = VoteRequest.newBuilder();

            builder.setTerm(event.getTerm() + 1);
            builder.setVersion(event.getVersion());

            ClientChannel clientChannel = pool.acquireHealthyOrNew(voteAddress);

            Promise<VoteResponse> promise = electEventExecutor.newPromise();
            promise.addListener(new GenericFutureListener<Future<VoteResponse>>() {
                @Override
                public void operationComplete(Future<VoteResponse> future) throws Exception {
                    if (future.isSuccess()) {
                        VoteResponse response = future.get();
                        if (response.getAck()) {
                            votes++;
                            if (peer != RaftPeer.LEADER && isAllowedVote && votes >= ((quorumAddress.size() - 1) >> 2)) {
                                peer = RaftPeer.LEADER;

                                event.setLeader(config.getServerId());
                                heartbeatProcessor.registerHeartbeat();
                            }
                        }
                    }
                }
            });

            clientChannel.invoker().invoke(QUORUM_VOTE, config.getInvokeTimeMs(), promise, builder.build(), VoteResponse.class);
        }
    }

    public int getTerm() {
        return event.getTerm();
    }

    public int getVersion() {
        return event.getVersion();
    }

    public void setAllowedVote(boolean vote) {
        this.isAllowedVote = vote;
    }

    public void setLastKeepTime(long lastKeepTime) {
        this.lastKeepTime = lastKeepTime;
    }

    public RaftPeer getPeer() {
        return peer;
    }

    public DistributedAtomicInteger getAtomicValue() {
        return atomicValue;
    }

    public void setTerm(int term) {
        event.setTerm(term);
    }

    public void setVersion(int version) {
        event.setVersion(version);
    }

    public void setLeader(String leader) {
        event.setLeader(leader);
    }

    public String getLeader() {
        return event.getLeader();
    }

    public void setAddress(SocketAddress address) {
        event.setLeaderAddress(address);
    }

    public SocketAddress getAddress() {
       return event.getLeaderAddress();
    }

    public void shutdownGracefully() throws Exception {
        if (electEventExecutor != null && !electEventExecutor.isShutdown()) {
            electEventExecutor.shutdownGracefully();
        }

        heartbeatProcessor.shutdownGracefully();
    }
}
