package org.shallow.nraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.*;
import org.shallow.internal.MetadataConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import java.net.SocketAddress;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.shallow.util.NetworkUtil.switchSocketAddress;
import static org.shallow.util.ObjectUtil.isNotNull;

public class RaftLeaderElector implements LeaderElector {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RaftLeaderElector.class);

    private RaftRole role;
    private int term;
    private final MetadataConfig config;
    private final VoteListener voteListener;
    private final HeartbeatListener heartBeatListener;
    private final EventExecutor voteAndHeartbeatEventExecutor;
    private SocketAddress leader;

    public RaftLeaderElector(EventExecutorGroup group, MetadataConfig config) {
        this.role = RaftRole.Follower;
        this.voteListener = new RaftVoteListener(config, group.next(), this);
        this.heartBeatListener = new RaftHeartbeatListener(config, group.next());
        this.voteAndHeartbeatEventExecutor = group.next();
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        registerHeartbeatScheduledTask();
    }

    private void registerHeartbeatScheduledTask() {
        final Runnable task = () -> {
            if (role != RaftRole.Leader) {
                long now = System.currentTimeMillis();
                if ((now - heartBeatListener.getHeartbeatTime()) > config.getHeartMaxIntervalTimeMs()) {
                    Promise<Boolean> promise = voteAndHeartbeatEventExecutor.newPromise();
                    promise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
                        if (f.isSuccess() && f.get()) {
                            voteListener.revert();

                            ++term;
                            if (!isLeader()) {
                                role = RaftRole.Leader;
                                final String host = config.getExposedHost();
                                final int port = config.getExposedPort();

                                leader = switchSocketAddress(host, port);
                                if (logger.isInfoEnabled()) {
                                    logger.info("The node<host={} port={}> is elected as the cluster master", host, port);
                                }
                                heartBeatListener.registerHeartbeat();
                            }
                        }
                    });

                    if (!isCandidate()) {
                        role = RaftRole.Candidate;
                    }

                   voteListener.revert();
                   voteListener.vote(promise);
                   return;
                }

                if (!isFollower()) {
                    role = RaftRole.Follower;
                }
            }
        };

        voteAndHeartbeatEventExecutor.scheduleAtFixedRate(task, ThreadLocalRandom.current().nextInt(300,1000),
                config.getRetryLeaderElectScheduledDelayMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void respond(int term, Promise<? super MessageLite> promise) {
        if (voteAndHeartbeatEventExecutor.inEventLoop()) {
           doRespond(term, promise);
        } else {
            voteAndHeartbeatEventExecutor.execute(() -> doRespond(term, promise));
        }
    }

    private void doRespond(int term, Promise<? super MessageLite> promise) {
        voteListener.answer(term, promise);
    }

    @Override
    public void keepHeartbeat() {
        voteListener.revert();
        heartBeatListener.receive(System.currentTimeMillis());
    }

    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public boolean isLeader() {
        return role == RaftRole.Leader;
    }

    @Override
    public boolean isFollower() {
        return role == RaftRole.Follower;
    }

    @Override
    public boolean isCandidate() {
        return role == RaftRole.Candidate;
    }

    @Override
    public boolean hasLeader() {
        return isNotNull(leader);
    }
}
