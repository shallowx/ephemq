package org.shallow.metadata.sraft;

import io.netty.util.concurrent.*;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.server.HeartBeatRequest;
import org.shallow.proto.server.HeartBeatResponse;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.Server.HEARTBEAT;
import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class SRaftHeartbeat {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftHeartbeat.class);

    private final BrokerConfig config;
    private final SRaftProcessController controller;
    private final SRaftQuorumVoter quorumVoter;
    private final EventExecutor scheduleQuorumVoteTask;
    private final EventExecutor scheduleHeartbeatTask;
    private long lastKeepHeartbeatTime;

    public SRaftHeartbeat(BrokerConfig config, SRaftProcessController controller) {
        this.config = config;
        this.controller = controller;
        final EventExecutorGroup group = newEventExecutorGroup(2, "sraft-heartbeat");
        this.scheduleQuorumVoteTask =  group.next();
        this.scheduleHeartbeatTask =  group.next();
        this.quorumVoter = new SRaftQuorumVoter(config, controller);
    }

    public void start() throws Exception {
        registerQuorumVote();
    }

    private void registerQuorumVote() {
        scheduleQuorumVoteTask.scheduleAtFixedRate(this::doRegisterQuorumVote,
                config.getHeartInitialDelayTimeMs(),
                ThreadLocalRandom.current().nextInt(config.getHeartbeatIntervalOriginTimeMs(),
                        config.getHeartbeatRandomBoundTimeMs()), TimeUnit.MILLISECONDS
        );
    }

    // TODO elect timeout
    private void doRegisterQuorumVote() {
        final Promise<Boolean> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
            if (f.isSuccess() && f.get()) {
                registerHeartbeat();
            }
        });

        final ProcessRoles role = quorumVoter.getSRaftRole();
        if (role == ProcessRoles.Follower) {
            final long now = System.currentTimeMillis();
            if ((now - lastKeepHeartbeatTime) > config.getHeartbeatFixedIntervalTimeMs()) {
                quorumVoter.quorumVote();
            }
        }
    }

    private void registerHeartbeat() {
        scheduleHeartbeatTask.scheduleAtFixedRate(this::doRegisterHeartbeat, 0, config.getHeartbeatFixedIntervalTimeMs(), TimeUnit.MILLISECONDS);
    }

    private void doRegisterHeartbeat() {
        final ProcessRoles role = quorumVoter.getSRaftRole();
        if (role == ProcessRoles.LEADER) {
            final Set<SocketAddress> socketAddresses = controller.toSocketAddress();

            final HeartBeatRequest request = HeartBeatRequest.newBuilder().build();
            for (SocketAddress address : socketAddresses) {
                try {
                    ClientChannel clientChannel = quorumVoter.acquire(address);
                    clientChannel.invoker().invokeWithCallback(HEARTBEAT, config.getInvokeTimeMs(), request, HeartBeatResponse.class);
                } catch (Throwable t) {
                    // TODO retry
                }
            }
        }
    }

    public void receiveHeartbeat() {
        this.lastKeepHeartbeatTime = System.currentTimeMillis();
    }

    public SRaftQuorumVoter getQuorumVoter() {
        return quorumVoter;
    }

    public void shutdownGracefully() throws Exception {
        scheduleHeartbeatTask.shutdownGracefully();
    }
}
