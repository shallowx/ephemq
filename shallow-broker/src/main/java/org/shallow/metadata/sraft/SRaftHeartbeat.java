package org.shallow.metadata.sraft;

import io.netty.util.concurrent.*;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.elector.RaftHeartbeatRequest;
import org.shallow.proto.server.RegisterNodeResponse;

import javax.annotation.concurrent.ThreadSafe;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.Server.HEARTBEAT;
import static org.shallow.util.NetworkUtil.*;

@ThreadSafe
public class SRaftHeartbeat {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftHeartbeat.class);

    private final BrokerConfig config;
    private final SRaftProcessController controller;
    private final SRaftQuorumVoter quorumVoter;
    private final EventExecutor scheduleQuorumVoteTask;
    private final EventExecutor scheduleHeartbeatTask;
    private long lastKeepHeartbeatTime;

    public SRaftHeartbeat(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.controller = manager.getController();
        final EventExecutorGroup group = newEventExecutorGroup(2, "sraft-heartbeat");
        this.scheduleQuorumVoteTask =  group.next();
        this.scheduleHeartbeatTask =  group.next();
        this.quorumVoter = new SRaftQuorumVoter(config, controller);
    }

    public void start() throws Exception {
        registerQuorumVote();
        quorumVoter.start();
    }

    private void registerQuorumVote() {
        scheduleQuorumVoteTask.scheduleAtFixedRate(this::doRegisterQuorumVote,
                config.getHeartInitialDelayTimeMs(),
                ThreadLocalRandom.current().nextInt(config.getHeartbeatIntervalOriginTimeMs(),
                        config.getHeartbeatRandomBoundTimeMs()), TimeUnit.MILLISECONDS);
    }

    private void doRegisterQuorumVote() {
        Promise<Boolean> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
            if (f.isSuccess() && f.get()) {
                if (logger.isInfoEnabled()) {
                    logger.info("The node<name={} host={} port={}> is elected as leader", config.getServerId(), config.getExposedHost(), config.getExposedPort());
                }
                controller.setMetadataLeader(switchSocketAddress(config.getExposedHost(), config.getExposedPort()));
                registerHeartbeat();
            }
        });

        ProcessRoles role = quorumVoter.getSRaftRole();
        if (role == ProcessRoles.Follower) {
            long now = System.currentTimeMillis();
            if ((now - lastKeepHeartbeatTime) > config.getHeartbeatFixedIntervalTimeMs()) {
                quorumVoter.quorumVote(promise);
            }
        }
    }

    private void registerHeartbeat() {
        scheduleHeartbeatTask.scheduleAtFixedRate(this::doRegisterHeartbeat, 0, config.getHeartbeatFixedIntervalTimeMs(), TimeUnit.MILLISECONDS);
    }

    public void stopHeartbeat() {

    }

    private void doRegisterHeartbeat() {
        ProcessRoles role = quorumVoter.getSRaftRole();
        if (role == ProcessRoles.LEADER) {
            Set<SocketAddress> socketAddresses = controller.toSocketAddress(true);

            RaftHeartbeatRequest request = RaftHeartbeatRequest
                    .newBuilder()
                    .setTerm(quorumVoter.getTerm())
                    .setDistributedValue(controller.getAtomicValue().get().preValue())
                    .build();

            for (SocketAddress address : socketAddresses) {
                try {
                    ClientChannel clientChannel = quorumVoter.acquire(address);
                    clientChannel.invoker().invoke(HEARTBEAT, config.getInvokeTimeMs(), request, RegisterNodeResponse.class);
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Failed to send heartbeat to address<{}>, trg again later", address);
                    }
                }
            }
        }
    }

    public void receiveHeartbeat(int term) {
        this.lastKeepHeartbeatTime = System.currentTimeMillis();
        quorumVoter.setTerm(term);
    }

    public SRaftQuorumVoter getQuorumVoter() {
        return quorumVoter;
    }

    public void shutdownGracefully() throws Exception {
        scheduleHeartbeatTask.shutdownGracefully();
    }
}
