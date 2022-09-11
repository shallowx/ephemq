package org.shallow.metadata.sraft;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.ClientChannel;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.elector.RaftHeartbeatRequest;
import org.shallow.proto.elector.RaftHeartbeatResponse;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class HeartbeatProcessor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(HeartbeatProcessor.class);

    private final BrokerConfig config;
    private final EventExecutor heartbeatExecutor;
    private final ShallowChannelPool pool;
    private final List<String> quorumAddress;
    private final LeaderElector leaderElector;
    private final EventExecutor retryExecutor;

    public HeartbeatProcessor(BrokerConfig config, RaftQuorumClient client, List<String> quorumAddress, LeaderElector leaderElector) {
        this.config = config;
        this.quorumAddress = quorumAddress;
        this.pool = client.getChanelPool();
        this.leaderElector = leaderElector;
        this.heartbeatExecutor = newEventExecutorGroup(1, "raft-heartbeat").next();
        this.retryExecutor = newEventExecutorGroup(1, "raft-heartbeat-retry").next();
    }

    public void registerHeartbeat() {
        if (heartbeatExecutor == null || heartbeatExecutor.isShutdown()) {
            throw new RuntimeException("Heartbeat event executor is shutdown");
        }

        heartbeatExecutor.scheduleWithFixedDelay(new HeartbeatScheduleTask(), 0, 500, TimeUnit.MILLISECONDS);
    }

    private class HeartbeatScheduleTask implements Runnable {

        private static final InternalLogger logger = InternalLoggerFactory.getLogger(HeartbeatScheduleTask.class);

        @Override
        public void run() {
            RaftPeer peer = leaderElector.getPeer();
            if (peer != RaftPeer.LEADER) {
                return;
            }

            for (String address : quorumAddress) {
                sendHeartbeat(address);
            }
        }

        private void sendHeartbeat(String address) {
            DistributedAtomicInteger atomicValue = leaderElector.getAtomicValue();
            int currentAtomicValue = atomicValue.get().preValue();
            RaftHeartbeatRequest request = RaftHeartbeatRequest
                    .newBuilder()
                    .setTerm(leaderElector.getTerm())
                    .setVersion(leaderElector.getVersion())
                    .setLeader(config.getServerId())
                    .setDistributedValue(currentAtomicValue)
                    .build();

            Promise<RaftHeartbeatResponse> promise = retryExecutor.newPromise();
            promise.addListener(future -> {
                if (!future.isSuccess()) {
                    if (retryExecutor.isShutdown()) {
                        return;
                    }

                    RaftPeer peer = leaderElector.getPeer();
                    if (peer != RaftPeer.LEADER) {
                        return;
                    }

                    retryExecutor.schedule(() -> sendHeartbeat(address), 0, TimeUnit.MILLISECONDS);
                }
            });

            try {
                SocketAddress socketAddress = switchSocketAddress(address);
                ClientChannel clientChannel = pool.acquireHealthyOrNew(socketAddress);

                clientChannel.invoker().invoke(ProcessCommand.Server.HEARTBEAT, config.getInvokeTimeMs(), promise, request, RaftHeartbeatResponse.class);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Send heartbeat failure, and try again later, address={}", address);
                }
                promise.tryFailure(t);
            }
        }
    }

    public void shutdownGracefully() throws Exception {
        if (heartbeatExecutor != null && !heartbeatExecutor.isShutdown()) {
            heartbeatExecutor.shutdownGracefully();
        }
    }
}
