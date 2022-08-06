package org.shallow.nraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.internal.MetadataConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.DefaultChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.elector.RaftHeartbeatRequest;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.processor.ProcessCommand.NameServer.RAFT_HEARTBEAT;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class RaftHeartbeatListener implements HeartbeatListener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RaftHeartbeatListener.class);

    private long lastKeepHeartBeatTime;
    private final MetadataConfig config;
    private final ShallowChannelPool pool;
    private final EventExecutor heartbeatTaskExecutor;

    public RaftHeartbeatListener(MetadataConfig config, EventExecutorGroup group) {
        this.config = config;
        this.pool = DefaultChannelPoolFactory.INSTANCE.acquireChannelPool();
        this.heartbeatTaskExecutor = group.next();
    }


    @Override
    public void registerHeartbeat() {
        heartbeatTaskExecutor.scheduleAtFixedRate(
                this::doRegisterHeartScheduledSendTask,
                0,
                config.getRetryLeaderElectScheduledDelayMs(),
                TimeUnit.MILLISECONDS
        );
    }

    private void doRegisterHeartScheduledSendTask() {
        Set<SocketAddress> socketAddresses = toSocketAddress();
        if (socketAddresses.isEmpty()) {
            return;
        }

        final RaftHeartbeatRequest request = RaftHeartbeatRequest.newBuilder().build();
        for (SocketAddress address : socketAddresses) {
            try {
                ClientChannel channel = acquireChannel(address);
                channel.invoker().invoke(RAFT_HEARTBEAT, 1000, newImmediatePromise(), request, MessageLite.class);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("[doVote] - failed to send vote request with socket address<{}>", address.toString());
                }
            }
        }
    }

    private Set<SocketAddress> toSocketAddress() {
        return Arrays.stream(config.getClusterUrl().split(","))
                .filter(f -> !f.equals(config.getExposedHost() + ":" + config.getExposedPort()))
                .map(NetworkUtil::switchSocketAddress)
                .collect(Collectors.toSet());
    }

    private ClientChannel acquireChannel(SocketAddress address) {
        return pool.acquireHealthyOrNew(address);
    }

    @Override
    public void receive(long time) {
        this.lastKeepHeartBeatTime = time;
    }

    @Override
    public long getHeartbeatTime() {
        return lastKeepHeartBeatTime;
    }
}
