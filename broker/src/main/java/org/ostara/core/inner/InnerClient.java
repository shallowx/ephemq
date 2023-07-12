package org.ostara.core.inner;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.ostara.client.internal.ClientConfig;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientListener;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.CoreConfig;
import org.ostara.management.Manager;

import java.net.SocketAddress;

import static org.ostara.metrics.MetricsConstants.*;

public class InnerClient extends Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InnerClient.class);
    private final CoreConfig config;
    private final Manager manager;

    public InnerClient(String name, ClientConfig clientConfig, ClientListener listener, CoreConfig config, Manager manager) {
        super(name, clientConfig, listener);
        this.config = config;
        this.manager = manager;
    }

    @Override
    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new InnerClientChannel(clientConfig, channel, address, config, manager);
    }

    public void bindTo(MeterRegistry meterRegistry) {
        {
            SingleThreadEventExecutor executor = (SingleThreadEventExecutor) taskExecutor;
            Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag(TYPE_TAG, "client-task")
                    .tag(NAME, name)
                    .tag(ID, executor.threadProperties().name())
                    .register(meterRegistry);
        }

        for (EventExecutor eventExecutor : workerGroup) {
            try {
                SingleThreadEventExecutor executor = (SingleThreadEventExecutor) eventExecutor;
                Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                        .tag(CLUSTER_TAG, config.getClusterName())
                        .tag(BROKER_TAG, config.getServerId())
                        .tag(TYPE_TAG, "client-task")
                        .tag(NAME, name)
                        .tag(ID, executor.threadProperties().name())
                        .register(meterRegistry);
            } catch (Throwable t){
                logger.error("Inner client bind failed, executor={}", eventExecutor.toString(), t);
            }
        }

    }
}
