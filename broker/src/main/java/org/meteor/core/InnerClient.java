package org.meteor.core;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.meteor.configuration.CommonConfiguration;
import org.meteor.management.Manager;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientChannel;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.internal.ClientListener;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import java.net.SocketAddress;

import static org.meteor.metrics.MetricsConstants.*;

public class InnerClient extends Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InnerClient.class);
    private final CommonConfiguration configuration;
    private final Manager manager;

    public InnerClient(String name, ClientConfig clientConfig, ClientListener listener, CommonConfiguration configuration, Manager manager) {
        super(name, clientConfig, listener);
        this.configuration = configuration;
        this.manager = manager;
    }

    @Override
    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new InnerClientChannel(clientConfig, channel, address, configuration, manager);
    }

    public void bindTo(MeterRegistry meterRegistry) {
        {
            SingleThreadEventExecutor executor = (SingleThreadEventExecutor) taskExecutor;
            Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, configuration.getClusterName())
                    .tag(BROKER_TAG, configuration.getServerId())
                    .tag(TYPE_TAG, "client-task")
                    .tag(NAME, name)
                    .tag(ID, executor.threadProperties().name())
                    .register(meterRegistry);
        }

        for (EventExecutor eventExecutor : workerGroup) {
            try {
                SingleThreadEventExecutor executor = (SingleThreadEventExecutor) eventExecutor;
                Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                        .tag(CLUSTER_TAG, configuration.getClusterName())
                        .tag(BROKER_TAG, configuration.getServerId())
                        .tag(TYPE_TAG, "client-task")
                        .tag(NAME, name)
                        .tag(ID, executor.threadProperties().name())
                        .register(meterRegistry);
            } catch (Throwable t) {
                logger.error("Inner client bind failed, executor={}", eventExecutor.toString(), t);
            }
        }

    }
}
