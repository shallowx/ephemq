package org.ostara.core.inner;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.ostara.client.ClientConfig;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientListener;
import org.ostara.core.Config;
import org.ostara.management.Manager;
import org.ostara.metrics.MetricsConstants;

import java.net.SocketAddress;

import static org.ostara.metrics.MetricsConstants.TYPE_TAG;

public class InnerClient extends Client {
    private final Config config;
    private final Manager manager;

    public InnerClient(String name, ClientConfig clientConfig, ClientListener listener, Config config, Manager manager) {
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
                    .tag(MetricsConstants.CLUSTER_TAG, config.getClusterName())
                    .tag(MetricsConstants.BROKER_TAG, config.getServerId())
                    .tag(TYPE_TAG, "client-task")
                    .tag("name", name)
                    .tag("id", executor.threadProperties().name())
                    .register(meterRegistry);
        }

        for (EventExecutor eventExecutor : workerGroup) {
            SingleThreadEventExecutor executor = (SingleThreadEventExecutor) eventExecutor;
            Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                    .tag(MetricsConstants.CLUSTER_TAG, config.getClusterName())
                    .tag(MetricsConstants.BROKER_TAG, config.getServerId())
                    .tag(TYPE_TAG, "client-task")
                    .tag("name", name)
                    .tag("id", executor.threadProperties().name())
                    .register(meterRegistry);
        }

    }
}
