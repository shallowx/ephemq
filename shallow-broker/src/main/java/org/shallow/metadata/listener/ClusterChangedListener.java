package org.shallow.metadata.listener;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.shallow.internal.BrokerManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.network.BrokerConnectionManager;
import org.shallow.processor.AwareInvocation;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.ProtoBufUtil;

import java.io.IOException;
import java.util.Iterator;

import static org.shallow.processor.ProcessCommand.Client.CLUSTER_CHANGED;

public class ClusterChangedListener implements ClusterListener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterChangedListener.class);

    private final BrokerManager manager;

    public ClusterChangedListener(BrokerManager manager) {
        this.manager = manager;
    }

    @Override
    public void onServerRegistration(String nodeId, String host, int port) {
        if (logger.isDebugEnabled()) {
            logger.debug("This server is registered successfully, name={} host={} post={}", nodeId, host, port);
        }
        handleServerOnChanged(nodeId, host, port);
    }

    @Override
    public void onServerOffline(String nodeId, String host, int port) {
        if (logger.isDebugEnabled()) {
            logger.debug("This server is offline successfully, name={} host={} post={}", nodeId, host, port);
        }
        handleServerOnChanged(nodeId, host, port);
    }

    private void handleServerOnChanged(String nodeId, String host, int port) {
        BrokerConnectionManager connectionManager = manager.getBrokerConnectionManager();

        if (connectionManager.isEmpty()) {
            return;
        }

        for (Channel channel : connectionManager) {
            ByteBuf buf = null;
            try {
                buf = applyServerOfflineData(channel, nodeId, host, port);
                AwareInvocation invocation = AwareInvocation.newInvocation(CLUSTER_CHANGED, (short) 0, buf, (byte) 0);
                channel.writeAndFlush(invocation);
            } catch (Throwable t) {
                ByteBufUtil.release(buf);
                throw new RuntimeException(String.format("Failed to handle server offline, name=%s host=%s port=%d", nodeId, host, port));
            }
        }
    }

    private ByteBuf applyServerOfflineData(Channel channel, String nodeId, String host, int port) throws IOException {
        NodeOfflineSignal signal = NodeOfflineSignal
                .newBuilder()
                .setNodeId(nodeId)
                .setHost(host)
                .setPort(port)
                .build();

        int length = ProtoBufUtil.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);

        ProtoBufUtil.writeProto(buf, signal);

        return buf;
    }
}
