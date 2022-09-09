package org.shallow.metadata.listener;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.shallow.internal.BrokerManager;
import org.shallow.network.BrokerConnectionManager;
import org.shallow.processor.AwareInvocation;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.ProtoBufUtil;

import java.io.IOException;

import static org.shallow.processor.ProcessCommand.Client.CLUSTER_CHANGED;

public class ClusterChangedListener implements ClusterListener {

    private final BrokerManager manager;

    public ClusterChangedListener(BrokerManager manager) {
        this.manager = manager;
    }

    @Override
    public void nodeOffline(String nodeId, String host, int port) {
        handleNodeOffline(nodeId, host, port);
    }

    private void handleNodeOffline(String nodeId, String host, int port) {
        BrokerConnectionManager connectionManager = manager.getBrokerConnectionManager();

        if (connectionManager.isEmpty()) {
            return;
        }

        for (Channel channel : connectionManager) {
            ByteBuf buf = null;
            try {
                buf = applyNodeOfflineData(channel, nodeId, host, port);
                AwareInvocation invocation = AwareInvocation.newInvocation(CLUSTER_CHANGED, (short) 0, buf, (byte) 0);
                channel.writeAndFlush(invocation);
            } catch (Throwable t) {
                ByteBufUtil.release(buf);
                throw new RuntimeException(String.format("Failed to handle server offline, nodeId=%s host=%s port=%d", nodeId, host, port));
            }
        }
    }

    private ByteBuf applyNodeOfflineData(Channel channel, String nodeId, String host, int port) throws IOException {
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
