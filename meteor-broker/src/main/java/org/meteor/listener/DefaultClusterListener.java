package org.meteor.listener;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.util.Set;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.config.NetworkConfig;
import org.meteor.remote.invoke.Command;
import org.meteor.remote.invoke.WrappedInvocation;
import org.meteor.remote.proto.NodeMetadata;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;
import org.meteor.support.ConnectionCoordinator;
import org.meteor.support.Coordinator;

public class DefaultClusterListener implements ClusterListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultClusterListener.class);
    private final Coordinator coordinator;
    private final NetworkConfig config;

    public DefaultClusterListener(Coordinator coordinator, NetworkConfig config) {
        this.coordinator = coordinator;
        this.config = config;
    }

    @Override
    public void onGetControlRole(Node node) {
    }

    @Override
    public void onLostControlRole(Node node) {
    }

    @Override
    public void onNodeJoin(Node node) {
        if (logger.isInfoEnabled()) {
            logger.info("Node[{}] join the cluster", node);
        }
    }

    @Override
    public void onNodeDown(Node node) {
        if (logger.isInfoEnabled()) {
            logger.info("Node[{}] become down state", node);
        }
    }

    @Override
    public void onNodeLeave(Node node) {
        if (logger.isInfoEnabled()) {
            logger.info("Node[{}] left the cluster", node);
        }
        processServerOffline(node);
    }

    private void processServerOffline(Node node) {
        ConnectionCoordinator connectionCoordinator = coordinator.getConnectionCoordinator();
        Set<Channel> channels = connectionCoordinator.getReadyChannels();
        if (channels != null && !channels.isEmpty()) {
            for (Channel channel : channels) {
                sendServerOfflineData(channel, node);
            }
        }
    }

    private void sendServerOfflineData(Channel channel, Node node) {
        ByteBuf buf = null;
        try {
            buf = assembleServerOfflineData(channel, node);
            WrappedInvocation awareInvocation = WrappedInvocation.newInvocation(Command.Client.SERVER_OFFLINE, buf, config.getNotifyClientTimeoutMilliseconds(), null);
            channel.writeAndFlush(awareInvocation);
        } catch (Exception e) {
            ByteBufUtil.release(buf);
            if (logger.isErrorEnabled()) {
                logger.error("Send server offline failed, channel[{}]", channel, e);
            }
        }
    }

    private ByteBuf assembleServerOfflineData(Channel channel, Node node) throws Exception {
        NodeMetadata metadata = NodeMetadata.newBuilder()
                .setClusterName(node.getCluster())
                .setId(node.getId())
                .setHost(node.getHost())
                .setPort(node.getPort())
                .build();

        NodeOfflineSignal signal = NodeOfflineSignal.newBuilder().setNode(metadata).build();
        int length = ProtoBufUtil.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);
        ProtoBufUtil.writeProto(buf, signal);
        return buf;
    }
}
