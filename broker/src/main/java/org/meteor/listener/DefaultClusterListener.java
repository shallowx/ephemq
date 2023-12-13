package org.meteor.listener;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.meteor.configuration.NetworkConfiguration;
import org.meteor.coordinatio.ConnectionCoordinator;
import org.meteor.coordinatio.Coordinator;
import org.meteor.common.Node;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.processor.AwareInvocation;
import org.meteor.remote.processor.ProcessCommand;
import org.meteor.remote.proto.NodeMetadata;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.util.ByteBufUtils;
import org.meteor.remote.util.ProtoBufUtils;

import java.util.Set;

public class DefaultClusterListener implements ClusterListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultClusterListener.class);

    private final Coordinator manager;
    private final NetworkConfiguration config;

    public DefaultClusterListener(Coordinator manager, NetworkConfiguration config) {
        this.manager = manager;
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
        logger.info("Node {} join the cluster", node);
    }

    @Override
    public void onNodeDown(Node node) {
        logger.info("Node {} become down state", node);
    }

    @Override
    public void onNodeLeave(Node node) {
        logger.info("Node {} left the cluster", node);
        processServerOffline(node);
    }

    private void processServerOffline(Node node) {
        ConnectionCoordinator connectionManager = manager.getConnectionManager();
        Set<Channel> channels = connectionManager.getChannels();
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
            AwareInvocation awareInvocation = AwareInvocation.newInvocation(ProcessCommand.Client.SERVER_OFFLINE, buf, config.getNotifyClientTimeoutMs(), null);
            channel.writeAndFlush(awareInvocation);
        } catch (Exception e) {
            ByteBufUtils.release(buf);
            logger.error("Send server offline failed, channel={}", channel, e);
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
        int length = ProtoBufUtils.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);
        ProtoBufUtils.writeProto(buf, signal);
        return buf;
    }
}
