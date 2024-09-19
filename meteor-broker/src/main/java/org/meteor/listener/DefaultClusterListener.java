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
import org.meteor.support.Connection;
import org.meteor.support.Manager;

/**
 * DefaultClusterListener implements the ClusterListener interface to handle
 * events related to nodes in a cluster. It provides specific actions to be taken
 * when nodes join, leave, or change state within the cluster.
 */
public class DefaultClusterListener implements ClusterListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultClusterListener.class);
    /**
     * The manager responsible for overseeing and coordinating the operations
     * related to the cluster. This includes handling tasks such as starting
     * and shutting down services, managing connections, handling logging,
     * and supporting various event executor groups and listeners.
     */
    private final Manager manager;
    /**
     * Network configuration settings used by the DefaultClusterListener.
     * Holds various properties related to networking that are referenced when managing
     * cluster events such as node joins, leaves, or changes in control roles.
     */
    private final NetworkConfig config;

    /**
     * Constructs a new DefaultClusterListener with the specified Manager and NetworkConfig.
     *
     * @param manager the Manager instance responsible for overseeing various operations.
     * @param config  the NetworkConfig instance that provides network-related configuration settings.
     */
    public DefaultClusterListener(Manager manager, NetworkConfig config) {
        this.manager = manager;
        this.config = config;
    }

    /**
     * Callback method that is triggered when a node gains the controller role within the cluster.
     *
     * @param node the node that has obtained the controller role.
     */
    @Override
    public void onGetControlRole(Node node) {
    }

    /**
     * This method is called when the current node loses its controller role within the cluster.
     *
     * @param node The node that lost the controller role.
     */
    @Override
    public void onLostControlRole(Node node) {
    }

    /**
     * Handles the event when a node joins the cluster. If the info logging level is enabled,
     * logs an informational message indicating that a node has joined.
     *
     * @param node the instance of the Node that has joined the cluster
     */
    @Override
    public void onNodeJoin(Node node) {
        if (logger.isInfoEnabled()) {
            logger.info("Node[{}] join the cluster", node);
        }
    }

    /**
     * This method is called when a node is detected to be down or non-operational.
     * It logs an informational message indicating that the specified node has
     * transitioned to a down state if info logging is enabled.
     *
     * @param node the node that is down or non-operational
     */
    @Override
    public void onNodeDown(Node node) {
        if (logger.isInfoEnabled()) {
            logger.info("Node[{}] becomes down state", node);
        }
    }

    /**
     * The method is called when a node leaves the cluster. It logs the event
     * at the info level if info logging is enabled and then processes the
     * server offline state for the given node.
     *
     * @param node the node that is leaving the cluster
     */
    @Override
    public void onNodeLeave(Node node) {
        if (logger.isInfoEnabled()) {
            logger.info("Node[{}] left the cluster", node);
        }
        processServerOffline(node);
    }

    /**
     * Processes a server node that has gone offline by retrieving the current connection and
     * sending server offline data to all ready channels.
     *
     * @param node the server node that has gone offline
     */
    private void processServerOffline(Node node) {
        Connection connection = manager.getConnection();
        Set<Channel> channels = connection.getReadyChannels();
        if (channels != null && !channels.isEmpty()) {
            for (Channel channel : channels) {
                sendServerOfflineData(channel, node);
            }
        }
    }

    /**
     * Sends server offline data to a specified channel for a given node.
     *
     * @param channel The communication channel to send the data through.
     * @param node    The node associated with the server offline data.
     */
    private void sendServerOfflineData(Channel channel, Node node) {
        ByteBuf buf = null;
        try {
            buf = assembleServerOfflineData(channel, node);
            WrappedInvocation awareInvocation = WrappedInvocation.newInvocation(Command.Client.SERVER_OFFLINE, buf, config.getNotifyClientTimeoutMilliseconds(), null);
            channel.writeAndFlush(awareInvocation);
        } catch (Exception e) {
            ByteBufUtil.release(buf);
            if (logger.isErrorEnabled()) {
                logger.error(STR."[:: channel:\{channel}]Send server offline failed,", e);
            }
        }
    }

    /**
     * Assembles a ByteBuf containing server offline data for a given node.
     *
     * @param channel the Netty channel that will be used to allocate the buffer
     * @param node the node which is going offline
     * @return ByteBuf containing the serialized NodeOfflineSignal data
     * @throws Exception if an error occurs during serialization
     */
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
