package org.meteor.listener;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.PartitionInfo;
import org.meteor.common.message.TopicAssignment;
import org.meteor.common.message.TopicPartition;
import org.meteor.config.CommonConfig;
import org.meteor.config.NetworkConfig;
import org.meteor.remote.invoke.Command;
import org.meteor.remote.invoke.WrappedInvocation;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;
import org.meteor.support.Manager;

/**
 * DefaultTopicListener is a concrete implementation of the TopicListener interface.
 * It listens for various topic-related events such as topic creation, deletion,
 * partition changes, etc., and reacts to them by sending signals as needed.
 */
public class DefaultTopicListener implements TopicListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultTopicListener.class);
    /**
     * The manager responsible for controlling and managing various underlying components
     * required by the DefaultTopicListener implementation.
     */
    private final Manager manager;
    /**
     * Provides a unified configuration management for server settings used by the DefaultTopicListener.
     * This instance is immutable and holds various configuration parameters essential for topic and partition handling.
     */
    private final CommonConfig commonConfiguration;
    /**
     * Represents the network configuration settings used by the DefaultTopicListener.
     * <p>
     * This variable holds an instance of the NetworkConfig class, which manages various
     * network-related settings such as connection timeouts, buffer sizes, thread limits, and
     * debug log configurations. These settings are utilized by the DefaultTopicListener for
     * network operations and interactions.
     */
    private final NetworkConfig networkConfiguration;

    /**
     * Constructs a DefaultTopicListener with the specified manager, common configuration,
     * and network configuration.
     *
     * @param manager The manager responsible for coordinating various services
     *                and resources required by the listener.
     * @param commonConfiguration The common configuration settings shared across the system.
     * @param networkConfiguration The network-specific configuration settings.
     */
    public DefaultTopicListener(Manager manager, CommonConfig commonConfiguration,
                                NetworkConfig networkConfiguration) {
        this.manager = manager;
        this.commonConfiguration = commonConfiguration;
        this.networkConfiguration = networkConfiguration;

    }

    /**
     * Handles the initialization of a partition for a given topic.
     *
     * @param topicPartition The partition of the topic being initialized.
     * @param ledger The ledger identifier associated with the partition.
     */
    @Override
    public void onPartitionInit(TopicPartition topicPartition, int ledger) {
    }

    /**
     * Handles the destruction of a partition in a topic.
     *
     * @param topicPartition the partition within the topic being destroyed
     * @param ledger the ledger associated with the partition
     */
    @Override
    public void onPartitionDestroy(TopicPartition topicPartition, int ledger) {
    }

    /**
     * Handles the event when a leader is elected for a given partition of a topic.
     *
     * @param topicPartition the partition of the topic for which a leader has been elected
     */
    @Override
    public void onPartitionGetLeader(TopicPartition topicPartition) {
    }

    /**
     * Handles the event when a partition loses its leader.
     *
     * @param topicPartition The topic-partition pair which lost its leader.
     */
    @Override
    public void onPartitionLostLeader(TopicPartition topicPartition) {
    }

    /**
     * This method is called when a new topic is created.
     *
     * @param topic the name of the topic that has been created.
     */
    @Override
    public void onTopicCreated(String topic) {
        sendTopicChangedSignal(topic, TopicChangedSignal.Type.CREATE);
    }

    /**
     * Handles the deletion of a topic.
     * When a topic is deleted, this method triggers the appropriate signal
     * to notify other components about the deletion.
     *
     * @param topic the name of the deleted topic
     */
    @Override
    public void onTopicDeleted(String topic) {
        sendTopicChangedSignal(topic, TopicChangedSignal.Type.DELETE);
    }

    /**
     * Handles changes in partition assignments by checking if the current server
     * is in the replicas and if there are any differences between the old and new assignments.
     * If a relevant change is detected, it signals that the partition has changed.
     *
     * @param topicPartition The topic partition associated with the changes.
     * @param oldAssigment The old assignment details of the partition.
     * @param newAssigment The new assignment details of the partition.
     */
    @Override
    public void onPartitionChanged(TopicPartition topicPartition, TopicAssignment oldAssigment, TopicAssignment newAssigment) {
        try {
            PartitionInfo partitionInfo = manager.getTopicHandleSupport().getPartitionInfo(topicPartition);
            if (partitionInfo != null) {
                if (partitionInfo.getReplicas().contains(commonConfiguration.getServerId())
                        && ((!Objects.equals(oldAssigment.getReplicas(), newAssigment.getReplicas())))
                        || !Objects.equals(oldAssigment.getLeader(), newAssigment.getLeader())) {
                    sendPartitionChangedSignal(topicPartition, newAssigment);
                }
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Send partition change failed", e);
            }
        }
    }

    /**
     * Sends a topic changed signal to all ready channels.
     *
     * @param topic the topic that has changed
     * @param type the type of change for the topic, represented by {@link TopicChangedSignal.Type}
     */
    private void sendTopicChangedSignal(String topic, TopicChangedSignal.Type type) {
        Set<Channel> channels = manager.getConnection().getReadyChannels();
        if (channels == null || channels.isEmpty()) {
            return;
        }

        for (Channel channel : channels) {
            ByteBuf buf = null;
            try {
                buf = assembleTopicChangedSignal(channel, topic, type);
                WrappedInvocation awareInvocation = WrappedInvocation.newInvocation(Command.Client.TOPIC_CHANGED, buf, networkConfiguration.getNotifyClientTimeoutMilliseconds(), null);
                channel.writeAndFlush(awareInvocation);
            } catch (Exception e) {
                ByteBufUtil.release(buf);
                if (logger.isErrorEnabled()) {
                    logger.error(STR."[:: channel:\{channel}]Send topic change failed", channel, e);
                }
            }
        }
    }

    /**
     * Sends a signal indicating that a partition has changed.
     *
     * @param topicPartition The partition that has changed.
     * @param assignment     The new assignment of the partition.
     */
    private void sendPartitionChangedSignal(TopicPartition topicPartition, TopicAssignment assignment) {
        Set<Channel> channels = manager.getConnection().getReadyChannels();
        if (channels == null || channels.isEmpty()) {
            return;
        }
        for (Channel channel : channels) {
            ByteBuf buf = null;
            try {
                buf = assemblePartitionChangedSignal(channel, topicPartition.topic(), assignment);
                WrappedInvocation awareInvocation = WrappedInvocation.newInvocation(Command.Client.TOPIC_CHANGED, buf, networkConfiguration.getNotifyClientTimeoutMilliseconds(), null);
                channel.writeAndFlush(awareInvocation);
            } catch (Exception e) {
                ByteBufUtil.release(buf);
                if (logger.isErrorEnabled()) {
                    logger.error(STR."[:: channel:\{channel}]Send partition change failed,", channel, e);
                }
            }
        }
    }

    /**
     * Assembles a signal indicating that a topic has changed and returns it as a ByteBuf.
     *
     * @param channel the Netty Channel used for buffer allocation
     * @param topic the name of the topic that has changed
     * @param type the type of the topic change (e.g., CREATED, DELETED)
     * @return a ByteBuf containing the serialized TopicChangedSignal
     * @throws IOException if an error occurs during signal building or serialization
     */
    private ByteBuf assembleTopicChangedSignal(Channel channel, String topic, TopicChangedSignal.Type type) throws IOException {
        TopicChangedSignal signal = TopicChangedSignal.newBuilder().setType(type).setTopic(topic).build();
        int length = ProtoBufUtil.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);
        ProtoBufUtil.writeProto(buf, signal);
        return buf;
    }

    /**
     * Assembles a partition changed signal for the given topic and assignment.
     *
     * @param channel The Netty channel used to allocate the buffer.
     * @param topic The topic for which the partition change has occurred.
     * @param assignment The new topic assignment details.
     * @return A ByteBuf containing the serialized partition changed signal.
     * @throws Exception If an error occurs during the signal assembly.
     */
    private ByteBuf assemblePartitionChangedSignal(Channel channel, String topic, TopicAssignment assignment) throws Exception {
        TopicChangedSignal signal = TopicChangedSignal.newBuilder()
                .setType(TopicChangedSignal.Type.UPDATE)
                .setTopic(topic)
                .setLedger(assignment.getLedgerId())
                .setLedgerVersion(assignment.getVersion())
                .build();
        int length = ProtoBufUtil.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);
        ProtoBufUtil.writeProto(buf, signal);
        return buf;
    }
}
