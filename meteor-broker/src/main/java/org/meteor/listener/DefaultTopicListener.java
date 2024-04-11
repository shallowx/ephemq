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

public class DefaultTopicListener implements TopicListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultTopicListener.class);
    private final Manager coordinator;
    private final CommonConfig commonConfiguration;
    private final NetworkConfig networkConfiguration;

    public DefaultTopicListener(Manager coordinator, CommonConfig commonConfiguration,
                                NetworkConfig networkConfiguration) {
        this.coordinator = coordinator;
        this.commonConfiguration = commonConfiguration;
        this.networkConfiguration = networkConfiguration;

    }

    @Override
    public void onPartitionInit(TopicPartition topicPartition, int ledger) {
    }

    @Override
    public void onPartitionDestroy(TopicPartition topicPartition, int ledger) {
    }

    @Override
    public void onPartitionGetLeader(TopicPartition topicPartition) {
    }

    @Override
    public void onPartitionLostLeader(TopicPartition topicPartition) {
    }

    @Override
    public void onTopicCreated(String topic) {
        sendTopicChangedSignal(topic, TopicChangedSignal.Type.CREATE);
    }

    @Override
    public void onTopicDeleted(String topic) {
        sendTopicChangedSignal(topic, TopicChangedSignal.Type.DELETE);
    }

    @Override
    public void onPartitionChanged(TopicPartition topicPartition, TopicAssignment oldAssigment, TopicAssignment newAssigment) {
        try {
            PartitionInfo partitionInfo = coordinator.getTopicCoordinator().getPartitionInfo(topicPartition);
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

    private void sendTopicChangedSignal(String topic, TopicChangedSignal.Type type) {
        Set<Channel> channels = coordinator.getConnectionCoordinator().getReadyChannels();
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
                    logger.error("Send topic change failed, channel[{}]", channel, e);
                }
            }
        }
    }

    private void sendPartitionChangedSignal(TopicPartition topicPartition, TopicAssignment assignment) {
        Set<Channel> channels = coordinator.getConnectionCoordinator().getReadyChannels();
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
                    logger.error("Send partition change failed, channel[{}]", channel, e);
                }
            }
        }
    }

    private ByteBuf assembleTopicChangedSignal(Channel channel, String topic, TopicChangedSignal.Type type) throws IOException {
        TopicChangedSignal signal = TopicChangedSignal.newBuilder().setType(type).setTopic(topic).build();
        int length = ProtoBufUtil.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);
        ProtoBufUtil.writeProto(buf, signal);
        return buf;
    }

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
