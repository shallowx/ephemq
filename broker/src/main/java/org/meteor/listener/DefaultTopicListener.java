package org.meteor.listener;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.meteor.common.PartitionInfo;
import org.meteor.common.TopicAssignment;
import org.meteor.common.TopicPartition;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.configuration.CommonConfig;
import org.meteor.configuration.NetworkConfig;
import org.meteor.coordinatior.Coordinator;
import org.meteor.remote.processor.AwareInvocation;
import org.meteor.remote.processor.ProcessCommand;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.util.ByteBufUtils;
import org.meteor.remote.util.ProtoBufUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class DefaultTopicListener implements TopicListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultTopicListener.class);

    private final Coordinator coordinator;
    private final CommonConfig commonConfiguration;
    private final NetworkConfig networkConfiguration;

    public DefaultTopicListener(Coordinator coordinator, CommonConfig commonConfiguration, NetworkConfig networkConfiguration) {
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
            logger.error("Send partition change failed", e);
        }
    }

    private void sendTopicChangedSignal(String topic, TopicChangedSignal.Type type) {
        Set<Channel> channels = coordinator.getConnectionCoordinator().getChannels();
        if (channels.isEmpty()) {
            return;
        }

        for (Channel channel : channels) {
            ByteBuf buf = null;
            try {
                buf = assembleTopicChangedSignal(channel, topic, type);
                AwareInvocation awareInvocation = AwareInvocation.newInvocation(ProcessCommand.Client.TOPIC_INFO_CHANGED, buf, networkConfiguration.getNotifyClientTimeoutMs(), null);
                channel.writeAndFlush(awareInvocation);
            } catch (Exception e) {
                ByteBufUtils.release(buf);
                logger.error("Send topic change failed, channel={}", channel, e);
            }
        }
    }

    private void sendPartitionChangedSignal(TopicPartition topicPartition, TopicAssignment assignment) {
        Set<Channel> channels = coordinator.getConnectionCoordinator().getChannels();
        if (channels.isEmpty()) {
            return;
        }
        for (Channel channel : channels) {
            ByteBuf buf = null;
            try {
                buf = assemblePartitionChangedSignal(channel, topicPartition.getTopic(), assignment);
                AwareInvocation awareInvocation = AwareInvocation.newInvocation(ProcessCommand.Client.TOPIC_INFO_CHANGED, buf, networkConfiguration.getNotifyClientTimeoutMs(), null);
                channel.writeAndFlush(awareInvocation);
            } catch (Exception e) {
                ByteBufUtils.release(buf);
                logger.error("Send partition change failed, channel={}", channel, e);
            }
        }
    }

    private ByteBuf assembleTopicChangedSignal(Channel channel, String topic, TopicChangedSignal.Type type) throws IOException {
        TopicChangedSignal signal = TopicChangedSignal.newBuilder().setType(type).setTopic(topic).build();
        int length = ProtoBufUtils.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);
        ProtoBufUtils.writeProto(buf, signal);
        return buf;
    }

    private ByteBuf assemblePartitionChangedSignal(Channel channel, String topic, TopicAssignment assignment) throws Exception {
        TopicChangedSignal signal = TopicChangedSignal.newBuilder()
                .setType(TopicChangedSignal.Type.UPDATE)
                .setTopic(topic)
                .setLedger(assignment.getLedgerId())
                .setLedgerVersion(assignment.getVersion())
                .build();
        int length = ProtoBufUtils.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);
        ProtoBufUtils.writeProto(buf, signal);
        return buf;
    }
}
