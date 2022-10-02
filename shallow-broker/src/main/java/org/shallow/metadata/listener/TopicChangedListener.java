package org.shallow.metadata.listener;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.shallow.internal.BrokerManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.network.BrokerConnectionManager;
import org.shallow.processor.AwareInvocation;
import org.shallow.proto.notify.PartitionChangedSignal;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.ProtoBufUtil;

import java.io.IOException;

import static org.shallow.processor.ProcessCommand.Client.CLUSTER_CHANGED;

public class TopicChangedListener implements TopicListener{

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterChangedListener.class);

    private final BrokerManager manager;

    public TopicChangedListener(BrokerManager manager) {
        this.manager = manager;
    }

    @Override
    public void update(String topic) {
        handleTopicOnChanged(topic);
    }

    private void handleTopicOnChanged(String topic) {
        BrokerConnectionManager connectionManager = manager.getBrokerConnectionManager();

        if (connectionManager.isEmpty()) {
            return;
        }

        for (Channel channel : connectionManager) {
            ByteBuf buf = null;
            try {
                buf = applyTopicUpdatedData(channel, topic);
                AwareInvocation invocation = AwareInvocation.newInvocation(CLUSTER_CHANGED, (short) 0, buf, (byte) 0);
                channel.writeAndFlush(invocation);
            } catch (Throwable t) {
                ByteBufUtil.release(buf);
                throw new RuntimeException(String.format("Failed to handle topic is updated, topic=%s", topic));
            }
        }
    }

    private ByteBuf applyTopicUpdatedData(Channel channel, String topic) throws IOException {
        PartitionChangedSignal signal = PartitionChangedSignal
                .newBuilder()
                .setTopic(topic)
                .build();

        int length = ProtoBufUtil.protoLength(signal);
        ByteBuf buf = channel.alloc().ioBuffer(length);

        ProtoBufUtil.writeProto(buf, signal);

        return buf;
    }
}
