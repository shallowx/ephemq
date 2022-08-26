package org.shallow.consumer.pull;

import io.netty.buffer.ByteBuf;
import org.shallow.Message;
import org.shallow.internal.Listener;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;
import org.shallow.util.ByteBufUtil;

import java.util.ArrayList;
import java.util.List;

public class PullConsumerListener implements Listener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PullConsumerListener.class);
    private final MessagePullListener listener;

    public PullConsumerListener(PullConsumer consumer) {
        this.listener = consumer.getPullListener();
    }

    @Override
    public void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal) {

    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {

    }

    @Override
    public void onPullMessage(String topic, ByteBuf data) {
        String queue = null;
        int ledger = 0;

        int limit = data.readInt();
        List<Message> messages = new ArrayList<>(limit);

        for (int i = 0; i < limit; i++) {
            int length = data.readInt();

            int queueLength = data.readInt();
            queue = ByteBufUtil.buf2String(data.readBytes(queueLength), queueLength);

            int epoch = data.readInt();
            long position = data.readLong();

            byte[] body = ByteBufUtil.buf2Bytes(data.readBytes((length - 16 - queue.length())));
            messages.add(new Message(topic, queue, body, epoch, position, null));
        }

        PullResult result = new PullResult(ledger, topic, queue, messages);
        listener.onMessage(result);
    }
}
