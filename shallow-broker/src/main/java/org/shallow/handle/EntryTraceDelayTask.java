package org.shallow.handle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import org.shallow.Type;
import org.shallow.codec.MessagePacket;
import org.shallow.log.Cursor;
import org.shallow.log.Offset;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.MessagePushSignal;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.ProtoBufUtil;
import javax.annotation.concurrent.ThreadSafe;
import static org.shallow.processor.ProcessCommand.Client.HANDLE_MESSAGE;

@ThreadSafe
public class EntryTraceDelayTask {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryTraceDelayTask.class);

    private final EntryTrace entryTrace;
    private final EntryDispatchHelper helper;
    private final Channel channel;
    private final int ledgerId;
    private final int traceLimit;
    private final int alignLimit;

    public EntryTraceDelayTask(EntryTrace trace, EntryDispatchHelper helper, int ledgerId) {
        this.entryTrace = trace;
        this.helper = helper;
        this.traceLimit = entryTrace.getTraceLimit();
        this.alignLimit = entryTrace.getAlignLimit();
        this.ledgerId = ledgerId;
        this.channel = channel();

    }

    public ChannelPromise newPromise() {
        ChannelPromise promise = channel.newPromise();
        promise.addListener((ChannelFutureListener) f -> {
            trace();
        });
        return promise;
    }

    public void trace() {
        try {
            EventExecutor executor = helper.channelExecutor(channel);
            executor.execute(this::doTrace);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Submit entry trace failure, task={}. error:{}", entryTrace, t);
            }
        }
    }

    private Channel channel() {
        EntrySubscription subscription = entryTrace.getSubscription();
        return subscription.getChannel();
    }


    private void doTrace() {
        EntrySubscription subscription = entryTrace.getSubscription();
        EntryPushHandler handler = subscription.getHandler();

        if (subscription != handler.getChannelShips().get(channel)) {
            return;
        }

        Cursor cursor = entryTrace.getCursor();
        Offset nextOffset = entryTrace.getOffset();

        int whole = 0;
        try {
            ByteBuf payload;
            while ((payload = cursor.next()) != null) {
                ByteBuf message = null;

                ByteBuf queueBuf = null;
                String queue = null;

                ByteBuf topicBuf = null;
                String topic = null;

                whole++;
                try {
                    short version = payload.readShort();

                    int topicLength = payload.readInt();
                    topicBuf = payload.retainedSlice(payload.readerIndex(), topicLength);
                    topic = ByteBufUtil.buf2String(topicBuf, topicLength);

                    payload.skipBytes(topicLength);

                    int queueLength = payload.readInt();
                    queueBuf = payload.retainedSlice(payload.readerIndex(), queueLength);
                    queue = ByteBufUtil.buf2String(queueBuf, queueLength);

                    payload.skipBytes(queueLength);

                    int epoch = payload.readInt();
                    long index = payload.readLong();

                    Offset messageOffset = buildOffset(epoch, index);

                    if (!messageOffset.after(nextOffset)) {
                        if (whole > traceLimit) {
                            break;
                        }
                        continue;
                    }

                    nextOffset = messageOffset;

                    short subscriptionVersion = subscription.getVersion();
                    if (!subscription.getTopic().equals(topic)) {
                        continue;
                    }

                    if (!subscription.getQueue().contains(queue)) {

                        continue;
                    }

                    if (subscriptionVersion != -1 && subscriptionVersion != version) {
                        continue;
                    }

                    subscription.setOffset(messageOffset);

                    message = buildByteBuf(topic, queue, version, new Offset(epoch, index), payload, channel.alloc());

                    if (!channel.isActive()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Entry trace subscribe channel<{}> is not active of task={}", channel, entryTrace);
                        }
                        return;
                    }

                    if (!channel.isWritable()) {
                        entryTrace.setOffset(nextOffset);
                        channel.writeAndFlush(message.retainedSlice(), newPromise());
                    }
                    channel.writeAndFlush(message, channel.voidPromise());

                    if (whole > traceLimit) {
                        break;
                    }

                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel trace message failed, topic={} queue={} offset={} error:{}", topic, queue, nextOffset, t);
                    }
                } finally {
                    ByteBufUtil.release(message);
                    ByteBufUtil.release(payload);
                    ByteBufUtil.release(queueBuf);
                    ByteBufUtil.release(topicBuf);
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }

        entryTrace.setOffset(nextOffset);
        Offset alignOffset = handler.getNextOffset();
        if (alignOffset != null && !nextOffset.before(alignOffset)) {
            align();
          return;
        }

        trace();
    }

    private void align() {
        try {
            EntrySubscription subscription = entryTrace.getSubscription();
            EntryPushHandler handler = subscription.getHandler();
            EventExecutor dispatchExecutor = handler.getDispatchExecutor();
            dispatchExecutor.execute(this::doAlign);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    private void doAlign() {
        EntrySubscription subscription = entryTrace.getSubscription();
        EntryPushHandler handler = subscription.getHandler();

        if (subscription != handler.getChannelShips().get(channel)) {
            return;
        }

        Offset alignOffset = handler.getNextOffset();
        Cursor cursor = entryTrace.getCursor();
        Offset nextOffset = entryTrace.getOffset();

        int whole = 0;
        try {
            ByteBuf payload;
            while ((payload = cursor.next()) != null) {
                ByteBuf message = null;

                ByteBuf queueBuf = null;
                String queue = null;

                ByteBuf topicBuf = null;
                String topic = null;

                whole++;
                try {
                    short version = payload.readShort();

                    int topicLength = payload.readInt();
                    topicBuf = payload.retainedSlice(payload.readerIndex(), topicLength);
                    topic = ByteBufUtil.buf2String(topicBuf, topicLength);

                    payload.skipBytes(topicLength);

                    int queueLength = payload.readInt();
                    queueBuf = payload.retainedSlice(payload.readerIndex(), queueLength);
                    queue = ByteBufUtil.buf2String(queueBuf, queueLength);

                    payload.skipBytes(queueLength);

                    int epoch = payload.readInt();
                    long index = payload.readLong();

                    Offset messageOffset = buildOffset(epoch, index);

                    if (!messageOffset.after(nextOffset)) {
                        if (whole > alignLimit) {
                            break;
                        }
                        continue;
                    }

                    if (messageOffset.after(alignOffset)) {
                        return;
                    }

                    nextOffset = messageOffset;

                    short subscriptionVersion = subscription.getVersion();
                    if (subscriptionVersion != -1 && subscriptionVersion != version) {
                        continue;
                    }

                    if (!subscription.getTopic().equals(topic)) {
                        continue;
                    }

                    if (!subscription.getQueue().contains(queue)) {
                        continue;
                    }

                    subscription.setOffset(messageOffset);

                    message = buildByteBuf(topic, queue, version, new Offset(epoch, index), payload, channel.alloc());
                    if (!channel.isActive()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Entry trace subscribe channel<{}> is not active of task={}", channel, entryTrace);
                        }
                        return;
                    }

                    if (!channel.isWritable()) {
                        entryTrace.setOffset(nextOffset);
                        channel.writeAndFlush(message.retainedSlice(), newPromise());
                    }
                    channel.writeAndFlush(message, channel.voidPromise());

                    if (whole > alignLimit) {
                        break;
                    }

                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel trace message failed, topic={} queue={} offset={} error:{}", topic, queue, nextOffset, t);
                    }
                } finally {
                    ByteBufUtil.release(message);
                    ByteBufUtil.release(payload);
                    ByteBufUtil.release(queueBuf);
                    ByteBufUtil.release(topicBuf);
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }

        entryTrace.setOffset(nextOffset);
        trace();
    }

    private ByteBuf buildByteBuf(String topic, String queue, short version, Offset offset , ByteBuf payload, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessagePushSignal signal = MessagePushSignal
                    .newBuilder()
                    .setQueue(queue)
                    .setTopic(topic)
                    .setLedgerId(ledgerId)
                    .setEpoch(offset.epoch())
                    .setIndex(offset.index())
                    .build();

            int signalLength = ProtoBufUtil.protoLength(signal);
            int payloadLength = payload.readableBytes();

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);

            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + payloadLength);
            buf.writeShort(version);
            buf.writeByte(HANDLE_MESSAGE);
            buf.writeByte(Type.PUSH.sequence());
            buf.writeInt(0);

            ProtoBufUtil.writeProto(buf, signal);

            buf = Unpooled.wrappedUnmodifiableBuffer(buf, payload.retainedSlice(payload.readerIndex(), payloadLength));

            return buf;
        } catch (Throwable t) {
            ByteBufUtil.release(buf);
            throw new RuntimeException(String.format("Failed to build payload: queue=%s", queue), t);
        }
    }

    private Offset buildOffset(int epoch, long index) {
        return new Offset(epoch, index);
    }
}
