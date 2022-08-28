package org.shallow.handle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.shallow.Type;
import org.shallow.codec.MessagePacket;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.log.Offset;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.MessagePullSignal;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.ProtoBufUtil;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.processor.ProcessCommand.Client.HANDLE_MESSAGE;
import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.ObjectUtil.isNull;

@ThreadSafe
public class EntryPullHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryPullHandler.class);

    private final BrokerConfig config;
    private final Int2ObjectMap<Channel> channels = new Int2ObjectOpenHashMap<>();
    private final EventExecutor transferExecutor = newEventExecutorGroup(1, "transfer").next();
    private final EventExecutor allocateExecutor = newEventExecutorGroup(1, "allocate").next();
    private final Set<Handler> handlers = new CopyOnWriteArraySet<>();

    public EntryPullHandler(BrokerConfig config) {
        this.config = config;
    }

    public void register(int requestId, Channel channel) {
        channels.computeIfAbsent(requestId, k -> channel);
    }

    private void cancel(int requestId) {
        channels.remove(requestId);
    }

    public void handle(int requestId, String topic, String queue, int ledgerId, int limit, Offset offset,  ByteBuf payload) {
        if (channels.isEmpty()) {
            return;
        }

        allocateExecutor.execute(() -> {
            Handler handler = allocateHandler();
            handler.handleExecutor.execute(() -> doHandle(requestId, topic, queue, ledgerId, limit, offset, payload));
        });
    }

    private void doHandle(int requestId, String topic, String queue, int ledgerId, int limit, Offset offset,  ByteBuf payload) {
        try {
            Channel channel = channels.get(requestId);
            if (isNull(channel)) {
                return;
            }

            ByteBuf buf = null;
            try {
                buf = buildByteBuf(topic, queue, ledgerId, limit, offset, payload, channel.alloc());
                if (!channel.isActive()) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Channel is not active, and will remove it");
                    }
                    cancel(requestId);
                    return;
                }

                if (!channel.isWritable()) {
                    transfer(requestId, payload, channel);
                    return;
                }

                channel.writeAndFlush(buf.retainedDuplicate());
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Channel pull message failed, error:{}", t);
                }
            } finally {
                cancel(requestId);
                ByteBufUtil.release(buf);
            }
        } catch (Throwable t) {
            throw new RuntimeException("Channel pull message failed", t);
        } finally {
            ByteBufUtil.release(payload);
        }
    }

    private void transfer(int requestId, ByteBuf payload, Channel channel) {
        try {
            int retryDelayTimeMs = config.getPullRetryTaskDelayTimeMs();
            Runnable retryTask = () -> {
                try {
                    if (!channel.isActive()) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Channel is not active, and will remove it");
                        }
                        cancel(requestId);
                        return;
                    }

                    if (!channel.isWritable()) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Channel is not writable, and try again after {} ms", retryDelayTimeMs);
                        }
                        transfer(requestId, payload, channel);
                        return;
                    }

                    channel.writeAndFlush(payload.retainedDuplicate());
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel pull message retry failed, and try again after {} ms,  error:{}", retryDelayTimeMs, t);
                    }
                } finally {
                    ByteBufUtil.release(payload);
                }
            };

            transferExecutor.schedule(retryTask, retryDelayTimeMs, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            transfer(requestId, payload, channel);
        }
    }

    private ByteBuf buildByteBuf(String topic, String queue, int ledgerId, int limit, Offset offset , ByteBuf payload, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessagePullSignal signal = MessagePullSignal
                    .newBuilder()
                    .setTopic(topic)
                    .setQueue(queue)
                    .setLedger(ledgerId)
                    .setLimit(limit)
                    .setEpoch(offset.epoch())
                    .setIndex(offset.index())
                    .build();

            int signalLength = ProtoBufUtil.protoLength(signal);
            int payloadLength = payload.readableBytes();

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);

            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + payloadLength);
            buf.writeShort(0);
            buf.writeByte(HANDLE_MESSAGE);
            buf.writeByte(Type.PULL.sequence());
            buf.writeInt(0);

            ProtoBufUtil.writeProto(buf, signal);
            buf = Unpooled.wrappedUnmodifiableBuffer(buf, payload.retainedSlice());

            return buf;
        } catch (Throwable t) {
            ByteBufUtil.release(buf);
            throw new RuntimeException(String.format("Failed to build payload: topic=%s queue=%s ledger=%d limit=%d", topic, queue, ledgerId, ledgerId), t);
        }
    }

    private Handler allocateHandler() {
        if (handlers.isEmpty()) {
            return buildHandler();
        }

        Handler minHandler = handlers.stream()
                .sorted(Comparator.comparingInt(h -> h.count))
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .iterator()
                .next();

        if (minHandler.count >= minHandler.limit) {
            return buildHandler();
        }

        minHandler.count += 1;
        return minHandler;
    }

    private Handler buildHandler() {
        EventExecutor executor = newEventExecutorGroup(1, "handle").next();
        Handler handler = new Handler(executor, config.getPullHandleThreadLimit(), 1);
        handlers.add(handler);
        return handler;
    }

   @SuppressWarnings("all")
   private class Handler {
        final EventExecutor handleExecutor;
        final int limit;
        int count;

        public Handler(EventExecutor handleExecutor, int limit, int count) {
            this.handleExecutor = handleExecutor;
            this.limit = limit;
        }
    }

    public void shutdownGracefully() {
        channels.clear();
        transferExecutor.shutdownGracefully();
        allocateExecutor.shutdownGracefully();

        if (handlers.isEmpty()) {
            return;
        }

        handlers.forEach(handler -> {
            handler.handleExecutor.shutdownGracefully();
        });
    }
}
