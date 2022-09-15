package org.shallow.handle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
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
import java.util.concurrent.TimeUnit;

import static org.shallow.codec.MessagePacket.HEADER_LENGTH;
import static org.shallow.codec.MessagePacket.MAGIC_NUMBER;
import static org.shallow.processor.ProcessCommand.Client.HANDLE_MESSAGE;
import static org.shallow.util.NetworkUtil.newEventExecutorGroup;

@ThreadSafe
public class DefaultEntryPullDispatcher implements PullDispatchProcessor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultEntryPullDispatcher.class);

    private final BrokerConfig config;
    private final Int2ObjectMap<Channel> channels = new Int2ObjectOpenHashMap<>();
    private final Int2ObjectMap<EntryPullHandler> executionHandlers = new Int2ObjectOpenHashMap<>();
    private final EventExecutor transferExecutor;
    private final EventExecutor chainExecutor;
    private final HandlerMapping chain;

    public DefaultEntryPullDispatcher(BrokerConfig config) {
        this.config = config;
        this.chain = new EntryPullHandlerExecutionMapping(config);
        this.transferExecutor = newEventExecutorGroup(config.getMessagePullTransferThreadLimit(), "transfer").next();
        this.chainExecutor = newEventExecutorGroup(config.getMessagePullChainThreadLimit(), "chain").next();
    }

    @Override
    public void register(int requestId, Channel channel) {
        if (chainExecutor.inEventLoop()) {
            doRegister(requestId, channel);
        } else {
            chainExecutor.execute(() -> doRegister(requestId, channel));
        }
    }

    private void doRegister(int requestId, Channel channel) {
        channels.computeIfAbsent(requestId, k -> channel);
        EntryPullHandler handler = chain.applyHandler();
        executionHandlers.computeIfAbsent(requestId, k -> handler);
    }

    public void dispatch(int requestId, String topic, String queue, short version, int ledgerId, int limit, Offset offset,  ByteBuf payload) {
        if (channels.isEmpty()) {
            return;
        }

        EntryPullHandler handler = executionHandlers.get(requestId);
        handler = chain.preHandle(handler);
        handler.executor().execute(() -> doDispatch(requestId, topic, queue, version, ledgerId, limit, offset, payload));
    }

    private void doDispatch(int requestId, String topic, String queue, short version, int ledgerId, int limit, Offset offset,  ByteBuf payload) {
        Channel channel = channels.get(requestId);
        try {
            if (null == channel) {
                return;
            }

            ByteBuf buf = null;
            try {
                buf = buildByteBuf(topic, queue, version, ledgerId, limit, offset, payload, channel.alloc());
                if (!channel.isActive()) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Channel<{}> is not active, and will remove it", channel.toString());
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
                    logger.error("Channel<{}> pull message failed, error:{}", channel.toString(), t);
                }
            } finally {
                cancel(requestId);
                ByteBufUtil.release(buf);
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Channel<{}> pull message failed, topic={} queue={} version={} error:{}", topic, queue, version, channel.toString(), t);
            }
        } finally {
            cancel(requestId);
            ByteBufUtil.release(payload);
        }
    }

    private void cancel(int id) {
        if (chainExecutor.inEventLoop()) {
            doCancel(id);
        } else {
            chainExecutor.execute(() -> doCancel(id));
        }
    }

    private void doCancel(int id) {
        channels.remove(id);
        executionHandlers.remove(id);

        EntryPullHandler handler = executionHandlers.get(id);
        chain.postHandle(handler);
    }

    private void transfer(int requestId, ByteBuf payload, Channel channel) {
        try {
            int retryDelayTimeMs = config.getPullRetryTaskDelayTimeMs();
            Runnable retryTask = () -> {
                try {
                    if (!channel.isActive()) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Channel<{}> is not active, and will remove it", channel.toString());
                        }
                        cancel(requestId);
                        return;
                    }

                    if (!channel.isWritable()) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Channel<{}> is not writable, and try again after {} ms", channel.toString(), retryDelayTimeMs);
                        }
                        transfer(requestId, payload, channel);
                        return;
                    }

                    channel.writeAndFlush(payload.retainedDuplicate());
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel<{}> pull message retry failed, and try again after {} ms,  error:{}", channel.toString(), retryDelayTimeMs, t);
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

    private ByteBuf buildByteBuf(String topic, String queue, short version, int ledgerId, int limit, Offset offset , ByteBuf payload, ByteBufAllocator alloc) {
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

            buf = alloc.ioBuffer(HEADER_LENGTH + signalLength);

            buf.writeByte(MAGIC_NUMBER);
            buf.writeMedium(HEADER_LENGTH + signalLength + payloadLength);
            buf.writeShort(version);
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

    @Override
    public void shutdownGracefully() {
        if (channels.isEmpty()) {
            return;
        }

        ObjectSet<Int2ObjectMap.Entry<Channel>> channelEntries = channels.int2ObjectEntrySet();
        for (Int2ObjectMap.Entry<Channel> channelEntry : channelEntries) {
            int requestId = channelEntry.getIntKey();

            EntryPullHandler entryPullHandler = executionHandlers.get(requestId);
            EventExecutor executor = entryPullHandler.executor();

            executor.submit(() -> {
                doCancel(requestId);
            });
        }

        if (!chainExecutor.isShutdown()) {
            chainExecutor.shutdownGracefully();
        }

        if (!transferExecutor.isShutdown()) {
            transferExecutor.shutdownGracefully();
        }

        chain.close();
    }
}
