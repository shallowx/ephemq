package org.ostara.remote.handle;

import static org.ostara.common.util.ObjectUtils.checkNotNull;
import static org.ostara.remote.RemoteException.of;
import static org.ostara.remote.util.ByteBufUtils.buf2String;
import static org.ostara.remote.util.ByteBufUtils.release;
import static org.ostara.remote.util.NetworkUtils.newFailurePacket;
import static org.ostara.remote.util.NetworkUtils.newSuccessPacket;
import static org.ostara.remote.util.NetworkUtils.switchAddress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.Immutable;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.RemoteException;
import org.ostara.remote.codec.MessagePacket;
import org.ostara.remote.invoke.GenericInvokeAnswer;
import org.ostara.remote.invoke.GenericInvokeHolder;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.invoke.InvokeHolder;
import org.ostara.remote.processor.AwareInvocation;
import org.ostara.remote.processor.ProcessorAware;

@Immutable
public class ProcessDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProcessDuplexHandler.class);

    private static final int FAILURE_CONTENT_LIMIT = 1024 * 1024 * 4;
    private static final int INT_ZERO = 0;
    private final InvokeHolder<ByteBuf> holder = new GenericInvokeHolder<>();
    private final ProcessorAware processor;

    public ProcessDuplexHandler(ProcessorAware processor) {
        this.processor = checkNotNull(processor, "Process cannot be null");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        processor.onActive(ctx.channel(), ctx.executor());
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof final MessagePacket packet) {
            try {
                logger.debug("Read message packet - [{}] form remote address - [{}]", packet,
                        switchAddress(ctx.channel()));

                final byte command = packet.command();
                if (command > INT_ZERO) {
                    processRequest(ctx, packet);
                } else {
                    processResponse(ctx, packet);
                }
            } finally {
                packet.release();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void processRequest(ChannelHandlerContext ctx, MessagePacket packet) {
        final byte command = packet.command();
        final short version = packet.version();
        final int answer = packet.answer();
        final byte type = packet.type();
        final int length = packet.body().readableBytes();
        final InvokeAnswer<ByteBuf> rejoin = answer == INT_ZERO ? null : new GenericInvokeAnswer<>((byteBuf, cause) -> {
            if (ctx.isRemoved() || !ctx.channel().isActive()) {
                return;
            }

            if (null == cause) {
                ctx.writeAndFlush(newSuccessPacket(answer, byteBuf == null ? null : byteBuf.retain()));
            } else {
                ChannelFuture future = ctx.writeAndFlush(newFailurePacket(answer, cause));
            }
        });

        final ByteBuf buf = packet.body().retain();
        try {
            processor.process(ctx.channel(), command, buf, rejoin, type, version);
        } catch (Throwable cause) {
            logger.error("Channel<{}> invoke processor error - command={}, rejoin={}, body length={}",
                    ctx.channel().remoteAddress(), command, rejoin, length, cause);
            if (null != rejoin) {
                rejoin.failure(cause);
            }
        } finally {
            release(buf);
        }
    }

    private void processResponse(ChannelHandlerContext ctx, MessagePacket packet) {
        final byte command = packet.command();
        final int answer = packet.answer();
        if (answer == INT_ZERO) {
            logger.error("Chanel<{}> command is invalid: command={} answer={} ", switchAddress(ctx.channel()),
                    command, answer);
            return;
        }

        final ByteBuf buf = packet.body().retain();
        final boolean freed;
        try {
            if (command == INT_ZERO) {
                freed = holder.free(answer, r -> r.success(buf.retain()));
            } else {
                final String message = buf2String(buf, FAILURE_CONTENT_LIMIT);
                final RemoteException cause = of(command, message);
                freed = holder.free(answer, r -> r.failure(cause));
            }
        } catch (Throwable cause) {
            logger.error("Chanel<{}> invoke not found: command={} answer={} ", ctx.channel().remoteAddress(),
                    command, answer);
            return;
        } finally {
            release(buf);
        }

        if (!freed) {
            logger.error("Channel<{}> invoke not found: command={} answer={}", ctx.channel().remoteAddress(),
                    command, answer);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof final AwareInvocation invocation) {
            final int answer = holder.hold(invocation.expired(), invocation.answer());
            final short version = invocation.version();
            final MessagePacket packet;
            try {
                packet = MessagePacket.newPacket(version, answer, invocation.command(), invocation.data().retain());
            } catch (Throwable cause) {
                holder.free(answer, r -> r.failure(cause));
                throw cause;
            } finally {
                invocation.release();
            }

            final EventExecutor executor = ctx.executor();
            scheduleExpiredTask(executor);

            if (answer != INT_ZERO && !promise.isVoid()) {
                promise.addListener(f -> {
                    Throwable cause = f.cause();
                    if (null == cause) {
                        return;
                    }
                    if (executor.inEventLoop()) {
                        holder.free(answer, r -> r.failure(cause));
                    } else {
                        executor.execute(() -> holder.free(answer, r -> r.failure(cause)));
                    }
                });
            }
            ctx.write(packet, promise);
        } else {
            ctx.write(msg, promise);
        }
    }

    private void scheduleExpiredTask(EventExecutor executor) {
        if (holder.isEmpty()) {
            return;
        }

        final Set<InvokeHolder<ByteBuf>> wholeHolders = WHOLE_INVOKE_HOLDER.get();
        if (!wholeHolders.isEmpty()) {
            wholeHolders.add(holder);
            return;
        }

        wholeHolders.add(holder);
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                var processHolder = INT_ZERO;
                var processInvoker = INT_ZERO;
                var remnantHolder = INT_ZERO;
                var remnantInvoker = INT_ZERO;
                final Iterator<InvokeHolder<ByteBuf>> iterator = wholeHolders.iterator();
                while (iterator.hasNext()) {
                    final var holder = iterator.next();
                    processHolder++;
                    processInvoker += holder.freeExpired(r -> r.failure(
                            of(RemoteException.Failure.INVOKE_TIMEOUT_EXCEPTION, "invoke handle timeout")));
                    if (holder.isEmpty()) {
                        iterator.remove();
                        continue;
                    }

                    remnantHolder++;
                    remnantInvoker += holder.size();
                }

                logger.debug("Handle expired schedule task: PH={} PI={} RH={} RI={}", processHolder, processInvoker,
                        remnantHolder, remnantInvoker);

                if (!wholeHolders.isEmpty()) {
                    executor.schedule(this, 1, TimeUnit.SECONDS);
                }
            }
        }, 1, TimeUnit.SECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final int whole = holder.freeEntire(c -> c.failure(of(RemoteException.Failure.INVOKE_TIMEOUT_EXCEPTION,
                String.format("Channel<%s> invoke timeout", ctx.channel().toString()))));
        logger.debug("Free entire invoke, whole={}", whole);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Channel<{}> caught {}", ctx.channel().toString(), cause);
        ctx.close();
    }

    private static final FastThreadLocal<Set<InvokeHolder<ByteBuf>>> WHOLE_INVOKE_HOLDER = new FastThreadLocal<>() {
        @Override
        protected Set<InvokeHolder<ByteBuf>> initialValue() throws Exception {
            return new HashSet<>();
        }
    };
}
