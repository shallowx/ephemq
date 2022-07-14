package org.shallow.handle;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import org.shallow.RemoteException;
import org.shallow.codec.MessagePacket;
import org.shallow.invoke.GenericInvokeHolder;
import org.shallow.invoke.GenericInvokeRejoin;
import org.shallow.invoke.InvokeHolder;
import org.shallow.invoke.InvokeRejoin;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.AwareInvocation;
import org.shallow.processor.ProcessorAware;

import java.util.Set;

import static org.shallow.ObjectUtil.checkNotNull;
import static org.shallow.ObjectUtil.isNull;
import static org.shallow.RemoteException.of;
import static org.shallow.util.ByteUtil.bufToString;
import static org.shallow.util.NetworkUtil.newFailurePacket;
import static org.shallow.util.NetworkUtil.newSuccessPacket;

public class ProcessDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProcessDuplexHandler.class);

    private static final int FAILURE_BODY_LIMIT = 64;
    private final InvokeHolder<ByteBuf> holder = new GenericInvokeHolder<>();
    private ProcessorAware processor;

    public ProcessDuplexHandler(ProcessorAware processor) {
        this.processor = checkNotNull(processor, "[Constructor] - process must be not null");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        processor.onActive(ctx);
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof final MessagePacket packet) {
            try {
                final int command = packet.command();
                if (command > 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Read message packet - [{}] form remote address - [{}]", packet, ctx.channel().remoteAddress());
                    }
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
        final int command = packet.command();
        final int answer = packet.rejoin();
        final int length = packet.body().readableBytes();
        final InvokeRejoin<ByteBuf> rejoin = answer == 0 ? null : new GenericInvokeRejoin<>((buf, cause) -> {
            if (ctx.isRemoved() || !ctx.channel().isActive()) {
                return;
            }

            if (isNull(cause)) {
                ctx.writeAndFlush(newSuccessPacket(answer, buf == null ? null : buf.retain()));
            } else {
                ctx.writeAndFlush(newFailurePacket(answer, cause));
            }
        });

        final ByteBuf body = packet.body().retain();
        try {
            processor.process(ctx.channel(), answer, body, rejoin);
        } catch (Throwable cause) {
            if (logger.isErrorEnabled()) {
                logger.error("[processRequest] <{}> invoke processor error - command={}, rejoin={}, body length={}",
                        ctx.channel().remoteAddress(), command, rejoin, length, cause);
            }
        } finally {
            body.release();
        }
    }

    private void processResponse(ChannelHandlerContext ctx, MessagePacket packet) {
        final byte command = packet.command();
        final int rejoin = packet.rejoin();
        final int length = packet.body().readableBytes();

        if (command == 0 && logger.isDebugEnabled()) {
            logger.debug("[processResponse] - <{}> command is invalid: command={} rejoin={} length={}", ctx.channel().remoteAddress(), command, rejoin, length);
            return;
        }
        final ByteBuf body = packet.body().retain();
        final boolean consumed;
        try {
            if (command == 0) {
                consumed = holder.consume(rejoin, r -> r.success(body.retain()));
            } else {
                final String message = bufToString(body, FAILURE_BODY_LIMIT);
                final RemoteException cause = of(command, message);
                consumed = holder.consume(rejoin, r -> r.failure(cause));
            }
        } catch (Throwable cause) {
            if (logger.isWarnEnabled()) {
                logger.warn("[processResponse] - <{}> invoke not found: command={} rejoin={} length={}", ctx.channel().remoteAddress(), command, rejoin, length);
            }
            return;
        } finally {
            body.release();
        }

        if (!consumed) {
            if (logger.isWarnEnabled()) {
                logger.warn("[processResponse] - <{}> invoke not found: command={} rejoin={} length={}", ctx.channel().remoteAddress(), command, rejoin, length);
            }
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof final AwareInvocation invocation) {
            final int rejoin = holder.hold(invocation.expires(), invocation.rejoin());
            final MessagePacket packet;
            try {
                packet = MessagePacket.newPacket(rejoin, invocation.command(),  invocation.data().retain());
            } catch (Throwable cause) {
                holder.consume(rejoin, r -> r.failure(cause));
                throw cause;
            } finally {
                invocation.release();
            }

            final EventExecutor executor = ctx.executor();
            scheduleExpiredTask(executor);
            if (rejoin != 0 && !promise.isVoid()) {
                promise.addListener(f -> {
                   Throwable cause = f.cause();
                   if (isNull(cause)) {
                       return;
                   }
                   if (executor.inEventLoop()) {
                       holder.consume(rejoin, r -> r.failure(cause));
                   } else {
                       executor.execute(() -> holder.consume(rejoin, r -> r.failure(cause)));
                   }
                });
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    private void scheduleExpiredTask(EventExecutor executor) {

    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final int whole = holder.consumeWholeVerbExpired(c -> c.failure(of(RemoteException.Failure.INVOKE_TIMEOUT_EXCEPTION, "invoke timeout")), null);
        if (logger.isDebugEnabled()) {
            logger.debug("[handlerRemoved] - consume whole without expired, whole={}", whole);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (logger.isErrorEnabled()) {
            logger.error("[exceptionCaught] <{}> caught {}", ctx.channel().remoteAddress(), cause);
        }
        ctx.close();
    }

    private static final FastThreadLocal<Set<InvokeHolder<ByteBuf>>> WHOLE_INVOKE_HOLDER = new FastThreadLocal<>() {
        @Override
        protected Set<InvokeHolder<ByteBuf>> initialValue() throws Exception {
            return new ObjectArraySet<>();
        }
    };
}
