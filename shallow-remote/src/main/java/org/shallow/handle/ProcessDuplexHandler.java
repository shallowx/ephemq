package org.shallow.handle;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import org.shallow.ObjectUtil;
import org.shallow.RemoteException;
import org.shallow.codec.MessagePacket;
import org.shallow.invoke.GenericInvokeAnswer;
import org.shallow.invoke.GenericInvokeHolder;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.invoke.InvokeHolder;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessorAware;
import org.shallow.util.ByteUtil;
import org.shallow.util.NetworkUtil;
import org.shallow.processor.AwareInvocation;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.shallow.RemoteException.of;

public class ProcessDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProcessDuplexHandler.class);

    private static final int FAILURE_CONTENT_LIMIT = 64;
    private static final int INT_ZERO = 0;
    private final InvokeHolder<ByteBuf> holder = new GenericInvokeHolder<>();
    private ProcessorAware processor;

    public ProcessDuplexHandler(ProcessorAware processor) {
        this.processor = ObjectUtil.checkNotNull(processor, "[Constructor] - Process must be not null");
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
                if (logger.isInfoEnabled()) {
                    logger.info("Read message packet - [{}] form remote address - [{}]", packet, NetworkUtil.switchAddress(ctx.channel()));
                }

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
        final int answer = packet.answer();
        final int length = packet.body().readableBytes();
        final InvokeAnswer<ByteBuf> rejoin = answer == INT_ZERO ? null : new GenericInvokeAnswer<>((byteBuf, cause) -> {
            if (ctx.isRemoved() || !ctx.channel().isActive()) {
                return;
            }

            if (ObjectUtil.isNull(cause)) {
                ctx.writeAndFlush(NetworkUtil.newSuccessPacket(answer, byteBuf == null ? null : byteBuf.retain()));
            } else {
                ctx.writeAndFlush(NetworkUtil.newFailurePacket(answer, cause));
            }
        });

        final ByteBuf buf = packet.body().retain();
        try {
            processor.process(ctx.channel(), command, buf, rejoin);
        } catch (Throwable cause) {
            if (logger.isInfoEnabled()) {
                logger.info("[ProcessRequest] <{}> invoke processor error - command={}, rejoin={}, body length={}",
                        ctx.channel().remoteAddress(), command, rejoin, length, cause);
            }
            if (ObjectUtil.isNotNull(rejoin)) {
                rejoin.failure(cause);
            }
        } finally {
            buf.release();
        }
    }

    private void processResponse(ChannelHandlerContext ctx, MessagePacket packet) {
        final byte command = packet.command();
        final int answer = packet.answer();
        if (answer == INT_ZERO) {
            if (logger.isInfoEnabled()) {
                logger.info("[ProcessResponse] - <{}> command is invalid: command={} answer={} ", NetworkUtil.switchAddress(ctx.channel()), command, answer);
            }
            return;
        }

        final ByteBuf buf = packet.body().retain();
        final boolean consumed;
        try {
            if (command == INT_ZERO) {
                consumed = holder.consume(answer, r -> r.success(buf.retain()));
            } else {
                final String message = ByteUtil.buf2String(buf, FAILURE_CONTENT_LIMIT);
                final RemoteException cause = of(command, message);
                consumed = holder.consume(answer, r -> r.failure(cause));
            }
        } catch (Throwable cause) {
            if (logger.isWarnEnabled()) {
                logger.warn("[ProcessResponse] - <{}> invoke not found: command={} answer={} ", ctx.channel().remoteAddress(), command, answer);
            }
            return;
        } finally {
            buf.release();
        }

        if (!consumed) {
            if (logger.isWarnEnabled()) {
                logger.warn("[ProcessResponse] - <{}> invoke not found: command={} answer={}", ctx.channel().remoteAddress(), command, answer);
            }
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof final AwareInvocation invocation) {
            final int answer = holder.hold(invocation.expired(), invocation.answer());
            final MessagePacket packet;
            try {
                packet = MessagePacket.newPacket(answer, invocation.command(),  invocation.data().retain());
            } catch (Throwable cause) {
                holder.consume(answer, r -> r.failure(cause));
                throw cause;
            } finally {
                invocation.release();
            }

            final EventExecutor executor = ctx.executor();
            scheduleExpiredTask(executor);
            if (answer != INT_ZERO && !promise.isVoid()) {
                promise.addListener(f -> {
                   Throwable cause = f.cause();
                   if (ObjectUtil.isNull(cause)) {
                       return;
                   }
                   if (executor.inEventLoop()) {
                       holder.consume(answer, r -> r.failure(cause));
                   } else {
                       executor.execute(() -> holder.consume(answer, r -> r.failure(cause)));
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
                    processInvoker += holder.consumeExpired(r -> r.failure(of(RemoteException.Failure.INVOKE_TIMEOUT_EXCEPTION, "invoke hold timeout")));
                    if (holder.isEmpty()) {
                        iterator.remove();
                        continue;
                    }

                    remnantHolder++;
                    remnantInvoker += holder.size();
                }

                if (logger.isInfoEnabled()) {
                    logger.info("[ScheduleExpiredTask] - execute expired task: PH={} PI={} RH={} RI={}", processHolder, processInvoker, remnantHolder, remnantInvoker);
                }

                if (!wholeHolders.isEmpty()) {
                    executor.schedule(this, 1, TimeUnit.SECONDS);
                }
            }
        }, 1, TimeUnit.SECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final int whole = holder.consumeWhole(c -> c.failure(of(RemoteException.Failure.INVOKE_TIMEOUT_EXCEPTION, "invoke timeout")));
        if (logger.isInfoEnabled()) {
            logger.info("[HandlerRemoved] - consume whole invoke, whole={}", whole);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (logger.isInfoEnabled()) {
            logger.info("[ExceptionCaught] <{}> caught {}", NetworkUtil.switchAddress(ctx.channel()), cause);
        }
        ctx.close();
    }

    private static final FastThreadLocal<Set<InvokeHolder<ByteBuf>>> WHOLE_INVOKE_HOLDER = new FastThreadLocal<>() {
        @Override
        protected Set<InvokeHolder<ByteBuf>> initialValue() throws Exception {
            return new HashSet<>();
        }
    };
}
