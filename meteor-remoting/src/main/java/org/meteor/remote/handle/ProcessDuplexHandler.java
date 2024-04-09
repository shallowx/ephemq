package org.meteor.remote.handle;

import static org.meteor.common.util.ObjectUtil.checkNotNull;
import static org.meteor.remote.exception.RemotingException.of;
import static org.meteor.remote.util.ByteBufUtil.buf2String;
import static org.meteor.remote.util.ByteBufUtil.release;
import static org.meteor.remote.util.NetworkUtil.newFailurePacket;
import static org.meteor.remote.util.NetworkUtil.newSuccessPacket;
import static org.meteor.remote.util.NetworkUtil.switchAddress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.Immutable;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.exception.RemotingException;
import org.meteor.remote.exception.RemotingTimeoutException;
import org.meteor.remote.invoke.CallableSafeInitializer;
import org.meteor.remote.invoke.GenericCallableSafeInitializer;
import org.meteor.remote.invoke.GenericInvokedFeedback;
import org.meteor.remote.invoke.InvokedFeedback;
import org.meteor.remote.invoke.Processor;
import org.meteor.remote.invoke.WrappedInvocation;

@Immutable
public class ProcessDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProcessDuplexHandler.class);
    private static final int FAILURE_CONTENT_LIMIT = 4 * 1024 * 1024;
    private static final FastThreadLocal<Set<CallableSafeInitializer<ByteBuf>>> WHOLE_INVOKE_INITIALIZER = new FastThreadLocal<>() {
        @Override
        protected Set<CallableSafeInitializer<ByteBuf>> initialValue() throws Exception {
            return new HashSet<>();
        }
    };
    private final CallableSafeInitializer<ByteBuf> initializer = new GenericCallableSafeInitializer<>();
    private final Processor processor;

    public ProcessDuplexHandler(Processor processor) {
        this.processor = checkNotNull(processor, "Processor handler cannot be null");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        processor.onActive(channel, ctx.executor());
        ctx.fireChannelActive();
        if (logger.isDebugEnabled()) {
            logger.debug("Processor duplex handler channel is active, and local_address[{}] remote_address[{}]", channel.localAddress().toString(), channel.remoteAddress().toString());
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof final MessagePacket packet) {
            try {
                final int command = packet.command();
                if (command > 0) {
                    processRequest(ctx, packet);
                } else {
                    processResponse(ctx, packet);
                }
                // if you need to debug, can move this code to the top
                if (logger.isDebugEnabled()) {
                    logger.debug("Read message packet[{}] form remote address[{}]", packet, switchAddress(ctx.channel()));
                }
            } finally {
                packet.release();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void processRequest(ChannelHandlerContext ctx, MessagePacket packet) {
        int command = packet.command();
        long feedback = packet.feedback();
        int length = packet.body().readableBytes();
        InvokedFeedback<ByteBuf> rejoin = feedback == 0L ? null : new GenericInvokedFeedback<>((byteBuf, cause) -> {
            if (ctx.isRemoved() || !ctx.channel().isActive()) {
                return;
            }

            if (null == cause) {
                ctx.writeAndFlush(newSuccessPacket(feedback, byteBuf == null ? null : byteBuf.retain()));
            } else {
                ctx.writeAndFlush(newFailurePacket(feedback, cause));
            }
        });

        final ByteBuf buf = packet.body().retain();
        try {
            processor.process(ctx.channel(), command, buf, rejoin);
        } catch (Throwable cause) {
            if (null != rejoin) {
                rejoin.failure(cause);
            }
            if (logger.isErrorEnabled()) {
                logger.error("Channel[{}] invoke processor failed, command[{}]  rejoin[{}]  body_length[{}]",
                        ctx.channel().remoteAddress(), command, rejoin, length, cause);
            }
        } finally {
            release(buf);
        }
    }

    private void processResponse(ChannelHandlerContext ctx, MessagePacket packet) {
        var command = packet.command();
        var feedback = packet.feedback();
        if (feedback == 0) {
            if (logger.isErrorEnabled()) {
                logger.error("Chanel[{}] command[{}] is invalid, and feedback[{}] ", switchAddress(ctx.channel()), command, feedback);
            }
            return;
        }

        var buf = packet.body().retain();
        boolean freed;
        try {
            if (command == 0) {
                freed = initializer.release(feedback, r -> r.success(buf.retain()));
            } else {
                String message = buf2String(buf, FAILURE_CONTENT_LIMIT);
                RemotingException cause = of(command, message);
                freed = initializer.release(feedback, r -> r.failure(cause));
            }
        } catch (Throwable cause) {
            if (logger.isErrorEnabled()) {
                logger.error("Chanel[{}] invoke not found that command[{}] and feedback[{}] ", ctx.channel().remoteAddress(),
                        command, feedback);
            }
            return;
        } finally {
            release(buf);
        }

        if (!freed) {
            if (logger.isErrorEnabled()) {
                logger.error("Channel[{}] invoke not found that command[{}] and feedback[{}]", ctx.channel().remoteAddress(), command, feedback);
            }
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof final WrappedInvocation invocation) {
            long feedback = initializer.get(invocation.expired(), invocation.feedback());
            MessagePacket packet;
            try {
                packet = MessagePacket.newPacket(feedback, invocation.command(), invocation.data().retain());
            } catch (Throwable cause) {
                initializer.release(feedback, r -> r.failure(cause));
                throw cause;
            } finally {
                invocation.release();
            }

            EventExecutor executor = ctx.executor();
            invokeIdleCheckTimerTask(executor);

            if (feedback != 0L && !promise.isVoid()) {
                promise.addListener(f -> {
                    Throwable cause = f.cause();
                    if (null == cause) {
                        return;
                    }
                    if (executor.inEventLoop()) {
                        initializer.release(feedback, r -> r.failure(cause));
                    } else {
                        executor.execute(() -> initializer.release(feedback, r -> r.failure(cause)));
                    }
                });
            }
            ctx.write(packet, promise);
        } else {
            ctx.write(msg, promise);
        }
    }

    private void invokeIdleCheckTimerTask(EventExecutor executor) {
        if (initializer.isEmpty()) {
            return;
        }

        final Set<CallableSafeInitializer<ByteBuf>> initializers = WHOLE_INVOKE_INITIALIZER.get();
        if (!initializers.isEmpty()) {
            initializers.add(initializer);
            return;
        }

        initializers.add(initializer);
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                var processHolder = 0;
                var processInvoker = 0;
                var remnantHolder = 0;
                var remnantInvoker = 0;
                final Iterator<CallableSafeInitializer<ByteBuf>> iterator = initializers.iterator();
                while (iterator.hasNext()) {
                    var holder = iterator.next();
                    processHolder++;
                    processInvoker += holder.releaseExpired(
                            r -> r.failure(new RemotingTimeoutException("invoke handle timeout")));
                    if (holder.isEmpty()) {
                        iterator.remove();
                        continue;
                    }

                    remnantHolder++;
                    remnantInvoker += holder.size();
                }

                if (!initializers.isEmpty()) {
                    executor.schedule(this, 1, TimeUnit.SECONDS);
                }

                // if you need to debug, can move this code to the top
                if (logger.isDebugEnabled()) {
                    logger.debug("Handle expired schedule task: PH[{}] PI[{}] RH[{}] RI[{}]", processHolder, processInvoker, remnantHolder, remnantInvoker);
                }
            }
        }, 1, TimeUnit.SECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        int count = initializer.releaseAll(c -> c.failure(
                new RemotingTimeoutException(String.format("Channel[%s] invoke timeout", ctx.channel().toString()))));
        if (logger.isDebugEnabled()) {
            logger.debug("Release entire invoke, handle count[{}]", count);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (logger.isErrorEnabled()) {
            logger.error("Channel[{}] was caught", ctx.channel().toString(), cause.getMessage(), cause);
        }
        ctx.close();
    }
}
