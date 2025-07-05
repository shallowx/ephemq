package org.meteor.remote.handle;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.exception.RemotingException;
import org.meteor.remote.exception.RemotingTimeoutException;
import org.meteor.remote.invoke.*;

import javax.annotation.concurrent.Immutable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.meteor.common.util.ObjectUtil.checkNotNull;
import static org.meteor.remote.exception.RemotingException.of;
import static org.meteor.remote.util.ByteBufUtil.buf2String;
import static org.meteor.remote.util.ByteBufUtil.release;
import static org.meteor.remote.util.NetworkUtil.*;

/**
 * ProcessDuplexHandler is an extension of the ChannelDuplexHandler that provides processing logic
 * for both inbound and outbound messages within a Netty channel pipeline. This handler integrates
 * with a Processor to manage custom business logic for active channels, message reading, writing,
 * and exception handling.
 * <p>
 * This class ensures thread-safe operations and efficient resource management using
 * CallableSafeInitializer for handling ByteBuf instances and managing feedback commands.
 * Logging is performed at various steps to aid in debugging and monitoring.
 * <p>
 * Key functionalities include:
 * - Activation of channels and invoking relevant processor callbacks.
 * - Reading incoming message packets and delegating the processing to the processor.
 * - Writing outbound messages and managing feedback through initializers.
 * - Handling exceptions and logging relevant errors.
 * - Managing the lifecycle of invoked tasks to prevent memory leaks and ensure timely execution.
 * - Releasing resources on handler removal or channel closure.
 */
@Immutable
public class ProcessDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProcessDuplexHandler.class);
    /**
     * The maximum size (in bytes) for content that is allowed to be processed
     * in the event of a failure. If the content exceeds this limit, it will not
     * be processed to prevent excessive resource usage.
     */
    private static final int FAILURE_CONTENT_LIMIT = 4 * 1024 * 1024;
    /**
     * A thread-local variable that holds a set of {@code CallableSafeInitializer<ByteBuf>} instances.
     * The set is initialized with a new {@code ObjectOpenHashSet<>}.
     * <p>
     * This variable ensures that each thread has its own instance of the set, which can store
     * initializers related to ByteBuf invocation tasks. It leverages the FastThreadLocal
     * from the Netty library for performance optimization.
     */
    private static final FastThreadLocal<Set<CallableSafeInitializer<ByteBuf>>> WHOLE_INVOKE_INITIALIZER = new FastThreadLocal<>() {
        @Override
        protected Set<CallableSafeInitializer<ByteBuf>> initialValue() throws Exception {
            return new ObjectOpenHashSet<>();
        }
    };
    /**
     * This variable initializes a callable-safe initializer for managing and reusing ByteBuf instances.
     * The initializer is of type `GenericCallableSafeInitializer<>`, which implements the `CallableSafeInitializer<ByteBuf>`
     * interface, providing methods for safe concurrent access and resource management.
     */
    private final CallableSafeInitializer<ByteBuf> initializer = new GenericCallableSafeInitializer<>();
    /**
     * Handles processing of data within a channel context.
     * Utilizes the provided {@link Processor} implementation to process inbound and outbound data.
     */
    private final Processor processor;

    /**
     * Constructs a ProcessDuplexHandler with the specified processor.
     *
     * @param processor the processor handling the processing of various channel events and commands
     *                  must not be null
     * @throws NullPointerException if the processor is null
     */
    public ProcessDuplexHandler(Processor processor) {
        this.processor = checkNotNull(processor, "Processor handler cannot be null");
    }

    /**
     * Handles the activation of the channel.
     * When the channel becomes active, it triggers the processor's onActive method,
     * fires the channel active event, and logs a debug message if debug level logging is enabled.
     *
     * @param ctx the ChannelHandlerContext which provides various operations for
     * interacting with the Channel and its pipeline
     * @throws Exception if an error occurs during the channel activation process
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        processor.onActive(channel, ctx.executor());
        ctx.fireChannelActive();
        if (logger.isDebugEnabled()) {
            logger.debug("Processor duplex handler channel is active, and local_address[{}] remote_address[{}]", channel.localAddress().toString(), channel.remoteAddress().toString());
        }
    }

    /**
     * Handles the reading of messages from the channel.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ChannelInboundHandler} belongs to
     * @param msg the message to read, typically a {@link MessagePacket}
     * @throws Exception if an error occurs while reading the message
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof final MessagePacket packet) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Read message packet[{}] form remote address[{}]", packet,
                            switchAddress(ctx.channel()));
                }

                final int command = packet.command();
                if (command > 0) {
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

    /**
     * Processes an incoming request message.
     *
     * @param ctx     the {@link ChannelHandlerContext} to which this {@code processRequest} belongs
     * @param packet  the {@link MessagePacket} containing the request message
     */
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

    /**
     * Processes the response message contained in the packet and releases resources as necessary.
     *
     * @param ctx the {@link ChannelHandlerContext} which provides access to the channel and pipeline
     * @param packet the {@link MessagePacket} holding the command and feedback to be processed
     */
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

    /**
     * Writes a message to the given channel. If the message is an instance of {@link WrappedInvocation}, it handles
     * the message by creating a new message packet and releasing resources as necessary. It also ensures proper
     * callback handling and exception propagation.
     *
     * @param ctx the {@link ChannelHandlerContext} to which the message should be written
     * @param msg the message to be written, which can be an instance of {@link WrappedInvocation}
     * @param promise the {@link ChannelPromise} to be used for notifying whether the write operation succeeded or failed
     * @throws Exception if an error occurs while writing the message or handling resources
     */
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

    /**
     * Invokes a timer task to check for idle callable initializers and handles their expiration if needed.
     * If there are no initializers, it returns immediately.
     * If initializers exist, it schedules a task in the executor to process them periodically.
     *
     * @param executor the executor used to schedule the idle check timer task
     */
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

                if (logger.isDebugEnabled()) {
                    logger.debug("Handle expired schedule task: PH[{}] PI[{}] RH[{}] RI[{}]", processHolder, processInvoker, remnantHolder, remnantInvoker);
                }

                if (!initializers.isEmpty()) {
                    executor.schedule(this, 1, TimeUnit.SECONDS);
                }
            }
        }, 1, TimeUnit.SECONDS);
    }

    /**
     * Handles the removal of a handler from the {@link ChannelHandlerContext}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ChannelHandler} now belongs to
     * @throws Exception is thrown if an error occurs
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        int count = initializer.releaseAll(c -> c.failure(
                new RemotingTimeoutException(String.format("Channel[%s] invoke timeout", ctx.channel().toString()))));
        if (logger.isDebugEnabled()) {
            logger.debug("Release entire invoke, handle count[{}]", count);
        }
    }

    /**
     * Handles an exception caught in the channel pipeline.
     *
     * @param ctx the context of the channel where the exception was caught
     * @param cause the exception that was caught
     * @throws Exception if any error occurs during exception handling
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (logger.isErrorEnabled()) {
            logger.error("Channel[{}] was caught", ctx.channel().toString(), cause.getMessage(), cause);
        }
        ctx.close();
    }
}
