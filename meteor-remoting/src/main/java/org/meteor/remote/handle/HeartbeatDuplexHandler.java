package org.meteor.remote.handle;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.codec.MessagePacket;

/**
 * HeartbeatDuplexHandler is a ChannelDuplexHandler that handles heartbeat signals and idle timeout for a channel.
 * <p>
 * The handler sends periodic heartbeat signals to ensure the connection is alive and optionally closes the
 * channel if it goes idle for a specified duration.
 */
public final class HeartbeatDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(HeartbeatDuplexHandler.class);
    /**
     * The interval, in milliseconds, at which heartbeat signals are sent.
     * Used to maintain connection liveness and detect idle connections.
     */
    private final long heartPeriodMillis;
    /**
     * The maximum time in milliseconds that a connection can remain idle before being closed.
     */
    private final long idleTimeoutMillis;
    /**
     * A Future task that manages the scheduled heartbeats to ensure the connection remains active.
     */
    private Future<?> heartFuture;
    /**
     * A Future instance representing the completion of a task scheduled to
     * monitor or manage the idle state of a network connection.
     *
     * This future can be used to check whether the idle state task has completed,
     * to retrieve any result the task might have produced, or to check for any
     * exceptions thrown during the execution of the task.
     */
    private Future<?> idleFuture;
    /**
     * Stores the timestamp of the last write operation in milliseconds.
     * This is used to track the last time data was written to the channel.
     */
    private long lastWriteTimeMillis;
    /**
     * Represents the timestamp of the last time data was read, measured in milliseconds.
     */
    private long lastReadTimeMillis;
    /**
     * Stores the most recent time, in milliseconds, that a heartbeat update was recorded.
     */
    private long heartLastUpdateTimeMillis;

    /**
     * Constructs a HeartbeatDuplexHandler with the specified heartbeat period and idle timeout.
     *
     * @param heartPeriodMillis the period in milliseconds between heartbeats
     * @param idleTimeoutMillis the timeout in milliseconds for idle connections
     */
    public HeartbeatDuplexHandler(long heartPeriodMillis, long idleTimeoutMillis) {
        this.heartPeriodMillis = StrictMath.max(heartPeriodMillis, 0);
        this.idleTimeoutMillis = StrictMath.max(idleTimeoutMillis, 0);
    }

    /**
     * Initializes heartbeat and idle timeout mechanisms when the handler is added to the channel pipeline.
     *
     * @param ctx The {@link ChannelHandlerContext} which this {@link ChannelHandler} belongs to.
     * @throws Exception If an error occurs during the addition of the handler.
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        heartLastUpdateTimeMillis = lastReadTimeMillis = lastWriteTimeMillis = System.currentTimeMillis();
        if (heartPeriodMillis > 0) {
            heartFuture = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    final long now = System.currentTimeMillis();
                    final long delay = heartPeriodMillis - (now - StrictMath.min(lastReadTimeMillis, lastWriteTimeMillis));
                    if (delay > 0) {
                        heartFuture = ctx.executor().schedule(this, delay, TimeUnit.MILLISECONDS);
                    } else {
                        ctx.writeAndFlush(MessagePacket.newPacket(0L, 0, null));
                        heartLastUpdateTimeMillis = lastWriteTimeMillis = now;
                        heartFuture = ctx.executor().schedule(this, heartPeriodMillis, TimeUnit.MILLISECONDS);
                    }
                }
            }, heartPeriodMillis, TimeUnit.MILLISECONDS);
        }

        if (idleTimeoutMillis > 0) {
            idleFuture = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    final long now = System.currentTimeMillis();
                    final long delay = idleTimeoutMillis - (now - StrictMath.min(lastReadTimeMillis, lastWriteTimeMillis));
                    if (delay > 0) {
                        idleFuture = ctx.executor().schedule(this, delay, TimeUnit.MILLISECONDS);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Channel[{}] is closing", ctx.channel().toString());
                        }
                        ctx.close();
                    }
                }
            }, idleTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Handles the removal of the handler from the context.
     *
     * Cancels scheduled future tasks for the heartbeat and idle timeout
     * if they exist, and sets them to null.
     *
     * @param ctx the context from where this handler is removed.
     * @throws Exception if an exception occurs during the removal process.
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (null != heartFuture) {
            heartFuture.cancel(false);
            heartFuture = null;
        }

        if (null != idleFuture) {
            idleFuture.cancel(false);
            idleFuture = null;
        }
    }

    /**
     * Handles reading data from the channel, managing heartbeats, and idle timeouts.
     *
     * @param ctx the context of the channel handler, which provides access to the channel and its associated pipeline
     * @param msg the message received, which may be a data packet or other protocol-specific object
     * @throws Exception if an error occurs while processing the message
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (idleTimeoutMillis > 0 || heartPeriodMillis > 0) {
            lastReadTimeMillis = System.currentTimeMillis();
        }

        if (msg instanceof final MessagePacket packet) {
            final int command = packet.command();
            final long feedback = packet.feedback();
            if (command <= 0 && feedback == 0L) {
                if (command == 0 && heartPeriodMillis == 0) {
                    long now = System.currentTimeMillis();
                    if (now - heartLastUpdateTimeMillis > 1000) {
                        ctx.writeAndFlush(MessagePacket.newPacket(0L, 0, null));
                        heartLastUpdateTimeMillis = lastWriteTimeMillis = now;
                    }
                }
                packet.release();
                return;
            }
        }
        ctx.fireChannelRead(msg);
    }

    /**
     * Overrides the write method to update the last write time and then delegates to the context's write method.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ChannelHandler} belongs to
     * @param msg the message to write
     * @param promise the {@link ChannelPromise} to be notified once the operation completes
     * @throws Exception if an error occurs
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (idleTimeoutMillis > 0 || heartPeriodMillis > 0) {
            lastWriteTimeMillis = System.currentTimeMillis();
        }
        ctx.write(msg, promise);
    }
}
