package org.meteor.remote.handle;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.codec.MessagePacket;

public final class HeartbeatDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(HeartbeatDuplexHandler.class);
    private final long heartPeriodMillis;
    private final long idleTimeoutMillis;
    private Future<?> heartFuture;
    private Future<?> idleFuture;
    private long lastWriteTimeMillis;
    private long lastReadTimeMillis;
    private long heartLastUpdateTimeMillis;

    public HeartbeatDuplexHandler(long heartPeriodMillis, long idleTimeoutMillis) {
        this.heartPeriodMillis = StrictMath.max(heartPeriodMillis, 0);
        this.idleTimeoutMillis = StrictMath.max(idleTimeoutMillis, 0);
    }

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

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (idleTimeoutMillis > 0 || heartPeriodMillis > 0) {
            lastWriteTimeMillis = System.currentTimeMillis();
        }
        ctx.write(msg, promise);
    }
}
