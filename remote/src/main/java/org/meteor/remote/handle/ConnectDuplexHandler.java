package org.meteor.remote.handle;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.codec.MessagePacket;

import java.util.concurrent.TimeUnit;


public final class ConnectDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConnectDuplexHandler.class);

    private final long heartPeriodMs;
    private final long idleTimeoutMs;
    private Future<?> heartFuture;
    private Future<?> idleFuture;

    private long lastWriteTime;
    private long lastReadTime;
    private long heartLastUpdateTime;

    public ConnectDuplexHandler(long heartPeriodMS, long idleTimeoutMs) {
        this.heartPeriodMs = StrictMath.max(heartPeriodMS, 0);
        this.idleTimeoutMs = StrictMath.max(idleTimeoutMs, 0);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        heartLastUpdateTime = lastReadTime = lastWriteTime = System.currentTimeMillis();
        if (heartPeriodMs > 0) {
            heartFuture = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    final long now = System.currentTimeMillis();
                    final long delay = heartPeriodMs - (now - StrictMath.min(lastReadTime, lastWriteTime));
                    if (delay > 0) {
                        heartFuture = ctx.executor().schedule(this, delay, TimeUnit.MILLISECONDS);
                    } else {
                        ctx.writeAndFlush(MessagePacket.newPacket(0, 0, null));
                        heartLastUpdateTime = lastWriteTime = now;
                        heartFuture = ctx.executor().schedule(this, heartPeriodMs, TimeUnit.MILLISECONDS);
                    }
                }
            }, heartPeriodMs, TimeUnit.MILLISECONDS);
        }

        if (idleTimeoutMs > 0) {
            idleFuture = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    final long now = System.currentTimeMillis();
                    final long delay = idleTimeoutMs - (now - StrictMath.min(lastReadTime, lastWriteTime));
                    if (delay > 0) {
                        idleFuture = ctx.executor().schedule(this, delay, TimeUnit.MILLISECONDS);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Channel<{}> is closed", ctx.channel().toString());
                        }
                        ctx.close();
                    }
                }
            }, idleTimeoutMs, TimeUnit.MILLISECONDS);
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
        if (idleTimeoutMs > 0 || heartPeriodMs > 0) {
            lastReadTime = System.currentTimeMillis();
        }

        if (msg instanceof final MessagePacket packet) {
            final int command = packet.command();
            final int answer = packet.answer();
            if (command <= 0 && answer == 0) {
                if (command == 0 && heartPeriodMs == 0) {
                    long now = System.currentTimeMillis();
                    if (now - heartLastUpdateTime > 1000) {
                        ctx.writeAndFlush(MessagePacket.newPacket(0, 0, null));
                        heartLastUpdateTime = lastWriteTime = now;
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
        if (idleTimeoutMs > 0 || heartPeriodMs > 0) {
            lastWriteTime = System.currentTimeMillis();
        }
        ctx.write(msg, promise);
    }
}
