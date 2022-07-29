package org.shallow.handle;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.shallow.codec.MessagePacket;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.shallow.util.ObjectUtil.isNotNull;

public class ConnectDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConnectDuplexHandler.class);

    private static final int INT_ZERO = 0;
    private final long heartPeriodMs;
    private final long idleTimeoutMs;
    private Future<?> heartFuture;
    private Future<?> idleFuture;

    private long lastWriteTime;
    private long lastReadTime;
    private long heartLastUpdateTime;

    public ConnectDuplexHandler(long heartPeriodMS, long idleTimeoutMs) {
        this.heartPeriodMs = StrictMath.max(heartPeriodMS, INT_ZERO);
        this.idleTimeoutMs = StrictMath.max(idleTimeoutMs, INT_ZERO);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        heartLastUpdateTime = lastReadTime = lastWriteTime = System.currentTimeMillis();
        if (heartPeriodMs > INT_ZERO) {
            heartFuture = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    final long now = System.currentTimeMillis();
                    final long delay = heartPeriodMs - (now - StrictMath.min(lastReadTime, lastWriteTime));
                    if (delay > INT_ZERO) {
                        heartFuture = ctx.executor().schedule(this, delay, TimeUnit.MICROSECONDS);
                    } else {
                        ctx.writeAndFlush(MessagePacket.newPacket(INT_ZERO, (byte) INT_ZERO, null));
                        heartLastUpdateTime = lastWriteTime = now;
                        heartFuture = ctx.executor().schedule(this, heartPeriodMs, TimeUnit.MICROSECONDS);
                    }
                }
            }, heartPeriodMs, TimeUnit.MICROSECONDS);
        }

        if (idleTimeoutMs > INT_ZERO) {
            idleFuture = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    final long now = System.currentTimeMillis();
                    final long delay = idleTimeoutMs - (now - StrictMath.min(lastReadTime, lastWriteTime));
                    if (delay > INT_ZERO) {
                        idleFuture = ctx.executor().schedule(this, delay, TimeUnit.MICROSECONDS);
                    } else {
                        ctx.close();
                    }
                }
            }, idleTimeoutMs, TimeUnit.MICROSECONDS);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (isNotNull(heartFuture)){
            heartFuture.cancel(false);
            heartFuture = null;
        }

        if (isNotNull(idleFuture)){
            idleFuture.cancel(false);
            idleFuture = null;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (idleTimeoutMs > INT_ZERO || heartPeriodMs > INT_ZERO) {
            lastReadTime = System.currentTimeMillis();
        }

        if (msg instanceof final MessagePacket packet) {
            final byte command = packet.command();
            final int answer = packet.answer();
            if (command <= INT_ZERO && answer == INT_ZERO) {
                if (command == INT_ZERO && heartPeriodMs == INT_ZERO) {
                    long now = System.currentTimeMillis();
                    if (now - heartLastUpdateTime > 1000) {
                        ctx.writeAndFlush(MessagePacket.newPacket(INT_ZERO, (byte) INT_ZERO, null));
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
        if (idleTimeoutMs > INT_ZERO || heartPeriodMs > INT_ZERO) {
            lastWriteTime = System.currentTimeMillis();
        }
        ctx.write(msg, promise);
    }
}
