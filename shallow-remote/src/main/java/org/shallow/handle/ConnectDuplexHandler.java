package org.shallow.handle;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.shallow.codec.MessagePacket;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.shallow.ObjectUtil.isNotNull;

public class ConnectDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConnectDuplexHandler.class);

    private final long heartPeriodMS;
    private final long idleTimeoutMs;
    private Future<?> heartFuture;
    private Future<?> idleFuture;

    private long lastWriteTime;
    private long lastReadTime;
    private long heartLastUpdateTime;

    public ConnectDuplexHandler(long heartPeriodMS, long idleTimeoutMs) {
        this.heartPeriodMS = StrictMath.max(heartPeriodMS, 0);
        this.idleTimeoutMs = StrictMath.max(idleTimeoutMs, 0);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        heartLastUpdateTime = lastReadTime = lastWriteTime = System.currentTimeMillis();
        if (heartPeriodMS > 0) {
            heartFuture = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    final long now = System.currentTimeMillis();
                    final long delay = heartPeriodMS - (now - StrictMath.min(lastReadTime, lastWriteTime));
                    if (delay > 0) {
                        heartFuture = ctx.executor().schedule(this, delay, TimeUnit.MICROSECONDS);
                    } else {
                        ctx.writeAndFlush(MessagePacket.newPacket(0, (byte) 0, null));
                        heartLastUpdateTime = lastWriteTime = now;
                        heartFuture = ctx.executor().schedule(this, heartPeriodMS, TimeUnit.MICROSECONDS);
                    }
                }
            }, heartPeriodMS, TimeUnit.MICROSECONDS);
        }

        if (idleTimeoutMs > 0) {
            idleFuture = ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    final long now = System.currentTimeMillis();
                    final long delay = idleTimeoutMs - (now - StrictMath.min(lastReadTime, lastWriteTime));
                    if (delay > 0) {
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
        if (idleTimeoutMs > 0 || heartPeriodMS > 0) {
            lastReadTime = System.currentTimeMillis();
        }

        if (msg instanceof final MessagePacket packet) {
            final int command = packet.command();
            final int rejoin = packet.rejoin();
            if (command <= 0 && rejoin == 0) {
                if (command == 0 && heartPeriodMS == 0) {
                    long now = System.currentTimeMillis();
                    if (now - heartLastUpdateTime > 1000) {
                        ctx.writeAndFlush(MessagePacket.newPacket(0, (byte) 0, null));
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
        if (idleTimeoutMs > 0 || heartPeriodMS > 0) {
            lastWriteTime = System.currentTimeMillis();
        }
        ctx.write(msg, promise);
    }
}
