package org.shallow.handle;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class ConnectDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConnectDuplexHandler.class);

    private final long heartPeriodMS;
    private final long idleTimeoutMs;

    public ConnectDuplexHandler(long heartPeriodMS, long idleTimeoutMs) {
        this.heartPeriodMS = StrictMath.max(heartPeriodMS, 0);
        this.idleTimeoutMs = StrictMath.max(idleTimeoutMs, 0);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

    }
}
