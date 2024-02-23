package org.meteor.remote.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.FastThreadLocal;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.invoke.InvokedFeedback;
import org.meteor.remote.invoke.Processor;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.NetworkUtil;

import java.util.concurrent.TimeUnit;

public class DemoServerProcessor implements Processor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoServerProcessor.class);
    private static final EventExecutorGroup executors = NetworkUtil.newEventExecutorGroup(1, "demo-buffer-group");
    private static final FastThreadLocal<ByteBuf> BUFFER = new FastThreadLocal<>();

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        Processor.super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        switch (command) {
            case 1 -> echo(channel, data, feedback);
            case 2 -> wait(data, feedback);
            case 3 -> pass(data);
            default -> {
                if (feedback != null) {
                    feedback.failure(new UnsupportedOperationException("Code invalid-" + command));
                }
            }
        }
    }

    private void echo(Channel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        ByteBuf echo = data.retainedSlice();
        ByteBuf retain = data.retain();

        executors.next().schedule(() -> {
            ByteBuf temp = BUFFER.get();
            if (temp == null) {
                temp = channel.alloc().ioBuffer(1048576, 1048576);
                BUFFER.set(temp);
            } else {
                temp.clear();
            }

            temp.writeBytes(retain);
            retain.release();
            if (feedback != null) {
                feedback.success(echo);
            } else {
                echo.release();
            }
        }, 10, TimeUnit.MILLISECONDS);
    }

    private void wait(ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        int bytes = data.readableBytes();
        long delay = bytes < 8 ? 0 : data.readLong();
        executors.next().schedule(() -> {
            if (feedback != null) {
                feedback.success(ByteBufUtil.string2Buf("Server wait " + data + " ms"));
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void pass(ByteBuf data) {
        logger.warn("Readable bytes:{}", data.readableBytes());
        // not feedback
    }
}
