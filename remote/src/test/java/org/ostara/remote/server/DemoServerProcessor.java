package org.ostara.remote.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.FastThreadLocal;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.ProcessorAware;
import org.ostara.remote.util.ByteBufUtils;
import org.ostara.remote.util.NetworkUtils;

import java.util.concurrent.TimeUnit;

public class DemoServerProcessor implements ProcessorAware {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoServerProcessor.class);
    private static final EventExecutorGroup executors = NetworkUtils.newEventExecutorGroup(1, "demo-buffer-group");
    private static final FastThreadLocal<ByteBuf> BUFFER = new FastThreadLocal<>();

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        ProcessorAware.super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        switch (command) {
            case 1 -> echo(channel, data, answer);
            case 2 -> wait(data, answer);
            case 3 -> pass(data);
            default -> {
                if (answer != null) {
                    answer.failure(new UnsupportedOperationException("Code invalid:" + command));
                }
            }
        }
    }

    private void echo(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
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
            if (answer != null) {
                answer.success(echo);
            } else {
                echo.release();
            }
        },10, TimeUnit.MILLISECONDS);
    }

    private void wait(ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        int bytes = data.readableBytes();
        long delay = bytes < 8 ? 0 : data.readLong();
        executors.next().schedule(() -> {
            if (answer != null) {
                answer.success(ByteBufUtils.string2Buf("server wait" + data + "ms"));
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void pass(ByteBuf data) {
        logger.warn("Readable bytes:{}", data.readableBytes());
        // not answer
    }
}
