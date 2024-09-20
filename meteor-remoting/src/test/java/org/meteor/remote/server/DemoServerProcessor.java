package org.meteor.remote.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.concurrent.TimeUnit;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.invoke.InvokedFeedback;
import org.meteor.remote.invoke.Processor;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.NetworkUtil;

/**
 * The DemoServerProcessor is responsible for handling specific commands received on a network channel.
 * It implements the Processor interface and provides mechanisms to process commands like echoing data,
 * introducing delays, and handling unsupported commands.
 */
public class DemoServerProcessor implements Processor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoServerProcessor.class);
    /**
     * An EventExecutorGroup instance used to manage and control a group of EventExecutor threads.
     * Initialized with a single thread and named "demo-buffer-group", this group is responsible
     * for executing tasks in a non-blocking, asynchronous manner. Used primarily within the
     * DemoServerProcessor class to handle delayed and parallel task execution.
     */
    private static final EventExecutorGroup executors = NetworkUtil.newEventExecutorGroup(1, "demo-buffer-group");
    /**
     * A thread-local storage for {@link ByteBuf} instances that are reused in the context of the
     * {@link DemoServerProcessor} to minimize memory allocations and deallocations during operations
     * such as echoing data back to a channel.
     */
    private static final FastThreadLocal<ByteBuf> BUFFER = new FastThreadLocal<>();

    /**
     * Invoked when a channel becomes active.
     * This method handles actions that need to be performed when the channel transitions to an active state.
     *
     * @param channel  The network channel that has become active.
     * @param executor The executor used to handle subsequent tasks asynchronously.
     */
    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        Processor.super.onActive(channel, executor);
    }

    /**
     * Processes a command received on a network channel.
     * Depending on the command, it delegates to the appropriate method for execution.
     *
     * @param channel  the {@link Channel} through which the command was received
     * @param command  an integer representing the specific command to be processed
     * @param data     the {@link ByteBuf} containing the data associated with the command
     * @param feedback the {@link InvokedFeedback} used for providing feedback after processing
     */
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

    /**
     * Handles the echo command by scheduling the data to be echoed back after a short delay.
     *
     * @param channel  the channel through which the command is received
     * @param data     the byte buffer containing the data to be echoed
     * @param feedback the feedback mechanism to indicate success or failure of the operation
     */
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

    /**
     * Schedules a feedback response after a delay, where the delay duration
     * is determined by the readable bytes in the provided ByteBuf.
     *
     * @param data the ByteBuf containing the delay duration if readable bytes are 8 or more; otherwise, the delay is 0
     * @param feedback the feedback object for giving the response back after the delay
     */
    private void wait(ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        int bytes = data.readableBytes();
        long delay = bytes < 8 ? 0 : data.readLong();
        executors.next().schedule(() -> {
            if (feedback != null) {
                feedback.success(ByteBufUtil.string2Buf(STR."Server wait \{data} ms"));
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Logs a warning message with the number of readable bytes in the provided ByteBuf,
     * if the warning log level is enabled.
     *
     * @param data the ByteBuf containing data for which the readable bytes should be logged
     */
    private void pass(ByteBuf data) {
        if (logger.isWarnEnabled()) {
            logger.warn("Readable bytes:{}", data.readableBytes());
        }
        // not feedback
    }
}
