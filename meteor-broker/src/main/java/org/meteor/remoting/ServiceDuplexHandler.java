package org.meteor.remoting;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.handle.ProcessDuplexHandler;
import org.meteor.remote.invoke.Processor;
import org.meteor.support.Manager;

/**
 * The ServiceDuplexHandler is a specialized handler that extends the ProcessDuplexHandler.
 * This class handles channel state changes and exceptions specifically for a service context,
 * integrating closely with a Manager to manage connections and internal logging.
 */
public class ServiceDuplexHandler extends ProcessDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceDuplexHandler.class);
    /**
     * The manager responsible for handling connections, dispatching events, logging,
     * and various other operations within the service context.
     */
    private final Manager manager;

    /**
     * Constructs a new ServiceDuplexHandler.
     *
     * @param manager   the manager responsible for overseeing channel operations and maintaining connections
     * @param processor the processor handling the processing of various channel events and commands
     */
    public ServiceDuplexHandler(Manager manager, Processor processor) {
        super(processor);
        this.manager = manager;
    }

    /**
     * Handles the event when the channel becomes inactive. When a channel is no longer active, it removes
     * the associated channel from the manager's connection list and logs the event if debug logging is enabled.
     *
     * @param ctx the context object for this handler, providing access to the channel and its configuration
     * @throws Exception if an error occurs during processing
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        manager.getConnection().remove(channel);
        super.channelInactive(ctx);
        if (logger.isDebugEnabled()) {
            logger.debug("Service duplex inactive channel, and local_address[{}], remote_address[{}]",
                    channel.localAddress().toString(), channel.remoteAddress().toString());
        }
    }

    /**
     * Handles exceptions that are caught during the operation of the channel.
     * This method will remove the channel from the manager's connection, close
     * the context, and log the exception details if debugging is enabled.
     *
     * @param ctx   the context of the channel handler
     * @param cause the throwable that was caught
     * @throws Exception if an error occurs during exception handling
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Channel channel = ctx.channel();
        manager.getConnection().remove(channel);
        ctx.close();
        if (logger.isDebugEnabled()) {
            logger.debug("Service duplex caught channel, and local address[{}], remote address[{}]",
                    channel.localAddress().toString(), channel.remoteAddress().toString());
        }
    }
}
