package org.ostara.network;

import static org.ostara.remote.util.NetworkUtils.newEventExecutorGroup;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.config.ServerConfig;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.ProcessCommand;
import org.ostara.remote.processor.ProcessorAware;

@SuppressWarnings("all")
public class MessageProcessorHandler implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageProcessorHandler.class);

    private final ServerConfig config;
    private final EventExecutor commandExecutor;

    public MessageProcessorHandler(ServerConfig config) {
        this.config = config;

        EventExecutorGroup group = newEventExecutorGroup(config.getProcessCommandHandleThreadLimit(), "processor-command-executor").next();
        this.commandExecutor = group.next();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        logger.debug("Get remote active address<{}> successfully", channel.remoteAddress().toString());
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }
}
