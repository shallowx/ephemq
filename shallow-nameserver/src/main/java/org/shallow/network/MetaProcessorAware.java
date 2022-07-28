package org.shallow.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.shallow.internal.MetaConfig;
import org.shallow.internal.MetaManager;
import org.shallow.RemoteException;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;

import static org.shallow.ObjectUtil.isNotNull;
import static org.shallow.util.NetworkUtil.switchAddress;

public class MetaProcessorAware implements ProcessorAware, ProcessCommand.NameServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetaProcessorAware.class);

    private final MetaManager metaManager;
    private final MetaConfig config;

    public MetaProcessorAware(MetaConfig config, MetaManager metaManager) {
        this.metaManager = metaManager;
        this.config = config;
    }

    @Override
    public void onActive(ChannelHandlerContext ctx) {
        if (logger.isDebugEnabled()) {
            logger.debug("[onActive] - active channel<{}>", ctx.channel());
        }
        ProcessorAware.super.onActive(ctx);
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            switch (command) {
                case REGISTER_NODE -> {}
                case OFFLINE -> {}
                default -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[Nameserver process]<{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, "Not supported command ["+ command +"]"));
                }
            }
    } catch (Throwable cause) {
        if (logger.isErrorEnabled()) {
            logger.error("[Nameserver process]<{}> - command [{}]", switchAddress(channel), command);
        }
        answerFailed(answer, cause);
    }
}

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (isNotNull(answer)) {
            answer.failure(cause);
        }
    }
}
