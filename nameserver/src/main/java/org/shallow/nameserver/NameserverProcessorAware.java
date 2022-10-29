package org.shallow.nameserver;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.remote.RemoteException;
import org.shallow.remote.invoke.InvokeAnswer;
import org.shallow.remote.processor.ProcessCommand;
import org.shallow.remote.processor.ProcessorAware;

import static org.shallow.remote.util.NetworkUtil.switchAddress;

@SuppressWarnings("all")
public class NameserverProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(NameserverProcessorAware.class);

    public NameserverProcessorAware() {
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get remote active address<{}> successfully", channel.remoteAddress().toString());
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        try {
            switch (command) {
                default -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel<{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, "Not supported command ["+ command +"]"));
                }
            }
        } catch (Throwable cause) {
            if (logger.isErrorEnabled()) {
                logger.error("Channel<{}> - command [{}]", switchAddress(channel), command);
            }
            answerFailed(answer, cause);
        }
    }

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (answer != null) {
            answer.failure(cause);
        }
    }
}
