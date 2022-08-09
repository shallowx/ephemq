package org.shallow.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.RemoteException;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.util.NetworkUtil;
import org.shallow.invoke.ClientChannel;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.processor.ProcessorAware;

import static org.shallow.util.ObjectUtil.isNotNull;

public class ClientServiceProcessorAware implements ProcessorAware, ProcessCommand.Client {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientServiceProcessorAware.class);

    private final ClientChannel clientChannel;

    public ClientServiceProcessorAware(ClientChannel clientChannel) {
        this.clientChannel = clientChannel;
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        Promise<ClientChannel> promise = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool().assemblePromise(channel);
        if (isNotNull(promise)) {
            promise.setSuccess(clientChannel);
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            switch (command) {
                case RECEIVE_MESSAGE -> {}
                case TOPIC_CHANGED -> {}
                case CLUSTER_CHANGED -> {}
                default -> {
                    if (isNotNull(answer)) {
                        answer.failure(RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION,"Unsupported command exception <" + command + ">"));
                    }
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("[Client process] <{}> code:[{}] process error", NetworkUtil.switchAddress(channel), ProcessCommand.Client.ACTIVE.get(command));
            }
        }
    }
}
