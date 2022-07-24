package org.shallow.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import org.shallow.ObjectUtil;
import org.shallow.RemoteException;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.ChannelPoolFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.util.NetworkUtil;
import org.shallow.invoke.ClientChannel;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.processor.ProcessorAware;

public class ClientServiceProcessorAware implements ProcessorAware, ProcessCommand.Client {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientServiceProcessorAware.class);

    private final ClientChannel clientChannel;

    public ClientServiceProcessorAware(ClientChannel clientChannel) {
        this.clientChannel = clientChannel;
    }

    @Override
    public void onActive(ChannelHandlerContext ctx) {
        Promise<ClientChannel> promise = ChannelPoolFactory.INSTANCE.obtainChannelPool().assemblePromise(ctx.channel());
        if (ObjectUtil.isNotNull(promise)) {
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
                    if (ObjectUtil.isNotNull(answer)) {
                        answer.failure(RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION,"Unsupported command exception <" + command + ">"));
                    }
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("[Client process] <{}> code:[{}] process error", NetworkUtil.switchAddress(channel), ProcessCommand.Client.ACTIVE.obtain(command));
            }
        }
    }
}
