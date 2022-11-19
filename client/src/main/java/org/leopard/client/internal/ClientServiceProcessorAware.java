package org.leopard.client.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.leopard.client.metadata.MetadataWriter;
import org.leopard.client.Client;
import org.leopard.remote.proto.notify.MessagePullSignal;
import org.leopard.remote.proto.notify.MessagePushSignal;
import org.leopard.remote.RemoteException;
import org.leopard.remote.Type;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.client.pool.DefaultFixedChannelPoolFactory;
import org.leopard.remote.processor.ProcessCommand;
import org.leopard.remote.util.NetworkUtils;
import org.leopard.remote.invoke.InvokeAnswer;
import org.leopard.remote.processor.ProcessorAware;

import static org.leopard.remote.util.ProtoBufUtils.readProto;

public class ClientServiceProcessorAware implements ProcessorAware, ProcessCommand.Client {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientServiceProcessorAware.class);

    private final ClientChannel clientChannel;
    private final ClientListener listener;
    private final Client client;

    public ClientServiceProcessorAware(ClientChannel clientChannel, Client client) {
        this.clientChannel = clientChannel;
        this.listener = client.getListener();
        this.client = client;
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        Promise<ClientChannel> promise = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool().assemblePromise(channel);
        if (null != promise) {
            promise.setSuccess(clientChannel);
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        try {
            switch (command) {
                case HANDLE_MESSAGE -> {
                    if (type == Type.PUSH.sequence()) {
                        onPushMessage(channel, version, data, answer);
                        return;
                    }

                    onPullMessage(channel, data, answer);
                }

                case TOPIC_CHANGED -> {
                    onPartitionChanged(answer);
                }

                case CLUSTER_CHANGED -> {
                    MetadataWriter metadataManager = client.getMetadataManager();
                    metadataManager.refreshMetadata();
                }

                default -> {
                    if (logger.isErrorEnabled()) {
                        logger.error("Unsupported command exception <{}>", command);
                    }

                    if (null != answer) {
                        answer.failure(RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION,"Unsupported command exception <" + command + ">"));
                    }
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Client process <{}> code:[{}] process error", NetworkUtils.switchAddress(channel), ProcessCommand.Client.ACTIVE.get(command));
            }
            tryFailure(answer, t);
        }
    }

    private void onPartitionChanged(InvokeAnswer<ByteBuf> answer) {
        try {
            listener.onPartitionChanged(null, null);
            trySuccess(answer);
        } catch (Throwable t) {
            tryFailure(answer, t);
        }
    }

    private void onPushMessage(Channel channel, short version, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            MessagePushSignal signal = readProto(data, MessagePushSignal.parser());
            String topic = signal.getTopic();
            String queue = signal.getQueue();
            int epoch = signal.getEpoch();
            long index = signal.getIndex();
            int ledgerId = signal.getLedgerId();

            listener.onPushMessage(channel, ledgerId, version, topic, queue, epoch, index, data);
            trySuccess(answer);
        } catch (Throwable t) {
            tryFailure(answer, t);
        }
    }

    private void onPullMessage(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            MessagePullSignal signal = readProto(data, MessagePullSignal.parser());

            String topic = signal.getTopic();
            String queue = signal.getQueue();
            int ledger = signal.getLedger();
            int limit = signal.getLimit();
            int epoch = signal.getEpoch();
            long index = signal.getIndex();

            listener.onPullMessage(channel, ledger, topic, queue, ledger, limit, epoch, index, data);
            trySuccess(answer);
        } catch (Throwable t) {
            tryFailure(answer, t);
        }
    }

    private void trySuccess(InvokeAnswer<ByteBuf> answer) {
        if (null != answer) {
            answer.success(null);
        }
    }

    private void tryFailure(InvokeAnswer<ByteBuf> answer, Throwable t) {
        if (null != answer) {
            answer.failure(t);
        }
    }
}
