package org.shallow.internal;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.Client;
import org.shallow.RemoteException;
import org.shallow.Type;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.notify.MessagePullSignal;
import org.shallow.proto.notify.MessagePushSignal;
import org.shallow.util.NetworkUtil;
import org.shallow.invoke.ClientChannel;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.processor.ProcessorAware;

import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ProtoBufUtil.readProto;

public class ClientServiceProcessorAware implements ProcessorAware, ProcessCommand.Client {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientServiceProcessorAware.class);

    private final ClientChannel clientChannel;
    private final Listener listener;

    public ClientServiceProcessorAware(ClientChannel clientChannel, Client client) {
        this.clientChannel = clientChannel;
        this.listener = client.getListener();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        Promise<ClientChannel> promise = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool().assemblePromise(channel);
        if (isNotNull(promise)) {
            promise.setSuccess(clientChannel);
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        try {
            switch (command) {
                case HANDLE_MESSAGE -> {
                    if (type == Type.PUSH.sequence()) {
                        onPushMessage(version, data, answer);
                        return;
                    }

                    onPullMessage(data, answer);
                }
                case TOPIC_CHANGED -> {
                    onPartitionChanged(answer);
                }
                case CLUSTER_CHANGED -> {

                }
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

    private void onPartitionChanged(InvokeAnswer<ByteBuf> answer) {
        listener.onPartitionChanged(null, null);
        trySuccess(answer);
    }

    private void onPushMessage(short version, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws InvalidProtocolBufferException {
        MessagePushSignal signal = readProto(data, MessagePushSignal.parser());
        String topic = signal.getTopic();
        String queue = signal.getQueue();
        int epoch = signal.getEpoch();
        long index = signal.getIndex();

        listener.onPushMessage(version, topic, queue, epoch, index, data);
        trySuccess(answer);
    }

    private void onPullMessage(ByteBuf data, InvokeAnswer<ByteBuf> answer) throws InvalidProtocolBufferException {
        MessagePullSignal signal = readProto(data, MessagePullSignal.parser());

        String topic = signal.getTopic();
        String queue = signal.getQueue();
        int ledger = signal.getLedger();
        int limit = signal.getLimit();
        int epoch = signal.getEpoch();
        long index = signal.getIndex();

        listener.onPullMessage(topic, queue, ledger, limit, epoch, index, data);
        trySuccess(answer);
    }

    private void trySuccess(InvokeAnswer<ByteBuf> answer) {
        if (isNotNull(answer)) {
            answer.success(null);
        }
    }
}
