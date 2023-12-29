package org.meteor.client.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.codec.MessageDecoder;
import org.meteor.remote.codec.MessageEncoder;
import org.meteor.remote.handle.HeartbeatDuplexHandler;
import org.meteor.remote.handle.ProcessDuplexHandler;
import org.meteor.remote.invoke.InvokeAnswer;
import org.meteor.remote.processor.ProcessCommand;
import org.meteor.remote.processor.Processor;
import org.meteor.remote.processor.RemoteException;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.util.NetworkUtil;
import org.meteor.remote.util.ProtoBufUtil;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

public class InternalChannelInitializer extends ChannelInitializer<SocketChannel> {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InternalChannelInitializer.class);
    private final SocketAddress address;
    private final ClientConfig config;
    private final ClientListener listener;
    private final ConcurrentMap<String, Promise<ClientChannel>> assembleChannels;
     InternalChannelInitializer(SocketAddress address, ClientConfig config, ClientListener listener, ConcurrentMap<String, Promise<ClientChannel>> assembleChannels) {
        this.address = address;
        this.config = config;
        this.listener = listener;
        this.assembleChannels = assembleChannels;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ClientChannel clientChannel = createClientChannel(config, socketChannel, address);
        socketChannel.pipeline()
                .addLast("packet-encoder", MessageEncoder.instance())
                .addLast("packet-decoder", new MessageDecoder())
                .addLast("connect-handler", new HeartbeatDuplexHandler(
                        config.getChannelKeepPeriodMs(), config.getChannelIdleTimeoutMs()
                ))
                .addLast("service-handler", new ProcessDuplexHandler(new InternalServiceProcessor(clientChannel)));
    }

    private class InternalServiceProcessor implements Processor, ProcessCommand.Client {

        private final ClientChannel clientChannel;

        public InternalServiceProcessor(ClientChannel channel) {
            this.clientChannel = channel;
        }

        @Override
        public void onActive(Channel channel, EventExecutor executor) {
            try {
                assembleChannels.computeIfAbsent(channel.id().asLongText(), k -> ImmediateEventExecutor.INSTANCE.newPromise()).setSuccess(clientChannel);
                channel.closeFuture().addListener((ChannelFutureListener) f -> listener.onChannelClosed(clientChannel));
                listener.onChannelActive(clientChannel);
            } catch (Throwable t) {
                channel.close();
            }
        }

        @Override
        public void process(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
            int length = data.readableBytes();
            try {
                switch (command) {
                    case ProcessCommand.Client.PUSH_MESSAGE -> onPushMessage(clientChannel, data, answer);
                    case ProcessCommand.Client.SERVER_OFFLINE -> onNodeOffline(clientChannel, data, answer);
                    case ProcessCommand.Client.TOPIC_INFO_CHANGED -> onTopicChanged(clientChannel, data, answer);
                    case ProcessCommand.Client.SYNC_MESSAGE -> onSyncMessage(clientChannel, data, answer);
                    default -> {
                        if (answer != null) {
                            answer.failure(RemoteException.of(ProcessCommand.Failure.COMMAND_EXCEPTION, "code unsupported: " + command));
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("<{}> processor is error, code={} length={}", NetworkUtil.switchAddress(clientChannel.channel()), command, length);
                if (answer != null) {
                    answer.failure(t);
                }
            }
        }
    }

    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new ClientChannel(clientConfig, channel, address);
    }

    private void onSyncMessage(ClientChannel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws Exception {
        SyncMessageSignal signal = ProtoBufUtil.readProto(data, SyncMessageSignal.parser());
        listener.onSyncMessage(channel, signal, data);
        if (answer != null) {
            answer.success(null);
        }
    }

    private void onTopicChanged(ClientChannel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws Exception {
        TopicChangedSignal signal = ProtoBufUtil.readProto(data, TopicChangedSignal.parser());
        listener.onTopicChanged(channel, signal);
        if (answer != null) {
            answer.success(null);
        }
    }

    private void onNodeOffline(ClientChannel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws Exception {
        NodeOfflineSignal signal = ProtoBufUtil.readProto(data, NodeOfflineSignal.parser());
        listener.onNodeOffline(channel, signal);
        if (answer != null) {
            answer.success(null);
        }
    }

    private void onPushMessage(ClientChannel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws Exception {
        MessagePushSignal signal = ProtoBufUtil.readProto(data, MessagePushSignal.parser());
        listener.onPushMessage(channel, signal, data);
        if (answer != null) {
            answer.success(null);
        }
    }
}
