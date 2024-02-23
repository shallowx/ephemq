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
import org.meteor.remote.invoke.Command;
import org.meteor.remote.invoke.InvokedFeedback;
import org.meteor.remote.invoke.Processor;
import org.meteor.remote.invoke.RemoteException;
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
    private final CombineListener listener;
    private final ConcurrentMap<String, Promise<ClientChannel>> channelOfPromise;
     InternalChannelInitializer(SocketAddress address, ClientConfig config, CombineListener listener, ConcurrentMap<String, Promise<ClientChannel>> channelOfPromise) {
        this.address = address;
        this.config = config;
        this.listener = listener;
        this.channelOfPromise = channelOfPromise;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ClientChannel clientChannel = createClientChannel(config, socketChannel, address);
        socketChannel.pipeline()
                .addLast("packet-encoder", MessageEncoder.instance())
                .addLast("packet-decoder", new MessageDecoder())
                .addLast("connect-handler", new HeartbeatDuplexHandler(
                        config.getChannelKeepPeriodMilliseconds(), config.getChannelIdleTimeoutMilliseconds()
                ))
                .addLast("service-handler", new ProcessDuplexHandler(new InternalServiceProcessor(clientChannel)));
    }

    private class InternalServiceProcessor implements Processor, Command.Client {

        private final ClientChannel clientChannel;

        public InternalServiceProcessor(ClientChannel channel) {
            this.clientChannel = channel;
        }

        @Override
        public void onActive(Channel channel, EventExecutor executor) {
            try {
                channelOfPromise.computeIfAbsent(channel.id().asLongText(), k -> ImmediateEventExecutor.INSTANCE.newPromise()).setSuccess(clientChannel);
                channel.closeFuture().addListener((ChannelFutureListener) f -> listener.onChannelClosed(clientChannel));
                listener.onChannelActive(clientChannel);
            } catch (Throwable t) {
                channel.close();
            }
        }

        @Override
        public void process(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
            int length = data.readableBytes();
            try {
                switch (command) {
                    case Command.Client.PUSH_MESSAGE -> onPushMessage(clientChannel, data, feedback);
                    case Command.Client.SERVER_OFFLINE -> onNodeOffline(clientChannel, data, feedback);
                    case Command.Client.TOPIC_CHANGED -> onTopicChanged(clientChannel, data, feedback);
                    case Command.Client.SYNC_MESSAGE -> onSyncMessage(clientChannel, data, feedback);
                    default -> {
                        if (feedback != null) {
                            feedback.failure(RemoteException.of(Command.Failure.COMMAND_EXCEPTION, "code[" + command + "]" + " unsupported"));
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Channel[{}] processor is error, code[{}] length[{}]", NetworkUtil.switchAddress(clientChannel.channel()), command, length);
                if (feedback != null) {
                    feedback.failure(t);
                }
            }
        }
    }

    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new ClientChannel(clientConfig, channel, address);
    }

    private void onSyncMessage(ClientChannel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) throws Exception {
        SyncMessageSignal signal = ProtoBufUtil.readProto(data, SyncMessageSignal.parser());
        listener.onSyncMessage(channel, signal, data);
        if (feedback != null) {
            feedback.success(null);
        }
    }

    private void onTopicChanged(ClientChannel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) throws Exception {
        TopicChangedSignal signal = ProtoBufUtil.readProto(data, TopicChangedSignal.parser());
        listener.onTopicChanged(channel, signal);
        if (feedback != null) {
            feedback.success(null);
        }
    }

    private void onNodeOffline(ClientChannel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) throws Exception {
        NodeOfflineSignal signal = ProtoBufUtil.readProto(data, NodeOfflineSignal.parser());
        listener.onNodeOffline(channel, signal);
        if (feedback != null) {
            feedback.success(null);
        }
    }

    private void onPushMessage(ClientChannel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) throws Exception {
        MessagePushSignal signal = ProtoBufUtil.readProto(data, MessagePushSignal.parser());
        listener.onPushMessage(channel, signal, data);
        if (feedback != null) {
            feedback.success(null);
        }
    }
}
