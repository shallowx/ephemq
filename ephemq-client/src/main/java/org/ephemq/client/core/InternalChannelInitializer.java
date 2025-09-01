package org.ephemq.client.core;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.remote.codec.MessageDecoder;
import org.ephemq.remote.codec.MessageEncoder;
import org.ephemq.remote.exception.RemotingException;
import org.ephemq.remote.handle.HeartbeatDuplexHandler;
import org.ephemq.remote.handle.ProcessDuplexHandler;
import org.ephemq.remote.invoke.Command;
import org.ephemq.remote.invoke.InvokedFeedback;
import org.ephemq.remote.invoke.Processor;
import org.ephemq.remote.proto.client.MessagePushSignal;
import org.ephemq.remote.proto.client.NodeOfflineSignal;
import org.ephemq.remote.proto.client.SyncMessageSignal;
import org.ephemq.remote.proto.client.TopicChangedSignal;
import org.ephemq.remote.util.NetworkUtil;
import org.ephemq.remote.util.ProtoBufUtil;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

/**
 * InternalChannelInitializer is a specialized subclass of ChannelInitializer.
 * It is responsible for initializing internal channels, particularly setting up
 * various handlers and processing different types of messages and events within a channel.
 */
public class InternalChannelInitializer extends ChannelInitializer<SocketChannel> {
    /**
     * Logger instance for the InternalChannelInitializer class.
     * Used to log debug, info, warning, and error messages specific to the channel initialization process.
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InternalChannelInitializer.class);
    /**
     * The network address used for socket connection initialization.
     */
    private final SocketAddress address;
    /**
     * Holds the configuration settings for the client.
     * This variable is used to initialize the client channel based on the provided configurations.
     */
    private final ClientConfig config;
    /**
     * An implementation of the {@link CombineListener} interface that handles various events
     * within a client channel, such as activation, closure, message pushes, topic changes,
     * node offline events, and synchronization messages.
     * <p>
     * This listener is used to process specific events and perform custom processing for each
     * event type within the {@link InternalChannelInitializer} class.
     */
    private final CombineListener listener;
    /**
     * A mapping between string keys and promises of ClientChannel instances.
     * This map is used to track the initialization and availability of client channels
     * within the internal channel initializer.
     */
    private final ConcurrentMap<String, Promise<ClientChannel>> channelOfPromise;

    /**
     * Initializes an internal channel with the given parameters.
     *
     * @param address The socket address to which the channel will be connected.
     * @param config The configuration for the client.
     * @param listener The listener to handle combined events.
     * @param channelOfPromise A concurrent map that holds promises for client channels.
     */
    InternalChannelInitializer(SocketAddress address, ClientConfig config, CombineListener listener, ConcurrentMap<String, Promise<ClientChannel>> channelOfPromise) {
        this.address = address;
        this.config = config;
        this.listener = listener;
        this.channelOfPromise = channelOfPromise;
    }

    /**
     * Initializes the channel by adding handlers to the channel's pipeline.
     * This method is called when a {@link SocketChannel} is registered to the EventLoop.
     *
     * @param socketChannel the {@code SocketChannel} to initialize
     * @throws Exception if an error occurs during channel initialization
     */
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

    /**
     * Creates a new {@link ClientChannel} instance using the provided client configuration,
     * channel, and socket address.
     *
     * @param clientConfig the configuration for the client channel
     * @param channel      the Netty channel associated with the client connection
     * @param address      the socket address for the channel
     * @return a newly created {@link ClientChannel} instance configured with the given parameters
     */
    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new ClientChannel(clientConfig, channel, address);
    }

    /**
     * Handles a synchronized message received from the given client channel.
     * This method processes the byte buffer data, parses it into a SyncMessageSignal,
     * and notifies the listener. It also manages the feedback response if provided.
     *
     * @param channel  the client channel from which the message was received
     * @param data     the byte buffer containing the serialized message data
     * @param feedback the feedback mechanism to signal success or failure of message processing
     * @throws Exception if an error occurs during message processing
     */
    private void onSyncMessage(ClientChannel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) throws Exception {
        SyncMessageSignal signal = ProtoBufUtil.readProto(data, SyncMessageSignal.parser());
        listener.onSyncMessage(channel, signal, data);
        if (feedback != null) {
            feedback.success(null);
        }
    }

    /**
     * Handles the event of a topic change by interpreting the data received and notifying
     * the associated listener. This method reads a protobuf message to get the details of the
     * topic change and relays this information to a designated listener.
     *
     * @param channel  the client channel through which the message was received
     * @param data     the ByteBuf containing the serialized data of the topic change event
     * @param feedback the feedback mechanism provided after handling the topic change
     * @throws Exception if an error occurs while processing the topic change event
     */
    private void onTopicChanged(ClientChannel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) throws Exception {
        TopicChangedSignal signal = ProtoBufUtil.readProto(data, TopicChangedSignal.parser());
        listener.onTopicChanged(channel, signal);
        if (feedback != null) {
            feedback.success(null);
        }
    }

    /**
     * Handles the event when a node goes offline. This method reads the {@code NodeOfflineSignal}
     * from the data buffer and notifies the appropriate listener. If feedback is provided,
     * it indicates success upon handling the signal.
     *
     * @param channel  the client channel through which the signal was received
     * @param data     the buffer containing the node offline signal data
     * @param feedback the feedback mechanism to report success or failure of the operation
     * @throws Exception if an error occurs while processing the node offline signal
     */
    private void onNodeOffline(ClientChannel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) throws Exception {
        NodeOfflineSignal signal = ProtoBufUtil.readProto(data, NodeOfflineSignal.parser());
        listener.onNodeOffline(channel, signal);
        if (feedback != null) {
            feedback.success(null);
        }
    }

    /**
     * Handles the push message received on the specified client channel.
     *
     * @param channel  the client channel on which the message is received
     * @param data     the message data in the form of a ByteBuf
     * @param feedback an optional feedback callback to indicate message handling success or failure
     * @throws Exception if there is an error processing the push message
     */
    private void onPushMessage(ClientChannel channel, ByteBuf data, InvokedFeedback<ByteBuf> feedback) throws Exception {
        MessagePushSignal signal = ProtoBufUtil.readProto(data, MessagePushSignal.parser());
        listener.onPushMessage(channel, signal, data);
        if (feedback != null) {
            feedback.success(null);
        }
    }

    private class InternalServiceProcessor implements Processor, Command.Client {

        /**
         * A client connection channel used within the internal service processor.
         * <p>
         * This instance handles the interaction with the client channel
         * to process commands, manage state changes, and facilitate communication
         * between the client and server.
         */
        private final ClientChannel clientChannel;

        /**
         * Creates a new instance of InternalServiceProcessor with the specified ClientChannel.
         *
         * @param channel the ClientChannel to be associated with this processor instance
         */
        public InternalServiceProcessor(ClientChannel channel) {
            this.clientChannel = channel;
        }

        /**
         * Handles the event when a channel becomes active. This method registers a promise with the channel,
         * sets up listeners for channel closure and activates the associated client channel.
         *
         * @param channel  the channel that has become active
         * @param executor the event executor associated with the channel
         */
        @Override
        public void onActive(Channel channel, EventExecutor executor) {
            try {
                channelOfPromise.computeIfAbsent(channel.id().asLongText(), k -> ImmediateEventExecutor.INSTANCE.newPromise()).setSuccess(clientChannel);
                channel.closeFuture().addListener((ChannelFutureListener) f -> listener.onChannelClosed(clientChannel));
                listener.onChannelActive(clientChannel);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error(t);
                }
                channel.close();
            }
        }

        /**
         * Processes commands received on the specified channel, delegating handling to the appropriate method
         * based on the command type. It manages feedback to indicate success or failure of the operation.
         *
         * @param channel  the channel through which the command was received
         * @param command  the type of command received
         * @param data     the ByteBuf containing the command data
         * @param feedback the feedback mechanism to report the outcome of processing the command
         */
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
                            feedback.failure(RemotingException.of(Command.Failure.COMMAND_EXCEPTION, String.format("Unsupported command[code: %s]", command)));
                        }
                    }
                }
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Channel[{}] processor is error, code[{}] length[{}]",
                            NetworkUtil.switchAddress(clientChannel.channel()), command, length);
                }

                if (feedback != null) {
                    feedback.failure(t);
                }
            }
        }
    }
}
