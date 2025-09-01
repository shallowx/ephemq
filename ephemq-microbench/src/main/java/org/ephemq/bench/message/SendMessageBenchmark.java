package org.ephemq.bench.message;

import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.ephemq.client.core.ClientChannel;
import org.ephemq.client.core.ClientConfig;
import org.ephemq.client.core.MessageLedger;
import org.ephemq.client.core.MessageRouter;
import org.ephemq.common.util.TopicPatternUtil;
import org.ephemq.remote.invoke.Callable;
import org.ephemq.remote.proto.MessageMetadata;
import org.ephemq.remote.proto.server.SendMessageRequest;
import org.ephemq.remote.proto.server.SendMessageResponse;
import org.ephemq.remote.util.ByteBufUtil;
import org.ephemq.remote.util.ProtoBufUtil;
import org.openjdk.jmh.annotations.*;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A benchmark class for measuring the performance of sending messages using the JMH library.
 * <p>
 * The class is annotated with JMH annotations to specify the benchmark mode, warmup iterations,
 * measurement iterations, number of threads, state, and output time unit.
 */
@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SendMessageBenchmark {
    /**
     *  Represents the topic to which messages will be sent in the SendMessageBenchmark class.
     */
    private static final String TOPIC = "test_topic";
    /**
     *
     */
    private static final String QUEUE = "test_queue";
    /**
     * The {@code messageRouter} is a static final instance of the {@link MessageRouter} class
     * used within the {@code SendMessageBenchmark} class. It is initialized with a token value
     * of 0, a topic defined by the {@code TOPIC} constant, and a map containing a single
     * {@link MessageLedger} entry.
     * <p>
     * This instance is employed for routing messages to the appropriate message ledger based
     * on the topic and queue during the benchmarking of the send message functionality.
     */
    private static final MessageRouter messageRouter = new MessageRouter(0, TOPIC, new HashMap<>() {{
        put(1, new MessageLedger(0, 0, new InetSocketAddress("127.0.0.1", 8080), null, TOPIC, 1));
    }});
    /**
     * A static map used for storing additional properties or metadata
     * related to the message to be sent. The keys and values are both strings.
     */
    private static final Map<String, String> extras = new HashMap<>();
    /**
     * A static final buffer that holds the message to be sent in the SendMessageBenchmark operations.
     * It is initialized with a test string converted to ByteBuf using ByteBufUtil.string2Buf method.
     */
    private static final ByteBuf message = ByteBufUtil.string2Buf("test");
    /**
     * A promise object that can be used to track the completion of asynchronous operations, in this case,
     * related to sending messages. This promise is instantiated using the ImmediateEventExecutor to handle
     * immediate execution of tasks.
     */
    private static final Promise<SendMessageResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
    /**
     * The clientChannel is a pre-configured static final instance of ClientChannel.
     * It is initialized with custom ClientConfig, an embedded Netty channel, and
     * the localhost address with port 8080. This channel is utilized for
     * performing network operations such as sending messages within the
     * SendMessageBenchmark class.
     */
    private static final ClientChannel clientChannel = new ClientChannel(new ClientConfig(), new EmbeddedChannel(), new InetSocketAddress("127.0.0.1", 8080));
    /**
     * A ByteBuf instance used in the SendMessageBenchmark class to hold
     * message data for sending.
     */
    private ByteBuf buf;

    /**
     * Sends a message to a designated queue and topic. This method validates the queue and topic names,
     * retrieves the appropriate message ledger and leader, and constructs and sends a message request.
     *
     * @throws IllegalStateException if the message ledger or leader is not found
     * @throws NullPointerException if the queue or topic is null
     */
    @Benchmark
    public void send() {
        TopicPatternUtil.validateQueue(QUEUE);
        TopicPatternUtil.validateTopic(TOPIC);

        MessageRouter router = messageRouter;

        MessageLedger ledger = router.routeLedger(QUEUE);
        if (ledger == null) {
            throw new IllegalStateException("Message ledger not found");
        }

        SocketAddress leader = ledger.leader();
        if (leader == null) {
            throw new IllegalStateException("Message leader not found");
        }

        int marker = router.routeMarker(QUEUE);
        SendMessageRequest request = SendMessageRequest.newBuilder().setLedger(ledger.id()).setMarker(marker).build();
        MessageMetadata metadata = buildMetadata();
        sendMessage(100, promise, request, metadata, message);
    }

    /**
     * Sends a message with the specified request, metadata, and message content, within a given timeout.
     *
     * @param timeoutMs the timeout in milliseconds for the message send operation
     * @param promise the promise to be fulfilled with the response of the send operation
     * @param request the request object containing details about the message to be sent
     * @param metadata the metadata associated with the message
     * @param message the actual message content to be sent
     */
    public void sendMessage(int timeoutMs, Promise<SendMessageResponse> promise, SendMessageRequest request, MessageMetadata metadata, ByteBuf message) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, SendMessageResponse.parser());
            buf = assembleSendMessageData(clientChannel.allocator(), request, metadata, message);
        } catch (Throwable t) {

        } finally {
            buf.release();
        }
    }

    /**
     * Assembles and constructs the data required for sending a message.
     *
     * @param allocator The ByteBufAllocator used to allocate memory for the ByteBuf.
     * @param request The SendMessageRequest containing the request details.
     * @param metadata The MessageMetadata containing metadata information.
     * @param message The ByteBuf containing the actual message data to be sent.
     * @return The ByteBuf containing the assembled message data ready to be sent.
     * @throws RuntimeException if any error occurs during the assembly of message data.
     */
    private ByteBuf assembleSendMessageData(ByteBufAllocator allocator, SendMessageRequest request, MessageMetadata metadata, ByteBuf message) {
        ByteBuf data = null;
        try {
            int length = ProtoBufUtil.protoLength(request) + ProtoBufUtil.protoLength(metadata) + ByteBufUtil.bufLength(message);
            data = allocator.ioBuffer(length);

            ProtoBufUtil.writeProto(data, request);
            ProtoBufUtil.writeProto(data, metadata);
            if (message != null && message.isReadable()) {
                data.writeBytes(message, message.readerIndex(), message.readableBytes());
            }
            return data;
        } catch (Throwable t) {
            ByteBufUtil.release(data);
            throw new RuntimeException("Assemble send message failed");
        }
    }

    /**
     * Assembles a callback for invoking a promise based on the provided parser.
     *
     * @param <T> the type of the result expected by the promise
     * @param promise the promise to be completed based on the parsing result
     * @param parser the parser used to parse the ByteBuf
     * @return a Callable that processes the ByteBuf and completes the promise,
     *         or null if the promise is null
     */
    private <T> Callable<ByteBuf> assembleInvokeCallback(Promise<T> promise, Parser<T> parser) {
        return promise == null ? null : (v, c) -> {
            if (c == null) {
                try {
                    promise.trySuccess(ProtoBufUtil.readProto(v, parser));
                } catch (Throwable t) {
                    promise.tryFailure(t);
                }
            } else {
                promise.tryFailure(c);
            }
        };
    }

    /**
     * Builds and returns a new instance of MessageMetadata.
     * <p>
     * This method constructs a MessageMetadata object using a builder. It sets the topic and queue
     * from the constants defined in the SendMessageBenchmark class. Additionally, it adds any extra
     * key-value pairs provided in the extras map of the SendMessageBenchmark class to the metadata.
     *
     * @return a new instance of MessageMetadata containing the topic, queue, and any extra key-value pairs.
     */
    private MessageMetadata buildMetadata() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder().setTopic(SendMessageBenchmark.TOPIC).setQueue(SendMessageBenchmark.QUEUE);
        for (Map.Entry<String, String> entry : SendMessageBenchmark.extras.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key != null && value != null) {
                builder.putExtras(key, value);
            }
        }
        return builder.build();
    }
}
