package org.meteor.bench.message;

import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.meteor.client.internal.ClientChannel;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.internal.MessageLedger;
import org.meteor.client.internal.MessageRouter;
import org.meteor.client.util.TopicPatternUtil;
import org.meteor.remote.invoke.Callable;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.proto.server.SendMessageRequest;
import org.meteor.remote.proto.server.SendMessageResponse;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;
import org.openjdk.jmh.annotations.*;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SendMessageBenchmark {
    @Benchmark
    public void send() { //do-send part
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

    private static final String TOPIC = "test_topic";
    private static final String QUEUE = "test_queue";
    private static final MessageRouter messageRouter = new MessageRouter(0, TOPIC, new HashMap<>(){{
        put(1, new MessageLedger(0,0,new InetSocketAddress("127.0.0.1", 8080), null, TOPIC, 1));
    }});
    private static final  Map<String, String> extras = new HashMap<>();
    private static final ByteBuf message = ByteBufUtil.string2Buf("test");
    private static final Promise<SendMessageResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
    private static final ClientChannel clientChannel = new ClientChannel(new ClientConfig(), new EmbeddedChannel(), new InetSocketAddress("127.0.0.1", 8080));
    private ByteBuf buf;

    public void sendMessage(int timeoutMs, Promise<SendMessageResponse> promise, SendMessageRequest request, MessageMetadata metadata, ByteBuf message) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, SendMessageResponse.parser());
            buf = assembleSendMessageData(clientChannel.allocator(), request, metadata, message);
        } catch (Throwable t) {

        } finally {
            buf.release();
        }
    }

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
