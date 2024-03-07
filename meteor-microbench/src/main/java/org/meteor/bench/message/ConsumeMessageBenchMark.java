package org.meteor.bench.message;

import io.netty.buffer.ByteBuf;
import org.meteor.client.consumer.MessageListener;
import org.meteor.common.message.MessageId;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.util.ByteBufUtil;
import org.openjdk.jmh.annotations.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ConsumeMessageBenchMark {
    @Setup
    public void setUp() {
        this.metadata = buildMetadata(new HashMap<>());
        this.wholeQueueTopics.put(QUEUE, new HashMap<>() {{
            put(TOPIC, org.meteor.client.consumer.Mode.APPEND);
        }});
        this.listener = (topic, queue, messageId, message, extras) -> {
            // keep empty, because this is user unittest that not need included in the scope of performance testing
        };
    }

    @Benchmark
    public void handle() {
        String topic = metadata.getTopic();
        String queue = metadata.getQueue();
        Map<String, org.meteor.client.consumer.Mode> topicModes = this.wholeQueueTopics.get(queue);
        org.meteor.client.consumer.Mode mode = topicModes == null ? null : topicModes.get(topic);
        if (mode == null || mode == org.meteor.client.consumer.Mode.DELETE) {
            return;
        }

        Map<String, String> extras = new HashMap<>();
        listener.onMessage(topic, queue, messageId, buf, extras);
    }

    @TearDown
    public void clear() {
        this.messageId = null;
        this.wholeQueueTopics = null;
        this.metadata = null;
    }

    private final ByteBuf buf = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
    private MessageMetadata metadata;
    private MessageId messageId;
    private MessageListener listener;
    private static final String TOPIC = "test_topic";
    private static final String QUEUE = "test_queue";
    private Map<String, Map<String, org.meteor.client.consumer.Mode>> wholeQueueTopics = new HashMap<>();

    private MessageMetadata buildMetadata(Map<String, String> extras) {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder().setTopic(ConsumeMessageBenchMark.TOPIC).setQueue(ConsumeMessageBenchMark.QUEUE);
        if (extras != null) {
            for (Map.Entry<String, String> entry : extras.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key != null && value != null) {
                    builder.putExtras(key, value);
                }
            }
        }
        return builder.build();
    }
}
