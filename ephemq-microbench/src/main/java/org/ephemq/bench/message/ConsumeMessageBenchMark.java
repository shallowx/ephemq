package org.ephemq.bench.message;

import io.netty.buffer.ByteBuf;
import org.ephemq.client.consumer.MessageListener;
import org.ephemq.common.message.MessageId;
import org.ephemq.remote.proto.MessageMetadata;
import org.ephemq.remote.util.ByteBufUtil;
import org.openjdk.jmh.annotations.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This class benchmarks the performance of consuming messages.
 * <p>
 * It uses JMH (Java Microbenchmark Harness) to measure the execution time of
 * the handle method in various benchmark modes.
 * <p>
 * The handle method processes messages based on predefined metadata and modes.
 * It will skip processing if the mode is DELETE or if the mode is not defined.
 * <p>
 * In the setup phase, the metadata and listener are initialized.
 * The listener used in the benchmark is a placeholder that performs no operations.
 * <p>
 * In the teardown phase, the benchmark class resets its state by clearing
 * internal fields.
 * <p>
 * An internal function, buildMetadata, constructs the metadata for a message
 * using the provided extras.
 * <p>
 * Annotations:
 * - @BenchmarkMode: Specifies the benchmark modes to be used.
 * - @Warmup: Defines the warmup configuration.
 * - @Measurement: Configures how the benchmark iterations should be measured.
 * - @Threads: Specifies the number of threads to be used.
 * - @State: Defines the scope of the state object.
 * - @OutputTimeUnit: Sets the time unit for benchmark results.
 */
@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ConsumeMessageBenchMark {
    /**
     * The topic name used for testing in the ConsumeMessageBenchMark class.
     */
    private static final String TOPIC = "test_topic";
    /**
     * The name of the queue to be used for testing purposes.
     * This is a constant value representing the queue identifier.
     */
    private static final String QUEUE = "test_queue";
    /**
     * A final ByteBuf instance that holds a randomly generated UUID string.
     * This ByteBuf is created by converting the UUID string using ByteBufUtil.string2Buf method.
     */
    private final ByteBuf buf = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
    /**
     * Metadata corresponding to a message, used within the benchmarking process of the ConsumeMessageBenchMark class.
     */
    private MessageMetadata metadata;
    /**
     * A unique identifier for a message within the benchmarking system.
     * The `messageId` is of type {@link MessageId}, which consists of a ledger, epoch, and index.
     * This identifier is crucial for tracking and handling messages uniquely within the distributed environment.
     */
    private MessageId messageId;
    /**
     * The message listener that handles incoming messages.
     * This listener is utilized in the context of the ConsumeMessageBenchMark class to process messages
     * with specific metadata such as topic, queue, messageId, and message content.
     */
    private MessageListener listener;
    /**
     * A map that holds the queue topics and their associated modes for the consumer.
     * The outer map's key is a string representing the topic name.
     * The value of the outer map is another map where:
     * - The key is a string representing a specific queue name or identifier within the topic.
     * - The value is an instance of {@code org.ephemq.client.consumer.Mode} indicating the mode for the specific queue.
     */
    private Map<String, Map<String, org.ephemq.client.consumer.Mode>> wholeQueueTopics = new HashMap<>();

    /**
     * Sets up the test environment for benchmarking consume message operations.
     * Initializes metadata using an empty HashMap, configures a queue topic with APPEND mode,
     * and assigns a no-op listener for message handling to avoid interference with performance tests.
     */
    @Setup
    public void setUp() {
        this.metadata = buildMetadata(new HashMap<>());
        this.wholeQueueTopics.put(QUEUE, new HashMap<>() {{
            put(TOPIC, org.ephemq.client.consumer.Mode.APPEND);
        }});
        this.listener = (topic, queue, messageId, message, extras) -> {
            // keep empty, because this is user unittest that not need included in the scope of performance testing
        };
    }

    /**
     * This method processes a message by fetching the topic and queue from the metadata
     * associated with the message. If the topic's mode for the given queue is not
     * found or is set to DELETE, the method returns immediately without processing.
     * Otherwise, it invokes the onMessage method of the listener, passing the topic,
     * queue, messageId, message buffer, and any additional parameters.
     * <p>
     * Annotated with @Benchmark, it is used for performance measurement.
     */
    @Benchmark
    public void handle() {
        String topic = metadata.getTopic();
        String queue = metadata.getQueue();
        Map<String, org.ephemq.client.consumer.Mode> topicModes = this.wholeQueueTopics.get(queue);
        org.ephemq.client.consumer.Mode mode = topicModes == null ? null : topicModes.get(topic);
        if (mode == null || mode == org.ephemq.client.consumer.Mode.DELETE) {
            return;
        }

        Map<String, String> extras = new HashMap<>();
        listener.onMessage(topic, queue, messageId, buf, extras);
    }

    /**
     * Clears the state of the benchmark by setting relevant fields to null.
     * This method is typically called after the tests are executed to clean up resources.
     */
    @TearDown
    public void clear() {
        this.messageId = null;
        this.wholeQueueTopics = null;
        this.metadata = null;
    }

    /**
     * Builds a {@link MessageMetadata} object using the provided extras map.
     * The message metadata is initialized with default topic and queue values,
     * and any additional key-value pairs from the extras map are added as metadata extras.
     *
     * @param extras A map of extra metadata key-value pairs to include in the message metadata. Can be null.
     * @return A {@link MessageMetadata} object containing the topic, queue, and any extra metadata.
     */
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
