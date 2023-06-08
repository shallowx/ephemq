package org.ostara.benchmarks.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomUtils;
import org.openjdk.jmh.annotations.*;
import org.ostara.client.ClientConfig;
import org.ostara.client.consumer.Consumer;
import org.ostara.client.consumer.ConsumerConfig;
import org.ostara.client.consumer.MessageListener;
import org.ostara.client.producer.Producer;
import org.ostara.client.producer.ProducerConfig;
import org.ostara.common.Extras;
import org.ostara.common.MessageId;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ConsumerBenchmark {
    private final String[] queues = new String[]{"a","b","c"};
    private Producer producer;
    private Consumer consumer;
    private final ThreadLocal<ByteBuf> message = new ThreadLocal<>();
    private final ThreadLocal<String> queue = new ThreadLocal<>();

    private final Map<Long, CompletableFuture<Long>> futureMap = new ConcurrentHashMap<>();

    @Setup
    public void prepare() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(Collections.singletonList("127.0.0.1:8888"));

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        producer = new Producer("benchmark-producer", producerConfig);
        producer.start();

        clientConfig.setBootstrapAddresses(Collections.singletonList("127.0.0.1:8888"));
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);
        consumer = new Consumer("benchmark-consumer", consumerConfig, new MessageListener() {
            @Override
            public void onMessage(String topic, String queue, MessageId messageId, ByteBuf message, Extras extras) {
                long startTime = message.readLong();
                futureMap.remove(startTime);
            }
        });
        consumer.start();
        for (String queue : queues) {
            consumer.attach("test", queue);
        }

        TimeUnit.SECONDS.sleep(10);
    }

    @Setup(Level.Invocation)
    public void before() {
        long startTime = System.nanoTime();
        futureMap.put(startTime, new CompletableFuture<>());
        ByteBuf buffer = Unpooled.buffer(8);
        buffer.writeLong(startTime);
        message.set(buffer);
        queue.set(queues[RandomUtils.nextInt(0, queues.length)]);
    }

    @TearDown(Level.Invocation)
    public void after() {
        message.remove();
        queue.remove();
    }

    @TearDown
    public void clean() {
        producer.close();
        consumer.close();
    }

    @Benchmark
    public void testSendOneway() throws ExecutionException, InterruptedException, TimeoutException {
        ByteBuf buf = message.get();
        long startTime = buf.getLong(0);
        producer.sendOneway("test", queue.get(), message.get(), new Extras());
        CompletableFuture<Long> future = futureMap.get(startTime);
        if (future != null) {
            future.get(30, TimeUnit.SECONDS);
        }
    }
}
