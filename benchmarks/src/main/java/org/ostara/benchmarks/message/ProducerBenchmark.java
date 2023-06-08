package org.ostara.benchmarks.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.*;
import org.ostara.client.ClientConfig;
import org.ostara.client.producer.Producer;
import org.ostara.client.producer.ProducerConfig;
import org.ostara.common.Extras;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ProducerBenchmark {

    private Producer producer;
    private final ThreadLocal<ByteBuf> message = new ThreadLocal<>();
    private final ThreadLocal<String> queue = new ThreadLocal<>();

    @Setup
    public void setUp() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(Collections.singletonList("127.0.0.1:8888"));

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        producer = new Producer("benchmark-producer", producerConfig);
        producer.start();
    }

    @Setup(Level.Invocation)
    public void before() throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(1);

        queue.set(RandomStringUtils.random(5));
        message.set(Unpooled.wrappedBuffer(RandomStringUtils.random(15).getBytes(StandardCharsets.UTF_8)));
    }

    @TearDown(Level.Invocation)
    public void after() {
        message.remove();
        queue.remove();
    }

    @TearDown
    public void clean() {
        producer.close();
    }

    @Benchmark
    public void testSendOneway() {
        producer.sendOneway("test", queue.get(), message.get(), new Extras());
    }

    @Benchmark
    public void testSend() {
        producer.send("test", queue.get(), message.get(), new Extras());
    }

    @Benchmark
    public void testSendAsync() {
        producer.sendAsync("test", queue.get(), message.get(), new Extras(), (messageId, t) -> {});
    }
}
