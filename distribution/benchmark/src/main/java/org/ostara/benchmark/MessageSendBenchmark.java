package org.ostara.benchmark;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.ostara.client.Message;
import org.ostara.client.internal.ClientConfig;
import org.ostara.client.producer.MessagePreInterceptor;
import org.ostara.client.producer.MessageProducer;
import org.ostara.client.producer.ProducerConfig;

/**
 * If no log is printed, the log level can be set to debug mode, but it may affect the performance test results
 */
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 3, time = 10)
@BenchmarkMode(Mode.All)
@Threads(5)
@Fork(value = 1, jvmArgsAppend = {
        "-XX:+UseLargePages",
        "-XX:+UseZGC",
        "-XX:MinHeapSize=4G",
        "-XX:InitialHeapSize=4G",
        "-XX:MaxHeapSize=4G",
        "-XX:MaxDirectMemorySize=10G",
        "-Dfile.encoding=UTF-8",
        "-Duser.timezone=Asia/Shanghai"
})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MessageSendBenchmark {

    private MessageProducer producer;

    @Setup
    public void setUp() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        producer = new MessageProducer("async-producer", producerConfig);
        producer.start();
    }

    @Benchmark
    public void send() {
        Message message = new Message("benchmark", "benchmark", "benchmark".getBytes(UTF_8), null);
        MessagePreInterceptor interceptor = sendMessage -> sendMessage;

        producer.sendAsync(message, interceptor, (sendResult, cause) -> {
        });
    }

    @TearDown
    public void shutdown() throws Exception {
        producer.shutdownGracefully();
    }
}
