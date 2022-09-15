package org.shallow;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.State;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.producer.MessagePreInterceptor;
import org.shallow.producer.MessageProducer;
import org.shallow.producer.ProducerConfig;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * If no log is printed, the log level can be set to debug mode, but it may affect the performance test results
 */
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 3, time = 10)
@BenchmarkMode(Mode.All)
@Threads(3)
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

    @TearDown
    public void shutdown() throws Exception {
        producer.shutdownGracefully();
    }

    @Benchmark
    public void sendAsync() {
        Message message = new Message("create", "message", "message".getBytes(UTF_8), null);
        MessagePreInterceptor interceptor = sendMessage -> sendMessage;

        producer.sendAsync(message, interceptor, (sendResult, cause) -> {});
    }
}
