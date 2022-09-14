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

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 3, time = 10)
@Threads(5)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MessageSendBenchmark {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageSendBenchmark.class);

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
        MessagePreInterceptor filter = sendMessage -> sendMessage;

        producer.sendAsync(message, filter, (sendResult, cause) -> {});
    }
}
