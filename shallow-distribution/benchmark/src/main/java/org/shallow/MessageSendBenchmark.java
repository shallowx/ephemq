package org.shallow;

import org.openjdk.jmh.annotations.*;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.producer.MessageFilter;
import org.shallow.producer.MessageProducer;
import org.shallow.producer.ProducerConfig;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

@BenchmarkMode(Mode.All)
@State(Scope.Thread)
@Measurement(iterations = 3, time = 5)
@Threads(10)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MessageSendBenchmark {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageSendBenchmark.class);

    private MessageProducer producer;
    private ClientConfig clientConfig;
    private ProducerConfig producerConfig;

    @Setup
    public void setUp() throws Exception {
        clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));

        producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        producer = new MessageProducer("async-producer", producerConfig);
        producer.start();
    }

    @Benchmark
    public void sendAsync() {
        Message message = new Message("create", "message", "message".getBytes(UTF_8), null);
        MessageFilter filter = sendMessage -> sendMessage;

        producer.sendAsync(message, filter, (sendResult, cause) -> {
            if (null == cause) {
                logger.warn("send result - {}", sendResult);
            } else {
                logger.error(cause);
            }
        });
    }

    @TearDown
    public void shutdown() throws Exception {
        producer.shutdownGracefully();
    }
}
