package org.shallow;

import io.netty.util.concurrent.Promise;
import org.openjdk.jmh.annotations.*;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.MetadataManager;
import org.shallow.proto.server.CreateTopicResponse;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.Server.CREATE_TOPIC;

@BenchmarkMode(Mode.All)
@State(Scope.Thread)
@Measurement(iterations = 2, time = 5)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.SECONDS)
public class CreateTopicBenchmark {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(CreateTopicBenchmark.class);

    private Client client;
    private ClientConfig clientConfig;

    @Setup
    public void setUp() {
        clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:7730"));

        client = new Client("Client", clientConfig);
        client.start();
    }

    @TearDown
    public void tearDown() {
        client.shutdownGracefully();
    }


    @Benchmark
    public void createTopic() {
        MetadataManager topicManager = new MetadataManager(clientConfig);
        Promise<CreateTopicResponse> promise = topicManager.createTopic(CREATE_TOPIC, "test-benchmark", 1, 1);
        try {
            promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }
}
