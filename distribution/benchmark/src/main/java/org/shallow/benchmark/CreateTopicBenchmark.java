package org.shallow.benchmark;

import io.netty.util.concurrent.Promise;
import org.openjdk.jmh.annotations.*;
import org.shallow.client.Client;
import org.shallow.client.ClientConfig;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.openjdk.jmh.annotations.State;
import org.shallow.proto.server.CreateTopicResponse;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * If no log is printed, the log level can be set to debug mode, but it may affect the performance test results
 */
@BenchmarkMode(Mode.All)
@State(Scope.Benchmark)
@Measurement(iterations = 2, time = 2000, timeUnit = TimeUnit.MILLISECONDS, batchSize = 10)
@Threads(1)
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
@OutputTimeUnit(TimeUnit.SECONDS)
public class CreateTopicBenchmark {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(CreateTopicBenchmark.class);

    private Client client;
    private ClientConfig clientConfig;

    @Setup
    public void setUp() throws Exception {
        clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        client = new Client("Client", clientConfig);
        client.start();
    }

    @TearDown
    public void tearDown() throws Exception {
        client.shutdownGracefully();
    }

    @Benchmark
    public void createTopic() {
        Promise<CreateTopicResponse> promise = client.getMetadataManager().createTopic( "test-benchmark-" + UUID.randomUUID(), 1, 1);
        try {
            promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }
}
