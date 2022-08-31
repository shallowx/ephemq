package org.shallow.example.metadata;

import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.Test;
import org.shallow.Client;
import org.shallow.ClientConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.TopicRecord;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.Server.CREATE_TOPIC;
import static org.shallow.processor.ProcessCommand.Server.DELETE_TOPIC;

@SuppressWarnings("all")
public class TopicMetadata {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadata.class);

    @Test
    public void createTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));
        Client client = new Client("create-client", clientConfig);
        client.start();

        Promise<CreateTopicResponse> promise = client.getMetadataManager().createTopic(CREATE_TOPIC, "create", 3, 1);
        CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        client.shutdownGracefully();
    }

    @Test
    public void delTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));
        Client client = new Client("del-client", clientConfig);
        client.start();

        Promise<DelTopicResponse> promise = client.getMetadataManager().delTopic(DELETE_TOPIC, "create");
        DelTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        client.shutdownGracefully();
    }

    @Test
    public void query() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));
        Client client = new Client("query-client", clientConfig);
        client.start();

        Map<String, TopicRecord> recordMap = client.getMetadataManager().queryTopicRecord(DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool().acquireWithRandomly(), List.of("create"));

        logger.info("result:{}", recordMap);
    }
}
