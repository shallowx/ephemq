package org.leopard.example.metadata;

import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.Test;
import org.leopard.client.Client;
import org.leopard.client.ClientConfig;
import org.leopard.client.pool.DefaultFixedChannelPoolFactory;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.TopicRecord;
import org.leopard.remote.proto.server.CreateTopicResponse;
import org.leopard.remote.proto.server.DelTopicResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("all")
public class TopicMetadataWriterExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadataWriterExample.class);

    @Test
    public void createTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("create-client", clientConfig);
        client.start();

        Promise<CreateTopicResponse> promise = client.getMetadataManager().createTopic("create", 3, 1);
        CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        client.shutdownGracefully();
    }

    @Test
    public void delTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("del-client", clientConfig);
        client.start();

        Promise<DelTopicResponse> promise = client.getMetadataManager().delTopic("create");
        DelTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        client.shutdownGracefully();
    }

    @Test
    public void query() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("query-client", clientConfig);
        client.start();

        Map<String, TopicRecord> recordMap = client.getMetadataManager().queryTopicRecord(DefaultFixedChannelPoolFactory.INSTANCE.accessChannelPool().acquireWithRandomly(), List.of("create"));

        logger.info("result:{}", recordMap);
    }
}
