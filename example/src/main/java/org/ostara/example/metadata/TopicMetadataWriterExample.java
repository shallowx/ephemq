package org.ostara.example.metadata;

import io.netty.util.concurrent.Promise;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientConfig;
import org.ostara.client.internal.pool.DefaultFixedChannelPoolFactory;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Topic;
import org.ostara.remote.proto.server.CreateTopicResponse;
import org.ostara.remote.proto.server.DelTopicResponse;

@SuppressWarnings("all")
public class TopicMetadataWriterExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadataWriterExample.class);

    @Test
    public void createTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("create-client", clientConfig);
        client.start();

        Promise<CreateTopicResponse> promise = client.getMetadataSupport().createTopic("create", 3, 1);
        CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        client.shutdownGracefully();
    }

    @Test
    public void delTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("del-client", clientConfig);
        client.start();

        Promise<DelTopicResponse> promise = client.getMetadataSupport().delTopic("create");
        DelTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        client.shutdownGracefully();
    }

    @Test
    public void query() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("query-client", clientConfig);
        client.start();

        Map<String, Topic> recordMap = client.getMetadataSupport()
                .queryTopicRecord(DefaultFixedChannelPoolFactory.INSTANCE.accessChannelPool().acquireWithRandomly(),
                        List.of("create"));

        logger.info("result:{}", recordMap);
    }
}
