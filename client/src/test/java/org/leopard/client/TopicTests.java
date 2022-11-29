package org.leopard.client;

import io.netty.util.concurrent.Promise;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.leopard.client.internal.pool.DefaultFixedChannelPoolFactory;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Topic;
import org.leopard.remote.processor.Ack;
import org.leopard.remote.proto.server.CreateTopicResponse;
import org.leopard.remote.proto.server.DelTopicResponse;

public class TopicTests {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicTests.class);

    @Test
    public void testCreateTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("create-client", clientConfig);
        client.start();

        Promise<CreateTopicResponse> promise = client.getMetadataWriter().createTopic("test", 3, 1);
        CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        Assert.assertNotNull(response);
        Assert.assertEquals(3, response.getPartitionLimit());
        Assert.assertEquals("test", response.getTopic());
        Assert.assertEquals(1, response.getReplicateLimit());
        Assert.assertEquals(Ack.SUCCESS, response.getAck());

        client.shutdownGracefully();
    }

    @Test
    public void testDelTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("del-client", clientConfig);
        client.start();

        Promise<DelTopicResponse> promise = client.getMetadataWriter().delTopic("test");
        DelTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        Assert.assertNotNull(response);
        Assert.assertEquals("test", response.getTopic());
        Assert.assertEquals(Ack.SUCCESS, response.getAck());

        client.shutdownGracefully();
    }

    @Test
    public void testQuery() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("query-client", clientConfig);
        client.start();

        Map<String, Topic> recordMap = client.getMetadataWriter()
                .queryTopicRecord(DefaultFixedChannelPoolFactory.INSTANCE.accessChannelPool().acquireWithRandomly(),
                        List.of("create"));

        logger.info("result:{}", recordMap);
    }
}
