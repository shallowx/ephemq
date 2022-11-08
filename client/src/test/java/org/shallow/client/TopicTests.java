package org.shallow.client;

import io.netty.util.concurrent.Promise;
import org.junit.Assert;
import org.junit.Test;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.common.meta.TopicRecord;
import org.shallow.client.pool.DefaultFixedChannelPoolFactory;
import org.shallow.remote.proto.server.CreateTopicResponse;
import org.shallow.remote.proto.server.DelTopicResponse;
import org.shallow.remote.processor.Ack;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TopicTests {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicTests.class);

    @Test
    public void testCreateTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("create-client", clientConfig);
        client.start();

        Promise<CreateTopicResponse> promise = client.getMetadataManager().createTopic("create", 3, 1);
        CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        Assert.assertNotNull(response);
        Assert.assertEquals(1, response.getLatencies());
        Assert.assertEquals("create", response.getTopic());
        Assert.assertEquals(3, response.getPartitions());
        Assert.assertEquals(Ack.SUCCESS, response.getAck());

        client.shutdownGracefully();
    }

    @Test
    public void testDelTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("del-client", clientConfig);
        client.start();

        Promise<DelTopicResponse> promise = client.getMetadataManager().delTopic("create");
        DelTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        Assert.assertNotNull(response);
        Assert.assertEquals("create", response.getTopic());
        Assert.assertEquals(Ack.SUCCESS, response.getAck());

        client.shutdownGracefully();
    }

    @Test
    public void testQuery() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("query-client", clientConfig);
        client.start();

        Map<String, TopicRecord> recordMap = client.getMetadataManager().queryTopicRecord(DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool().acquireWithRandomly(), List.of("create"));

        logger.info("result:{}", recordMap);
    }
}
