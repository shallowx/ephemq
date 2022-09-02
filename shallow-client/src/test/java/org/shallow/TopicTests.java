package org.shallow;

import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;
import org.junit.Test;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.MetadataManager;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.processor.Ack;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.Server.CREATE_TOPIC;
import static org.shallow.processor.ProcessCommand.Server.DELETE_TOPIC;

public class TopicTests {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicTests.class);

    @Test
    public void testCreateTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("create-client", clientConfig);
        client.start();

        Promise<CreateTopicResponse> promise = client.getMetadataManager().createTopic(CREATE_TOPIC, "create", 3, 1);
        CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getLatencies(), 1);
        Assert.assertEquals(response.getTopic(), "create");
        Assert.assertEquals(response.getPartitions(), 3);
        Assert.assertEquals(response.getAck(), Ack.SUCCESS);

        client.shutdownGracefully();
    }

    @Test
    public void testDelTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("del-client", clientConfig);
        client.start();

        Promise<DelTopicResponse> promise = client.getMetadataManager().delTopic(DELETE_TOPIC, "create");
        DelTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getTopic(), "create");
        Assert.assertEquals(response.getAck(), Ack.SUCCESS);

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
