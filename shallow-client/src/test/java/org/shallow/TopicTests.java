package org.shallow;

import io.netty.util.concurrent.Promise;
import org.junit.Assert;
import org.junit.Test;
import org.shallow.meta.TopicManager;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicResponse;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.Server.CREATE_TOPIC;
import static org.shallow.processor.ProcessCommand.Server.DELETE_TOPIC;

public class TopicTests {

    @Test
    public void testCreateTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:7730"));
        Client client = new Client("Client", clientConfig);
        client.start();

        TopicManager topicManager = new TopicManager(clientConfig);
        Promise<CreateTopicResponse> promise = topicManager.createTopic(CREATE_TOPIC, "test-create", 1, 1);
        CreateTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getLatency(), 1);
        Assert.assertEquals(response.getTopic(), "test-create");
        Assert.assertEquals(response.getPartitions(), 1);
        Assert.assertEquals(response.getAck(), 1);

        client.shutdownGracefully();
    }

    @Test
    public void testDelTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:7730"));
        Client client = new Client("Client", clientConfig);
        client.start();

        TopicManager topicManager = new TopicManager(clientConfig);
        Promise<DelTopicResponse> promise = topicManager.delTopic(DELETE_TOPIC, "test-create");
        DelTopicResponse response = promise.get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getTopic(), "test-create");
        Assert.assertEquals(response.getAck(), 1);

        client.shutdownGracefully();
    }
}
