package org.shallow;

import io.netty.util.concurrent.Promise;
import org.junit.Assert;
import org.junit.Test;
import org.shallow.meta.TopicManager;
import org.shallow.proto.server.CreateTopicResponse;

import java.util.List;

public class TopicTests {

    @Test
    public void testCreateTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:7730"));
        Client client = new Client("Client", clientConfig);
        client.start();

        TopicManager topicManager = new TopicManager(clientConfig);
        Promise<CreateTopicResponse> promise = topicManager.createTopic("test-create", 1, 1);
        CreateTopicResponse response = promise.get();

        Assert.assertNotNull(response);
        Assert.assertEquals(response.getLatency(), 1);
        Assert.assertEquals(response.getTopic(), "test-create");
        Assert.assertEquals(response.getPartitions(), 1);
        Assert.assertEquals(response.getAck(), 1);

        client.shutdownGracefully();
    }
}
