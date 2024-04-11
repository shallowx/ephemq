package org.meteor.client;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.internal.CombineListener;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.proto.ClusterInfo;

public class ClientTest {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientTest.class);

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testCreateTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        Client client = new Client("default", clientConfig, new CombineListener() {
        });
        client.start();

        client.createTopic("#test#default", 1, 1);
        // the duration setting only for testing
        new CountDownLatch(1).await(5000, TimeUnit.MILLISECONDS);
        client.close();
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testDeleteTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        Client client = new Client("default", clientConfig, new CombineListener() {
        });
        client.start();

        client.deleteTopic("#test#default");
        // the duration setting only for testing
        new CountDownLatch(1).await(5000, TimeUnit.MILLISECONDS);
        client.close();
    }


    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testClusterInfo() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        Client client = new Client("default", clientConfig, new CombineListener() {
        });
        client.start();
        ClientChannel clientChannel = client.getActiveChannel(null);

        ClusterInfo info = client.queryClusterInfo(clientChannel);
        if (logger.isInfoEnabled()) {
            logger.info("cluster information:{}", info);
        }
        // the duration setting only for testing
        new CountDownLatch(1).await(5000, TimeUnit.MILLISECONDS);
        client.close();
    }

}
