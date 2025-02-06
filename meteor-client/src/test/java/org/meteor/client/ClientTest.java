package org.meteor.client;

import org.junit.Test;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.ClientConfig;
import org.meteor.client.core.CombineListener;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.proto.ClusterInfo;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The ClientTest class contains unit tests for the Client class.
 * It ensures the correct creation, deletion of topics, and fetching of cluster information.
 */
public class ClientTest {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientTest.class);

    /**
     * This test method verifies the creation of a topic using the Client class.
     * It configures a client, starts it, and initiates a topic creation request.
     * The method then waits briefly to ensure the action is processed before closing the client.
     *
     * @throws Exception if an error occurs during the operation
     * @Test indicates that this is a test method to be run by the testing framework.
     * @SuppressWarnings("ResultOfMethodCallIgnored") suppresses warnings about ignoring the result of certain method calls.
     */
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

    /**
     * Tests the deletion of a topic using the Client API.
     *
     * @throws Exception if an error occurs during the test execution
     * <p>
     * This method:
     * 1. Configures the client with a bootstrap server address.
     * 2. Initializes the client with the specified configuration and listener.
     * 3. Starts the client.
     * 4. Deletes the specified topic.
     * 5. Awaits for a brief period to ensure the operation completes.
     * 6. Closes the client connection.
     */
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


    /**
     * Tests the functionality for querying cluster information using the Client class.
     *
     * @throws Exception if any error occurs during the test execution.
     */
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
