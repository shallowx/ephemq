package org.ostara.client;

import org.junit.Test;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientConfig;
import org.ostara.client.internal.ClientListener;
import org.ostara.remote.proto.ClusterInfo;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientTests {

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testCreateTopic() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        Client client = new Client("default", clientConfig, new ClientListener() {
        });
        client.start();

        client.createTopic("#test#default", 10, 1);
        new CountDownLatch(1).await(5000, TimeUnit.MILLISECONDS);
        client.close();
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testClusterInfo() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        Client client = new Client("default", clientConfig, new ClientListener() {
        });
        client.start();
        ClientChannel clientChannel = client.fetchChannel(null);

        ClusterInfo info = client.queryClusterInfo(clientChannel);
        System.out.println(info);

        new CountDownLatch(1).await(5000, TimeUnit.MILLISECONDS);
        client.close();
    }

}
