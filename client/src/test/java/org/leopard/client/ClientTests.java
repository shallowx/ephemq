package org.leopard.client;

import org.junit.Assert;
import org.junit.Test;
import org.leopard.client.internal.ClientChannel;
import org.leopard.client.pool.DefaultFixedChannelPoolFactory;
import org.leopard.client.pool.ShallowChannelPool;
import java.util.List;

import static org.leopard.remote.util.NetworkUtils.switchSocketAddress;

public class ClientTests {

    @Test
    public void testClientStart() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("start-client", clientConfig);
        client.start();

        ShallowChannelPool pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(switchSocketAddress("127.0.0.1", 9127));

        Assert.assertNotNull(clientChannel);

        client.shutdownGracefully();
    }
}
