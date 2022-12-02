package org.leopard.client;

import static org.leopard.remote.util.NetworkUtils.switchSocketAddress;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.leopard.client.internal.Client;
import org.leopard.client.internal.ClientChannel;
import org.leopard.client.internal.ClientConfig;
import org.leopard.client.internal.pool.DefaultFixedChannelPoolFactory;
import org.leopard.client.internal.pool.ShallowChannelPool;

public class ClientTests {

    @Test
    public void testClientStart() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("start-client", clientConfig);
        client.start();

        ShallowChannelPool pool = DefaultFixedChannelPoolFactory.INSTANCE.accessChannelPool();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(switchSocketAddress("127.0.0.1", 9127));

        Assert.assertNotNull(clientChannel);

        client.shutdownGracefully();
    }
}
