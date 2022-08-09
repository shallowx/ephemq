package org.shallow;

import org.junit.Assert;
import org.junit.Test;
import org.shallow.invoke.ClientChannel;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class ClientTests {

    @Test
    public void testClientStart() throws ExecutionException, InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:7730"));
        Client client = new Client("Client", clientConfig);
        client.start();

        ShallowChannelPool pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(switchSocketAddress("127.0.0.1", 7730));

        Assert.assertNotNull(clientChannel);

        client.shutdownGracefully();
    }
}
