package org.shallow;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.Assert;
import org.junit.Test;
import org.shallow.invoke.ClientChannel;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.pool.ChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.CreateTopicAnswer;
import org.shallow.proto.CreateTopicRequest;
import java.util.List;

import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class ClientTests {

    @SuppressWarnings("unchecked")
    @Test
    public void testClientStart() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:7730"));
        Client client = new Client("Client", clientConfig);
        client.start();

        ShallowChannelPool pool = ChannelPoolFactory.INSTANCE.obtainChannelPool();
        Future<ClientChannel> future = pool.acquire(switchSocketAddress("127.0.0.1", 7730));

        Promise<CreateTopicAnswer> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        CreateTopicRequest request = CreateTopicRequest.newBuilder().setName("test-remote-network").build();

        ClientChannel clientChannel = future.get();
        clientChannel.invoker().invoke(ProcessCommand.Server.CREATE_TOPIC, 100, promise, request, CreateTopicAnswer.class);

        Assert.assertEquals(InvokeAnswer.SUCCESS, promise.get().getAck());

        client.shutdownGracefully();
    }
}
