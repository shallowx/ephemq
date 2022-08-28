package org.shallow.pool;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.shallow.invoke.ClientChannel;
import java.net.SocketAddress;

public interface ShallowChannelPool {
    ClientChannel acquireWithRandomly();
    ClientChannel acquireHealthyOrNew(SocketAddress address);
    Promise<ClientChannel> assemblePromise(Channel channel);

    void shutdownGracefully() throws Exception;
}
