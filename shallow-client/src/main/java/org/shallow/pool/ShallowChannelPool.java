package org.shallow.pool;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.ClientChannel;
import java.net.SocketAddress;

public interface ShallowChannelPool {

    void initChannelPool() throws Exception;
    ClientChannel acquireWithRandomly();
    ClientChannel acquireHealthyOrNew(SocketAddress address);
    Promise<ClientChannel> assemblePromise(Channel channel);

    void shutdownGracefully() throws Exception;
}
