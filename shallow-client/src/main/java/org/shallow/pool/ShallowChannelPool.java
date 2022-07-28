package org.shallow.pool;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.shallow.invoke.ClientChannel;
import java.net.SocketAddress;

public interface ShallowChannelPool {
    Future<ClientChannel> acquire(SocketAddress address);
    Future<ClientChannel> acquire();
    Promise<ClientChannel> assemblePromise(Channel channel);
}
