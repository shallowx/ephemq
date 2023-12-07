package org.ostara.proxy.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.ostara.core.CoreConfig;
import org.ostara.management.Manager;
import org.ostara.net.ServiceProcessor;
import org.ostara.remote.invoke.InvokeAnswer;
public class ProxyServiceProcessor extends ServiceProcessor {
    public ProxyServiceProcessor(CoreConfig config, Manager manager) {
        super(config, manager);
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }
}
