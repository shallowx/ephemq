package org.ostara.client.internal;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import java.net.SocketAddress;
import org.ostara.remote.codec.MessageDecoder;
import org.ostara.remote.codec.MessageEncoder;
import org.ostara.remote.handle.ConnectDuplexHandler;
import org.ostara.remote.handle.ProcessDuplexHandler;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final SocketAddress socketAddress;
    private final Client client;

    public ClientChannelInitializer(SocketAddress socketAddress, Client client) {
        this.socketAddress = socketAddress;
        this.client = client;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast("encoder", MessageEncoder.instance())
                .addLast("decoder", new MessageDecoder())
                .addLast("connect-handler", new ConnectDuplexHandler(30000, 40000))
                .addLast("service-handler", new ProcessDuplexHandler(
                        new MessageProcessorAware(
                                new ClientChannel(socketChannel, client.getClientConfig(), socketAddress), client)));
    }
}
