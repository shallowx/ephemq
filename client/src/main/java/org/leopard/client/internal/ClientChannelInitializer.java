package org.leopard.client.internal;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.leopard.client.Client;
import org.leopard.remote.codec.MessageDecoder;
import org.leopard.remote.codec.MessageEncoder;
import org.leopard.remote.handle.ConnectDuplexHandler;
import org.leopard.remote.handle.ProcessDuplexHandler;

import java.net.SocketAddress;

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
                        new ClientServiceProcessorAware(
                                new ClientChannel(socketChannel, client.getClientConfig(), socketAddress),client)));
    }
}
