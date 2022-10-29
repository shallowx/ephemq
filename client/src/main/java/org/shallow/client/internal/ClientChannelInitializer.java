package org.shallow.client.internal;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.shallow.client.Client;
import org.shallow.remote.codec.MessageDecoder;
import org.shallow.remote.codec.MessageEncoder;
import org.shallow.remote.handle.ConnectDuplexHandler;
import org.shallow.remote.handle.ProcessDuplexHandler;

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
