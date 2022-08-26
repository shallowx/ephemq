package org.shallow.internal;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.shallow.Client;
import org.shallow.ClientConfig;
import org.shallow.codec.MessageDecoder;
import org.shallow.codec.MessageEncoder;
import org.shallow.handle.ConnectDuplexHandler;
import org.shallow.handle.ProcessDuplexHandler;
import org.shallow.invoke.ClientChannel;

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
                .addLast("connect-handler", new ConnectDuplexHandler(10000, 20000))
                .addLast("service-handler", new ProcessDuplexHandler(
                        new ClientServiceProcessorAware(
                                new ClientChannel(socketChannel, client.getClientConfig(), socketAddress),client)));
    }
}
