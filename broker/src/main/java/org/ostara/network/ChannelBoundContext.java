package org.ostara.network;

import io.netty.channel.Channel;
import java.util.concurrent.CopyOnWriteArraySet;

public class ChannelBoundContext extends CopyOnWriteArraySet<Channel> {

}
