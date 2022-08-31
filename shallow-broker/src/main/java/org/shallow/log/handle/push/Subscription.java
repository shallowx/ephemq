package org.shallow.log.handle.push;

import io.netty.channel.Channel;
import org.shallow.log.Offset;

public record Subscription(Channel channel, String queue, Offset offset, short version) {
}
