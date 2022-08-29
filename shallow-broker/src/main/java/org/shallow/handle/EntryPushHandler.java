package org.shallow.handle;

import io.netty.channel.Channel;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.log.Offset;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@ThreadSafe
public class EntryPushHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryPushHandler.class);

    private final BrokerConfig config;
    private final Map<String, Set<Subscription>> subscriptionMap = new ConcurrentHashMap<>();


    public EntryPushHandler(BrokerConfig config) {
        this.config = config;
    }

    public void subscribe(Channel channel, String queue, Offset offset) {
        subscriptionMap.computeIfAbsent(queue, k -> new CopyOnWriteArraySet<>()).add(new Subscription(channel, queue, offset));
    }

    public void handle(){

    }

    private static class Subscription {
        Channel channel;
        String queue;
        Offset offset;

        public Subscription(Channel channel, String queue, Offset offset) {
            this.channel = channel;
            this.queue = queue;
            this.offset = offset;
        }
    }

}
